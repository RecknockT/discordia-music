use crate::source::{Kind, Provider, Source};
use reqwest::{Client, Response, StatusCode, redirect::Policy};
use serde::Deserialize;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{io::AsyncWriteExt, sync::Mutex};
use url::Url;

const MAX_JSON: usize = 256 * 1024;
const MAX_AUDIO: usize = 128 * 1024 * 1024;
type Result<T> = std::result::Result<T, &'static str>;

#[derive(Deserialize)]
struct TokenReply {
    access_token: String,
    refresh_token: String,
    expires_in: u64,
}
struct Token {
    access: String,
    refresh: String,
    expires: Instant,
}
struct Auth {
    token: Option<Token>,
    retry: Instant,
}

pub struct SoundCloud {
    client: Client,
    id: String,
    secret: String,
    auth: Mutex<Auth>,
    cooldown: Mutex<Instant>,
}

pub struct Prepared {
    pub path: std::path::PathBuf,
    pub title: String,
    pub directory: Arc<tempfile::TempDir>,
}

impl SoundCloud {
    pub fn configured() -> Result<Self> {
        let id = std::env::var("SOUNDCLOUD_CLIENT_ID")
            .map_err(|_| "configure SOUNDCLOUD_CLIENT_ID on the music service")?;
        let secret = std::env::var("SOUNDCLOUD_CLIENT_SECRET")
            .map_err(|_| "configure SOUNDCLOUD_CLIENT_SECRET on the music service")?;
        if id.is_empty() || secret.is_empty() || id.len() > 4096 || secret.len() > 4096 {
            return Err("invalid SoundCloud service credentials");
        }
        let client = Client::builder()
            .https_only(true)
            .no_proxy()
            .redirect(Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(20))
            .user_agent("Discordia-Music/0.1")
            .build()
            .map_err(|_| "could not initialize SoundCloud")?;
        Ok(Self {
            client,
            id,
            secret,
            cooldown: Mutex::new(Instant::now()),
            auth: Mutex::new(Auth {
                token: None,
                retry: Instant::now(),
            }),
        })
    }

    async fn token(&self) -> Result<String> {
        let mut auth = self.auth.lock().await;
        if let Some(token) = &auth.token
            && token.expires > Instant::now() + Duration::from_secs(60)
        {
            return Ok(token.access.clone());
        }
        if auth.retry > Instant::now() {
            return Err("SoundCloud authentication is temporarily unavailable; retry later");
        }
        auth.retry = Instant::now() + Duration::from_secs(300);
        let request = self
            .client
            .post("https://secure.soundcloud.com/oauth/token");
        let request = if let Some(token) = auth.token.take() {
            request.form(&[
                ("grant_type", "refresh_token"),
                ("refresh_token", token.refresh.as_str()),
                ("client_id", self.id.as_str()),
                ("client_secret", self.secret.as_str()),
            ])
        } else {
            request
                .basic_auth(&self.id, Some(&self.secret))
                .form(&[("grant_type", "client_credentials")])
        };
        let response = request
            .send()
            .await
            .map_err(|_| "SoundCloud authentication could not connect")?;
        let value: TokenReply = serde_json::from_slice(&bounded(response, MAX_JSON).await?)
            .map_err(|_| "invalid SoundCloud authentication response")?;
        if value.access_token.is_empty()
            || value.access_token.len() > 8192
            || value.refresh_token.is_empty()
            || value.refresh_token.len() > 8192
            || value.expires_in < 120
        {
            return Err("invalid SoundCloud authentication response");
        }
        let access = value.access_token.clone();
        auth.token = Some(Token {
            access: value.access_token,
            refresh: value.refresh_token,
            expires: Instant::now() + Duration::from_secs(value.expires_in.min(86400)),
        });
        Ok(access)
    }

    async fn get(&self, mut url: Url, token: &str, cap: usize) -> Result<(Url, Vec<u8>)> {
        for _ in 0..4 {
            if *self.cooldown.lock().await > Instant::now() {
                return Err("SoundCloud rate limit reached; retry later");
            }
            validate_media_url(&url)?;
            let mut request = self.client.get(url.clone());
            if url.host_str() == Some("api.soundcloud.com") {
                request = request.header("Authorization", format!("OAuth {token}"));
            }
            let response = request
                .send()
                .await
                .map_err(|_| "SoundCloud connection failed")?;
            if response.status() == StatusCode::TOO_MANY_REQUESTS {
                let delay = response
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(60)
                    .clamp(1, 3600);
                *self.cooldown.lock().await = Instant::now() + Duration::from_secs(delay);
                return Err("SoundCloud rate limit reached; retry later");
            }
            if response.status().is_redirection() {
                let location = response
                    .headers()
                    .get("location")
                    .and_then(|v| v.to_str().ok())
                    .ok_or("invalid SoundCloud redirect")?;
                url = url
                    .join(location)
                    .map_err(|_| "invalid SoundCloud redirect")?;
                continue;
            }
            return Ok((url, bounded(response, cap).await?));
        }
        Err("too many SoundCloud redirects")
    }

    pub async fn prepare(&self, source: &Source) -> Result<Prepared> {
        if source.provider() != Provider::Soundcloud || source.kind() != Kind::Track {
            return Err(
                "select a single public SoundCloud track; other providers and collections are not playable yet",
            );
        }
        tokio::time::timeout(Duration::from_secs(120), self.prepare_inner(source))
            .await
            .map_err(|_| "SoundCloud preparation timed out")?
    }

    async fn prepare_inner(&self, source: &Source) -> Result<Prepared> {
        let token = self.token().await?;
        let mut resolve = Url::parse("https://api.soundcloud.com/resolve")
            .map_err(|_| "invalid SoundCloud endpoint")?;
        resolve
            .query_pairs_mut()
            .append_pair("url", source.canonical_url());
        let (_, data) = self.get(resolve, &token, MAX_JSON).await?;
        let track: serde_json::Value =
            serde_json::from_slice(&data).map_err(|_| "invalid SoundCloud track")?;
        let urn = track["urn"]
            .as_str()
            .ok_or("SoundCloud did not return a track identifier")?;
        validate_urn(urn)?;
        if track["access"] != "playable" || track["streamable"] != true {
            return Err(
                "SoundCloud restricts this track; previews and blocked tracks are not played as full songs",
            );
        }
        if track["duration"]
            .as_u64()
            .is_none_or(|ms| ms == 0 || ms > 30 * 60 * 1000)
        {
            return Err("SoundCloud tracks must be at most 30 minutes");
        }
        let title = track["title"]
            .as_str()
            .filter(|s| !s.is_empty() && s.len() <= 2048)
            .ok_or("invalid SoundCloud title")?
            .to_string();
        let mut streams = Url::parse("https://api.soundcloud.com/tracks/")
            .map_err(|_| "invalid SoundCloud endpoint")?;
        streams
            .path_segments_mut()
            .map_err(|_| "invalid SoundCloud endpoint")?
            .push(urn)
            .push("streams");
        let (_, data) = self.get(streams, &token, MAX_JSON).await?;
        let streams: serde_json::Value =
            serde_json::from_slice(&data).map_err(|_| "invalid SoundCloud streams")?;
        let playlist = Url::parse(
            streams["hls_mp3_128_url"]
                .as_str()
                .ok_or("SoundCloud did not offer a supported full MP3 stream")?,
        )
        .map_err(|_| "invalid SoundCloud stream URL")?;
        let mut playlist = playlist;
        let mut prepared_segments = None;
        for _ in 0..3 {
            let (base, data) = self.get(playlist, &token, MAX_JSON).await?;
            if let Some(variant) = variant(&base, &data)? {
                playlist = variant;
            } else {
                prepared_segments = Some(segments(&base, &data)?);
                break;
            }
        }
        let segments =
            prepared_segments.ok_or("SoundCloud playlist nesting exceeds its resource limit")?;
        let directory = tokio::task::spawn_blocking(tempfile::tempdir)
            .await
            .map_err(|_| "could not create audio workspace")?
            .map_err(|_| "could not create audio workspace")?;
        let directory = Arc::new(directory);
        let path = directory.path().join("track.mp3");
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
            .map_err(|_| "could not create prepared audio")?;
        let mut total = 0;
        for segment in segments {
            let (_, data) = self
                .get(segment, &token, (MAX_AUDIO - total).min(8 * 1024 * 1024))
                .await?;
            total += data.len();
            file.write_all(&data)
                .await
                .map_err(|_| "could not store prepared audio")?;
        }
        if total == 0 {
            return Err("SoundCloud returned empty audio");
        }
        file.flush()
            .await
            .map_err(|_| "could not finish prepared audio")?;
        Ok(Prepared {
            path,
            title,
            directory,
        })
    }
}

async fn bounded(mut response: Response, cap: usize) -> Result<Vec<u8>> {
    if response.status() == StatusCode::TOO_MANY_REQUESTS {
        return Err("SoundCloud rate limit reached; retry later");
    }
    if response.status() == StatusCode::UNAUTHORIZED || response.status() == StatusCode::FORBIDDEN {
        return Err("SoundCloud rejected access to this resource");
    }
    if !response.status().is_success() {
        return Err("SoundCloud resource is unavailable");
    }
    if response.content_length().is_some_and(|n| n > cap as u64) {
        return Err("SoundCloud response exceeds its resource limit");
    }
    let mut result = Vec::new();
    while let Some(bytes) = response
        .chunk()
        .await
        .map_err(|_| "SoundCloud download failed")?
    {
        if bytes.len() > cap - result.len() {
            return Err("SoundCloud response exceeds its resource limit");
        }
        result.extend_from_slice(&bytes);
    }
    Ok(result)
}

fn validate_media_url(url: &Url) -> Result<()> {
    let host = url.host_str().ok_or("invalid SoundCloud destination")?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
        || !(host == "api.soundcloud.com" || host.ends_with(".sndcdn.com"))
    {
        return Err("untrusted SoundCloud destination");
    }
    Ok(())
}

fn validate_urn(urn: &str) -> Result<()> {
    if urn
        .strip_prefix("soundcloud:tracks:")
        .is_none_or(|id| id.is_empty() || id.len() > 32 || !id.bytes().all(|c| c.is_ascii_digit()))
    {
        return Err("invalid SoundCloud track identifier");
    }
    Ok(())
}

fn variant(base: &Url, data: &[u8]) -> Result<Option<Url>> {
    let text = std::str::from_utf8(data).map_err(|_| "invalid SoundCloud playlist")?;
    if !text.starts_with("#EXTM3U") {
        return Err("invalid SoundCloud playlist");
    }
    let mut expected = false;
    let mut variants = Vec::new();
    for line in text.lines().map(str::trim).filter(|s| !s.is_empty()) {
        if line.starts_with("#EXT-X-STREAM-INF:") {
            if expected {
                return Err("invalid SoundCloud variant");
            }
            expected = true;
        } else if expected && !line.starts_with('#') {
            let url = base
                .join(line)
                .map_err(|_| "invalid SoundCloud variant URL")?;
            validate_media_url(&url)?;
            variants.push(url);
            if variants.len() > 8 {
                return Err("too many SoundCloud variants");
            }
            expected = false;
        }
    }
    if expected {
        return Err("incomplete SoundCloud variant");
    }
    Ok(variants.into_iter().next())
}

fn segments(base: &Url, data: &[u8]) -> Result<Vec<Url>> {
    let text = std::str::from_utf8(data).map_err(|_| "invalid SoundCloud playlist")?;
    if !text.starts_with("#EXTM3U") || !text.lines().any(|line| line.trim() == "#EXT-X-ENDLIST") {
        return Err("live or incomplete SoundCloud streams are not supported");
    }
    let mut result = Vec::new();
    let mut duration = 0.0f64;
    let mut expected = false;
    for line in text.lines().map(str::trim).filter(|s| !s.is_empty()) {
        if let Some(value) = line.strip_prefix("#EXTINF:") {
            let seconds = value
                .split(',')
                .next()
                .and_then(|s| s.parse::<f64>().ok())
                .ok_or("invalid SoundCloud segment duration")?;
            if expected || !seconds.is_finite() || seconds <= 0.0 || seconds > 60.0 {
                return Err("invalid SoundCloud segment duration");
            }
            duration += seconds;
            expected = true;
        } else if line.starts_with("#EXT-X-KEY:")
            || line.starts_with("#EXT-X-MAP:")
            || line.starts_with("#EXT-X-BYTERANGE:")
            || line.starts_with("#EXT-X-STREAM-INF:")
            || line.starts_with("#EXT-X-DISCONTINUITY")
        {
            return Err("unsupported SoundCloud playlist format");
        } else if !line.starts_with('#') {
            if !expected || result.len() >= 512 || duration > 1800.0 {
                return Err("SoundCloud playlist exceeds its resource limit");
            }
            let url = base
                .join(line)
                .map_err(|_| "invalid SoundCloud segment URL")?;
            validate_media_url(&url)?;
            result.push(url);
            expected = false;
        }
    }
    if result.is_empty() || expected {
        return Err("empty or incomplete SoundCloud playlist");
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_finite_bounded_full_mp3_playlists_are_accepted() {
        let base = Url::parse("https://cf-hls-media.sndcdn.com/a/list.m3u8").unwrap();
        let valid = b"#EXTM3U\n#EXTINF:5.0,\none.mp3\n#EXTINF:2.0,\ntwo.mp3\n#EXT-X-ENDLIST\n";
        assert_eq!(segments(&base, valid).unwrap().len(), 2);
        for bad in [
            "#EXTM3U\n#EXTINF:5,\na.mp3",
            "#EXTM3U\n#EXTINF:NaN,\na.mp3\n#EXT-X-ENDLIST",
            "#EXTM3U\n#EXTINF:5,\nhttp://127.0.0.1/a\n#EXT-X-ENDLIST",
            "#EXTM3U\n#EXT-X-KEY:METHOD=AES-128\n#EXTINF:5,\na.mp3\n#EXT-X-ENDLIST",
        ] {
            assert!(segments(&base, bad.as_bytes()).is_err());
        }
    }
    #[test]
    fn master_playlists_only_follow_bounded_provider_variants() {
        let base = Url::parse("https://cf-hls-media.sndcdn.com/a/list.m3u8").unwrap();
        assert_eq!(
            variant(
                &base,
                b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=128000\ntrack.m3u8\n"
            )
            .unwrap()
            .unwrap()
            .as_str(),
            "https://cf-hls-media.sndcdn.com/a/track.m3u8"
        );
        assert!(
            variant(
                &base,
                b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=128000\nhttps://evil.test/a\n"
            )
            .is_err()
        );
        assert!(variant(&base, b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=128000\n").is_err());
    }

    #[test]
    fn provider_destinations_and_track_identifiers_cannot_be_confused() {
        for bad in [
            "https://sndcdn.com.evil.test/a",
            "https://127.0.0.1/a",
            "https://api.soundcloud.com:8443/a",
            "https://me@api.soundcloud.com/a",
            "file:///a",
        ] {
            assert!(validate_media_url(&Url::parse(bad).unwrap()).is_err());
        }
        for bad in [
            "soundcloud:users:123",
            "soundcloud:tracks:",
            "soundcloud:tracks:../me",
        ] {
            assert!(validate_urn(bad).is_err());
        }
        assert!(validate_urn("soundcloud:tracks:123456").is_ok());
    }
}
