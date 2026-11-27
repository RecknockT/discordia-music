use clap::Parser;
use discordia_bot::BotIdentity;
use discordia_music::activity::runtime;
use std::{
    error::Error,
    io::ErrorKind,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    fs::OpenOptions,
    io::{AsyncReadExt, AsyncWriteExt},
};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Parser)]
#[command(about = "Run optional Music Lab: public metadata queue and opt-in local audio.")]
struct Args {
    #[arg(long, env = "SERVER_URL", default_value = "ws://127.0.0.1:9000")]
    server: String,
    #[arg(long, env = "MUSIC_KEY_FILE")]
    key_file: Option<PathBuf>,
    #[arg(long = "audio-file", value_name = "PATH", action = clap::ArgAction::Append)]
    audio_files: Vec<PathBuf>,
    #[arg(long)]
    soundcloud: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    #[cfg(not(feature = "soundcloud"))]
    if args.soundcloud {
        return Err("SoundCloud requires building with --features soundcloud".into());
    }
    #[cfg(feature = "soundcloud")]
    let source = if args.soundcloud {
        Some(std::sync::Arc::new(
            discordia_music::soundcloud::SoundCloud::configured()?,
        ))
    } else {
        None
    };
    #[cfg(not(feature = "playback"))]
    if !args.audio_files.is_empty() {
        return Err("local audio requires building with --features playback".into());
    }
    #[cfg(feature = "playback")]
    let library = if args.audio_files.is_empty() {
        #[cfg(feature = "soundcloud")]
        if args.soundcloud {
            Some(discordia_music::activity::playback::Library::empty())
        } else {
            None
        }
        #[cfg(not(feature = "soundcloud"))]
        None
    } else {
        Some(discordia_music::activity::playback::Library::open(args.audio_files.clone()).await?)
    };
    let key_path = args.key_file.as_deref().unwrap_or_else(|| {
        Path::new(if args.audio_files.is_empty() && !args.soundcloud {
            "music.key"
        } else {
            "music-local.key"
        })
    });
    let identity = identity(key_path).await?;
    eprintln!(
        "Music Lab — experimental shared queue and optional operator-configured local audio."
    );
    eprintln!(
        "Install this app key in Integrations → Install an app: {}",
        identity.pubkey()
    );
    #[cfg(feature = "playback")]
    if let Some(library) = library {
        let (stop, receiver) = tokio::sync::watch::channel(false);
        let task = reconnect_audio(
            &args.server,
            &identity,
            library,
            receiver,
            #[cfg(feature = "soundcloud")]
            source,
        );
        tokio::pin!(task);
        return tokio::select! {
            signal=tokio::signal::ctrl_c()=>{ signal?; stop.send_replace(true); task.await },
            result=&mut task=>result,
        };
    }
    tokio::select! {
        signal = tokio::signal::ctrl_c() => { signal?; eprintln!("Music service stopped."); Ok(()) }
        result = reconnect(&args.server, &identity) => result,
    }
}

#[cfg(feature = "playback")]
async fn reconnect_audio(
    server: &str,
    identity: &BotIdentity,
    library: discordia_music::activity::playback::Library,
    mut stop: tokio::sync::watch::Receiver<bool>,
    #[cfg(feature = "soundcloud")] source: Option<
        std::sync::Arc<discordia_music::soundcloud::SoundCloud>,
    >,
) -> Result<()> {
    loop {
        if *stop.borrow() {
            return Ok(());
        }
        #[cfg(feature = "soundcloud")]
        let result = if let Some(source) = source.clone() {
            runtime::connect_soundcloud(server, identity, library.clone(), source, stop.clone())
                .await
        } else {
            runtime::connect_audio(server, identity, library.clone(), stop.clone()).await
        };
        #[cfg(not(feature = "soundcloud"))]
        let result = runtime::connect_audio(server, identity, library.clone(), stop.clone()).await;
        if let Err(error) = result {
            if error.is::<discordia_music::activity::playback::CleanupFailure>() {
                return Err(error);
            }
            eprintln!("Local audio service disconnected; its publications were stopped.");
        }
        if *stop.borrow() {
            return Ok(());
        }
        tokio::select! { _=tokio::time::sleep(Duration::from_secs(5))=>{}, _=stop.changed()=>return Ok(()) }
    }
}

async fn reconnect(server: &str, identity: &BotIdentity) -> Result<()> {
    loop {
        match runtime::connect(server, identity).await {
            Ok(()) => eprintln!("Host connection closed; reconnecting in 5 seconds."),
            Err(_) => eprintln!("Music service connection failed; reconnecting in 5 seconds."),
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn identity(path: &Path) -> Result<BotIdentity> {
    match tokio::fs::File::open(path).await {
        Ok(file) => {
            if file.metadata().await?.len() > 256 {
                return Err("the existing music key file is too large".into());
            }
            let mut secret = String::new();
            file.take(256).read_to_string(&mut secret).await?;
            Ok(BotIdentity::from_base58_secret(secret.trim())?)
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            let identity = BotIdentity::generate();
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            options.mode(0o600);
            let mut file = options.open(path).await?;
            file.write_all(identity.secret_base58().as_bytes()).await?;
            file.sync_all().await?;
            Ok(identity)
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn key_is_stable_and_invalid_file_is_never_replaced() {
        let dir = std::env::temp_dir().join(format!(
            "discordia-music-key-{}",
            discordia_bot::protocol::Id::new_v4()
        ));
        tokio::fs::create_dir(&dir).await.unwrap();
        let path = dir.join("app.key");
        let first = identity(&path).await.unwrap();
        assert_eq!(identity(&path).await.unwrap().pubkey(), first.pubkey());
        tokio::fs::write(&path, "invalid-existing-key")
            .await
            .unwrap();
        assert!(identity(&path).await.is_err());
        assert_eq!(
            tokio::fs::read_to_string(&path).await.unwrap(),
            "invalid-existing-key"
        );
        tokio::fs::remove_file(path).await.unwrap();
        tokio::fs::remove_dir(dir).await.unwrap();
    }
}
