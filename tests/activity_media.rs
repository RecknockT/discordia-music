#![cfg(feature = "playback")]
mod support;

use discordia_bot::{
    Bot, BotIdentity,
    activity_audio::KeyContext,
    protocol::{
        ActivityAudioRole, ActivityInstance, ActivityRole, ChannelKind, ClientMessage, Id,
        ServerMessage, activity_audio_epoch,
    },
};
use discordia_music::{
    activity::{audio_manifest, playback::Library, runtime},
    audio::{RATE, transport::music_room_options},
};
use discordia_server::{ServerConfig, livekit::LiveKitConfig};
use futures_util::{FutureExt, StreamExt};
use livekit::{prelude::*, webrtc::audio_stream::native::NativeAudioStream};
use serde_json::{Value, json};
use std::{f64::consts::TAU, path::Path, time::Duration};

#[track_caller]
fn wait<T>(
    bot: &mut Bot,
    mut select: impl FnMut(ServerMessage) -> Option<T>,
) -> impl std::future::Future<Output = T> {
    let caller = std::panic::Location::caller();
    async move {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let event = bot.next_event().await.expect("test gateway closed");
                let rejected = match &event {
                    ServerMessage::Error { message } => {
                        Some(message.chars().take(140).collect::<String>())
                    }
                    _ => None,
                };
                if let Some(value) = select(event) {
                    return value;
                }
                if let Some(message) = rejected {
                    panic!("gateway rejected a synthetic test request: {message}");
                }
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "test gateway event timed out at {}:{}",
                caller.file(),
                caller.line()
            )
        })
    }
}

async fn user(url: &str, name: &str) -> (Bot, BotIdentity) {
    let identity = BotIdentity::generate();
    let mut bot = Bot::connect_as_user(url, &identity, name).await.unwrap();
    wait(&mut bot, |event| {
        matches!(event, ServerMessage::Ready { .. }).then_some(())
    })
    .await;
    (bot, identity)
}

async fn command(bot: &mut Bot, instance: &ActivityInstance, action: &str) {
    bot.send(&ClientMessage::ActivitySend {
        instance_id: instance.id,
        to: Some(instance.app.clone()),
        data: json!({"t":"playback","action":action,"request_id":Id::new_v4()}).to_string(),
    })
    .await
    .unwrap();
}

async fn phase(bot: &mut Bot, app: &str, desired: &str, track: usize) {
    wait(bot, |event| match event {
        ServerMessage::ActivityEvent { from, data, .. } if from == app => {
            let value: Value = serde_json::from_str(&data).unwrap();
            (value["t"] == "playback_state" && value["phase"] == desired && value["track"] == track)
                .then_some(())
        }
        _ => None,
    })
    .await;
}

fn wave(path: &Path, frequency: f64) {
    let frames = RATE
        * if std::env::var_os("MUSIC_TEST_WEBVIEW_PROBE").is_some() {
            120
        } else {
            12
        };
    let mut bytes = Vec::with_capacity(frames as usize * 4 + 44);
    bytes.extend_from_slice(b"RIFF");
    bytes.extend_from_slice(&(frames * 4 + 36).to_le_bytes());
    bytes.extend_from_slice(b"WAVEfmt ");
    bytes.extend_from_slice(&16u32.to_le_bytes());
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.extend_from_slice(&2u16.to_le_bytes());
    bytes.extend_from_slice(&RATE.to_le_bytes());
    bytes.extend_from_slice(&(RATE * 4).to_le_bytes());
    bytes.extend_from_slice(&4u16.to_le_bytes());
    bytes.extend_from_slice(&16u16.to_le_bytes());
    bytes.extend_from_slice(b"data");
    bytes.extend_from_slice(&(frames * 4).to_le_bytes());
    for frame in 0..frames {
        for hz in [frequency, frequency * 2.0] {
            bytes.extend_from_slice(
                &((0.2
                    * f64::from(i16::MAX)
                    * (TAU * hz * f64::from(frame) / f64::from(RATE)).sin())
                    as i16)
                    .to_le_bytes(),
            );
        }
    }
    std::fs::write(path, bytes).unwrap();
}

struct Receiver {
    room: Room,
    stream: NativeAudioStream,
    publication: [u8; 16],
    key: [u8; 32],
}

impl Receiver {
    async fn join(bot: &mut Bot, identity: &BotIdentity, instance: &ActivityInstance) -> Self {
        let request = Id::new_v4();
        let epoch = activity_audio_epoch(instance);
        bot.send(&ClientMessage::RequestActivityAudio {
            instance_id: instance.id,
            request_id: request,
            epoch: epoch.clone(),
            role: ActivityAudioRole::Listener,
        })
        .await
        .unwrap();
        let mut grant = None;
        let mut material = None;
        let rendezvous = tokio::time::timeout(Duration::from_secs(20), async {
            while grant.is_none() || material.is_none() {
                match bot.next_event().await.unwrap() {
                    ServerMessage::ActivityAudioToken {
                        request_id,
                        role,
                        livekit_url,
                        token,
                        ..
                    } if request_id == request && role == ActivityAudioRole::Listener => {
                        grant = Some((livekit_url, token))
                    }
                    ServerMessage::ActivityAudioError { request_id, .. }
                        if request_id == request =>
                    {
                        panic!("test listener was not authorized")
                    }
                    ServerMessage::ActivityEvent { from, data, .. } if from == instance.app => {
                        let value: Value = serde_json::from_str(&data).unwrap();
                        if value["__dxaudio"] != "key" {
                            continue;
                        }
                        let context: KeyContext =
                            serde_json::from_value(value["context"].clone()).unwrap();
                        if context.request != *request.as_bytes() {
                            continue;
                        }
                        assert_eq!(context.instance, *instance.id.as_bytes());
                        assert_eq!(context.channel, *instance.channel_id.as_bytes());
                        assert_eq!(hex::encode(context.epoch), epoch);
                        let key = identity
                            .open_activity_audio_key(
                                &instance.app,
                                &context,
                                value["ciphertext"].as_str().unwrap(),
                            )
                            .unwrap();
                        material = Some((context.publication, *key.as_bytes()));
                    }
                    _ => {}
                }
            }
        })
        .await;
        assert!(
            rendezvous.is_ok(),
            "music rendezvous timed out: grant={}, key={}",
            grant.is_some(),
            material.is_some()
        );
        let (url, token) = grant.unwrap();
        let (publication, key) = material.unwrap();
        static PROBED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
        if let Some(binary) = std::env::var_os("MUSIC_TEST_WEBVIEW_PROBE")
            && !PROBED.swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            command(bot, instance, "play").await;
            let mut probe = tokio::process::Command::new(binary)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::null())
                .kill_on_drop(true)
                .spawn()
                .expect("spawn explicitly selected native WebView probe");
            let options = json!({"url":url,"token":token,"keyHex":hex::encode(key),"publisher":format!("music-service:{}",instance.app),"request":request,"iceServers":[]});
            use tokio::io::AsyncWriteExt;
            let mut input = probe.stdin.take().unwrap();
            input
                .write_all(options.to_string().as_bytes())
                .await
                .unwrap();
            input.shutdown().await.unwrap();
            drop(input);
            let output = tokio::time::timeout(Duration::from_secs(90), probe.wait_with_output())
                .await
                .expect("native WebView probe timed out")
                .unwrap();
            assert!(output.status.success());
            assert!(
                String::from_utf8_lossy(&output.stdout).contains("MUSIC_WEBVIEW_PCM_VERIFIED"),
                "native WebView did not confirm decoded PCM: {}",
                String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .filter(|s| s.starts_with("MUSIC_WEBVIEW_"))
                    .collect::<Vec<_>>()
                    .join("; ")
            );
        }
        let connected = Room::connect(&url, token.as_str(), music_room_options(key, true)).await;
        assert!(connected.is_ok(), "test music room failed to connect");
        let (room, mut events) = connected.unwrap();
        let audio = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if let Some(RoomEvent::TrackSubscribed {
                    track: RemoteTrack::Audio(audio),
                    participant,
                    ..
                }) = events.recv().await
                {
                    assert_eq!(
                        participant.identity().as_str(),
                        format!("music-service:{}", instance.app)
                    );
                    return audio;
                }
            }
        })
        .await
        .expect("test music publication not received");
        let stream = NativeAudioStream::new(audio.rtc_track(), RATE as i32, 2);
        Self {
            room,
            stream,
            publication,
            key,
        }
    }

    async fn power(&mut self) -> (f64, [f64; 2]) {
        for _ in 0..1000 {
            if self.stream.next().now_or_never().flatten().is_none() {
                break;
            }
        }
        let mut count = 0u64;
        let mut power = 0.0;
        let mut bands = [[0.0f64; 2]; 2];
        let timer = tokio::time::sleep(Duration::from_millis(500));
        tokio::pin!(timer);
        loop {
            tokio::select! {
                _=&mut timer=>break,
                frame=self.stream.next()=>{let Some(frame)=frame else {break};for pair in frame.data.as_chunks::<2>().0 {
                    let sample=f64::from(pair[0])/f64::from(i16::MAX);power+=sample*sample;
                    for (i,hz) in [440.0,660.0].iter().enumerate(){let angle=TAU*hz*count as f64/f64::from(RATE);bands[i][0]+=sample*angle.sin();bands[i][1]+=sample*angle.cos();}
                    count+=1;
                }}
            }
        }
        (
            power / count.max(1) as f64,
            bands.map(|b| b[0] * b[0] + b[1] * b[1]),
        )
    }

    async fn close(mut self) {
        self.stream.close();
        self.room.close().await.unwrap();
    }
}

#[test]
#[ignore = "needs an explicitly configured isolated loopback SFU; generated tones only"]
fn local_activity_audio_is_authorized_shared_controlled_and_rekeyed() {
    let mut sfu = support::TestSfu::configured();
    let directory = tempfile::tempdir().unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(scenario(&directory, &sfu));
    drop(runtime);
    sfu.close().expect("owned test SFU cleanup must finish");
    let path = directory.keep();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        match std::fs::remove_dir_all(&path) {
            Ok(()) => break,
            Err(_) if std::time::Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(25))
            }
            Err(error) => panic!("synthetic test directory cleanup failed: {error}"),
        }
    }
}

async fn scenario(directory: &tempfile::TempDir, sfu: &support::TestSfu) {
    let first = directory.path().join("first.wav");
    let second = directory.path().join("second.wav");
    wave(&first, 440.0);
    wave(&second, 660.0);
    let library = Library::open(vec![first, second]).await.unwrap();
    let restart_library = library.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let config = ServerConfig {
        livekit: LiveKitConfig::new(
            directory.path(),
            Some(sfu.url.clone()),
            Some((sfu.key.clone(), sfu.secret.clone())),
            vec![],
        ),
        operators: Default::default(),
        data_dir: directory.path().join("gateway"),
        identities: discordia_server::local_identities(address.port()),
        launch: Default::default(),
        admission: Default::default(),
    };
    let (router, shutdown) = discordia_server::build_gateway(config).await.unwrap();
    let gateway = discordia_server::serve_router(listener, router);
    let url = format!("ws://{address}");
    let (mut owner, owner_id) = user(&url, "Local audio owner").await;
    owner
        .send(&ClientMessage::CreateGuild {
            name: "Isolated local music test".into(),
            template: None,
        })
        .await
        .unwrap();
    let (guild, channel) = wait(&mut owner, |event| match event {
        ServerMessage::GuildJoined {
            guild, channels, ..
        } => Some((
            guild.id,
            channels
                .into_iter()
                .find(|c| c.kind == ChannelKind::Text)
                .unwrap()
                .id,
        )),
        _ => None,
    })
    .await;
    let app = BotIdentity::generate();
    let restart_identity = BotIdentity::from_base58_secret(&app.secret_base58()).unwrap();
    let app_key = app.pubkey().to_string();
    let manifest = audio_manifest();
    let mut bot = Bot::connect(&url, &app, &manifest.name).await.unwrap();
    wait(&mut bot, |e| {
        matches!(e, ServerMessage::Ready { .. }).then_some(())
    })
    .await;
    bot.register_manifest(&app, &manifest).await.unwrap();
    let signed_manifest = app.sign_manifest(&manifest);
    let (stop, receiver) = tokio::sync::watch::channel(false);
    let service =
        tokio::spawn(async move { runtime::run_audio(bot, &app, library, receiver).await });
    owner
        .send(&ClientMessage::InstallBot {
            guild_id: guild,
            bot_pubkey: app_key.clone(),
            name: String::new(),
            permissions: vec![],
            intents: vec![],
            manifest: Some(signed_manifest),
        })
        .await
        .unwrap();
    wait(&mut owner, |e| match e {
        ServerMessage::GuildActivities { activities, .. }
            if activities.iter().any(|a| a.bot_pubkey == app_key) =>
        {
            Some(())
        }
        _ => None,
    })
    .await;
    owner
        .send(&ClientMessage::ActivityStart {
            channel_id: channel,
            app: app_key.clone(),
        })
        .await
        .unwrap();
    let initial = wait(&mut owner, |e| match e {
        ServerMessage::ActivityStarted { instance } => Some(instance),
        _ => None,
    })
    .await;
    let (mut guest, guest_id) = user(&url, "Local audio spectator").await;
    guest
        .send(&ClientMessage::JoinGuild {
            guild_id: guild,
            accept: true,
            pow_nonce: None,
        })
        .await
        .unwrap();
    wait(&mut guest, |e| {
        matches!(e, ServerMessage::GuildJoined { .. }).then_some(())
    })
    .await;
    guest
        .send(&ClientMessage::ActivityJoin {
            instance_id: initial.id,
            role: ActivityRole::Spectator,
        })
        .await
        .unwrap();
    let current = wait(&mut owner, |e| match e {
        ServerMessage::ActivityInstances { instances, .. } => instances
            .into_iter()
            .find(|i| i.id == initial.id && i.participants.len() == 2),
        _ => None,
    })
    .await;
    let owner_audio = Receiver::join(&mut owner, &owner_id, &current).await;
    let guest_audio = Receiver::join(&mut guest, &guest_id, &current).await;
    assert_eq!(owner_audio.publication, guest_audio.publication);
    assert_eq!(owner_audio.key, guest_audio.key);
    command(&mut guest, &current, "play").await;
    wait(&mut guest, |e| match e {
        ServerMessage::Error { message } => {
            assert!(message.contains("only players"));
            Some(())
        }
        _ => None,
    })
    .await;
    guest
        .send(&ClientMessage::ActivityJoin {
            instance_id: current.id,
            role: ActivityRole::Player,
        })
        .await
        .unwrap();
    let current = wait(&mut owner, |e| match e {
        ServerMessage::ActivityInstances { instances, .. } => instances.into_iter().find(|i| {
            i.id == current.id && i.role_of(guest_id.pubkey()) == Some(ActivityRole::Player)
        }),
        _ => None,
    })
    .await;
    owner_audio.close().await;
    guest_audio.close().await;
    let mut owner_audio = Receiver::join(&mut owner, &owner_id, &current).await;
    let mut guest_audio = Receiver::join(&mut guest, &guest_id, &current).await;
    command(&mut guest, &current, "play").await;
    phase(&mut owner, &app_key, "playing", 0).await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    let (a, b) = tokio::join!(owner_audio.power(), guest_audio.power());
    assert!(
        a.0 > 0.001 && b.0 > 0.001,
        "both participants must decode the shared song"
    );
    assert!(a.1[0] > a.1[1] * 10.0);
    command(&mut owner, &current, "pause").await;
    phase(&mut owner, &app_key, "paused", 0).await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    let (a, b) = tokio::join!(owner_audio.power(), guest_audio.power());
    assert!(
        a.0 < 0.0001 && b.0 < 0.0001,
        "pause must publish silence to both participants"
    );
    command(&mut guest, &current, "next").await;
    phase(&mut owner, &app_key, "paused", 1).await;
    command(&mut owner, &current, "play").await;
    phase(&mut owner, &app_key, "playing", 1).await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    let (power, bands) = owner_audio.power().await;
    assert!(
        power > 0.001 && bands[1] > bands[0] * 10.0,
        "next must select the second operator-configured file"
    );
    wait(&mut owner, |e| match e {
        ServerMessage::ActivityEvent { from, data, .. } if from == app_key => {
            let value: Value = serde_json::from_str(&data).unwrap();
            (value["t"] == "playback_state"
                && value["phase"] == "playing"
                && value["track"] == 1
                && value["position_secs"].as_u64().is_some_and(|v| v >= 2))
            .then_some(())
        }
        _ => None,
    })
    .await;
    guest
        .send(&ClientMessage::ActivityLeave {
            instance_id: current.id,
        })
        .await
        .unwrap();
    let changed = wait(&mut owner, |e| match e {
        ServerMessage::ActivityInstances { instances, .. } => instances
            .into_iter()
            .find(|i| i.id == initial.id && i.participants.len() == 1),
        _ => None,
    })
    .await;
    let mut replacement = Receiver::join(&mut owner, &owner_id, &changed).await;
    assert_ne!(owner_audio.publication, replacement.publication);
    assert_ne!(owner_audio.key, replacement.key);
    let replacement_id = Id::from_bytes(replacement.publication).to_string();
    wait(&mut owner, |e| match e {
        ServerMessage::ActivityEvent { from, data, .. } if from == app_key => {
            let value: Value = serde_json::from_str(&data).unwrap();
            (value["t"] == "playback_state"
                && value["publication"] == replacement_id
                && value["phase"] == "playing"
                && value["position_secs"].as_u64().is_some_and(|v| v >= 2))
            .then_some(())
        }
        _ => None,
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(replacement.power().await.0 > 0.001);
    assert!(
        guest_audio.power().await.0 < 0.0001,
        "departed listeners must not receive further song samples"
    );
    let request = Id::new_v4();
    guest
        .send(&ClientMessage::RequestActivityAudio {
            instance_id: changed.id,
            request_id: request,
            epoch: activity_audio_epoch(&changed),
            role: ActivityAudioRole::Listener,
        })
        .await
        .unwrap();
    wait(&mut guest, |e| {
        matches!(e,ServerMessage::ActivityAudioError {request_id,..} if request_id==request)
            .then_some(())
    })
    .await;
    stop.send_replace(true);
    assert!(
        tokio::time::timeout(Duration::from_secs(10), service)
            .await
            .unwrap()
            .unwrap()
            .is_ok(),
        "music service must close its publication on shutdown"
    );
    let restart_url = url.clone();
    let (_restart_stop, restart_receiver) = tokio::sync::watch::channel(false);
    let restarted = tokio::spawn(async move {
        runtime::connect_audio(
            &restart_url,
            &restart_identity,
            restart_library,
            restart_receiver,
        )
        .await
    });
    let mut second_device = Bot::connect_as_user(&url, &owner_id, "Local audio second device")
        .await
        .unwrap();
    wait(&mut second_device, |e| {
        matches!(e, ServerMessage::Ready { .. }).then_some(())
    })
    .await;
    let mut after_restart = Receiver::join(&mut second_device, &owner_id, &changed).await;
    assert_ne!(replacement.publication, after_restart.publication);
    assert_ne!(replacement.key, after_restart.key);
    command(&mut owner, &changed, "play").await;
    phase(&mut owner, &app_key, "playing", 0).await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(
        after_restart.power().await.0 > 0.001,
        "a second device must receive fresh music after service reconnect"
    );
    assert!(
        replacement.power().await.0 < 0.0001,
        "old keys must not decode a restarted publication in the same room"
    );
    shutdown.close_all();
    assert!(
        tokio::time::timeout(Duration::from_secs(10), restarted)
            .await
            .unwrap()
            .unwrap()
            .is_ok(),
        "gateway disconnect must stop the music publisher"
    );
    after_restart.close().await;
    replacement.close().await;
    owner_audio.close().await;
    guest_audio.close().await;
    gateway.abort();
}
