use super::{
    BUNDLE, manifest,
    session::{Command, Effect, Session},
};
use crate::probe::MetadataProbe;
use discordia_bot::{
    Bot, BotIdentity,
    protocol::{ActivityInstance, Id, MAX_ACTIVITY_DATA, ServerMessage},
};
use serde_json::json;
use std::{collections::HashMap, error::Error, sync::Arc, time::Duration};
use tokio::task::{AbortHandle, JoinSet};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const MAX_SESSIONS: usize = 16;
const MAX_PROBES: usize = 4;

pub async fn connect(server: &str, identity: &BotIdentity) -> Result<()> {
    let bot = registered(server, identity, &manifest()).await?;
    run(bot, identity.pubkey()).await
}

async fn registered(
    server: &str,
    identity: &BotIdentity,
    app: &discordia_bot::protocol::AppManifest,
) -> Result<Bot> {
    let mut bot = tokio::time::timeout(
        Duration::from_secs(20),
        Bot::connect(server, identity, &app.name),
    )
    .await??;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match bot.next_event().await {
                Some(ServerMessage::Ready { .. }) => return Ok(()),
                Some(ServerMessage::Error { .. }) | None => {
                    return Err("the host did not accept the music service");
                }
                _ => {}
            }
        }
    })
    .await??;
    tokio::time::timeout(Duration::from_secs(5), bot.register_manifest(identity, app)).await??;
    Ok(bot)
}

pub async fn run(mut bot: Bot, app_key: &str) -> Result<()> {
    run_inner(
        &mut bot,
        app_key,
        #[cfg(feature = "playback")]
        None,
    )
    .await
}

#[cfg(feature = "playback")]
pub async fn connect_audio(
    server: &str,
    identity: &BotIdentity,
    library: super::playback::Library,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let app = super::audio_manifest();
    let bot = tokio::select! {
        result=registered(server,identity,&app)=>result?,
        _=stop.changed()=>return Ok(()),
    };
    run_audio(bot, identity, library, stop).await
}

#[cfg(feature = "playback")]
type AudioEvent = super::playback::Event;
#[cfg(not(feature = "playback"))]
type AudioEvent = ();

async fn next_audio(
    #[cfg(feature = "playback")] audio: Option<&mut super::playback::Playback<'_>>,
) -> std::result::Result<AudioEvent, &'static str> {
    #[cfg(feature = "playback")]
    if let Some(audio) = audio {
        return audio.next().await;
    }
    std::future::pending().await
}

#[cfg(feature = "playback")]
pub async fn run_audio(
    bot: Bot,
    identity: &BotIdentity,
    library: super::playback::Library,
    stop: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    run_audio_inner(
        bot,
        identity,
        library,
        stop,
        #[cfg(feature = "soundcloud")]
        None,
    )
    .await
}

#[cfg(feature = "soundcloud")]
pub async fn connect_soundcloud(
    server: &str,
    identity: &BotIdentity,
    library: super::playback::Library,
    source: Arc<crate::soundcloud::SoundCloud>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let manifest = super::audio_manifest();
    let bot = tokio::select! { result=registered(server,identity,&manifest)=>result?, _=stop.changed()=>return Ok(()) };
    run_audio_inner(bot, identity, library, stop, Some(source)).await
}

#[cfg(feature = "playback")]
async fn run_audio_inner(
    mut bot: Bot,
    identity: &BotIdentity,
    library: super::playback::Library,
    stop: tokio::sync::watch::Receiver<bool>,
    #[cfg(feature = "soundcloud")] source: Option<Arc<crate::soundcloud::SoundCloud>>,
) -> Result<()> {
    let mut audio = super::playback::Playback::new(identity, library, stop);
    #[cfg(feature = "soundcloud")]
    {
        audio.soundcloud = source;
    }
    let result = run_inner(&mut bot, identity.pubkey(), Some(&mut audio)).await;
    audio.stop_all();
    while let Some(message) = audio.pop() {
        if !matches!(
            tokio::time::timeout(Duration::from_secs(2), bot.send(&message)).await,
            Ok(Ok(()))
        ) {
            break;
        }
    }
    let closed = audio.close().await;
    if closed.is_err() || audio.cleanup_failed() {
        return Err(super::playback::CleanupFailure.into());
    }
    result
}

async fn next_source(
    #[cfg(feature = "soundcloud")] sources: &mut super::source_jobs::Sources,
) -> SourceCompletion {
    #[cfg(feature = "soundcloud")]
    {
        return sources.next().await;
    }
    #[cfg(not(feature = "soundcloud"))]
    std::future::pending().await
}
#[cfg(feature = "soundcloud")]
type SourceCompletion = super::source_jobs::Completion;
#[cfg(not(feature = "soundcloud"))]
type SourceCompletion = ();

async fn run_inner(
    bot: &mut Bot,
    app_key: &str,
    #[cfg(feature = "playback")] mut audio: Option<&mut super::playback::Playback<'_>>,
) -> Result<()> {
    let probe = Arc::new(MetadataProbe::new()?);
    let mut sessions: HashMap<Id, Session> = HashMap::new();
    let mut jobs = JoinSet::new();
    let mut handles: HashMap<(Id, Id), AbortHandle> = HashMap::new();
    #[cfg(feature = "soundcloud")]
    let mut sources = super::source_jobs::Sources::default();
    loop {
        tokio::select! {
            event = bot.next_event() => match event {
                None => return Ok(()),
                Some(ServerMessage::ActivityInstances { channel_id, instances }) => {
                    #[cfg(feature = "playback")]
                    if let Some(audio)=audio.as_mut() { audio.sync(channel_id,&instances).await?; }
                    let changed = sync(&mut sessions, channel_id, instances, app_key);
                    for id in changed {
                        send_state(bot, &sessions, id, None, None).await?;
                        #[cfg(feature = "playback")]
                        if let Some(audio)=audio.as_mut() { audio.snapshot(id,None,None); }
                    }
                }
                Some(ServerMessage::ActivityEvent { instance_id, from, data }) => {
                    let Some(session) = sessions.get_mut(&instance_id) else {
                        send_error(bot, instance_id, &from, None, "This session is unavailable. Only one Music Lab session per channel and 16 per service are supported.").await?;
                        continue;
                    };
                    if data.len() > 4096 {
                        send_error(bot, instance_id, &from, None, "That music command is too large.").await?;
                        continue;
                    }
                    #[cfg(feature="soundcloud")]
                    if let Some(audio)=audio.as_mut() && sources.dispatch(session,&from,&data,audio,bot).await? {continue;}
                    #[cfg(feature = "playback")]
                    if let Some(audio)=audio.as_mut() && audio.dispatch(&session.instance,&from,&data) {
                            while let Some(message)=audio.pop() {
                                if audio.stopping() {return Ok(());}
                                tokio::time::timeout(Duration::from_secs(5),bot.send(&message)).await??;
                            }
                            continue;
                    }
                    let command = match serde_json::from_str::<Command>(&data) {
                        Ok(command) => command,
                        Err(_) => {
                            send_error(bot, instance_id, &from, None, "Invalid music command.").await?;
                            continue;
                        }
                    };
                    let hello = matches!(command, Command::Hello { .. });
                    let request_id = command.request_id();
                    match session.apply(&from, command, jobs.len() < MAX_PROBES) {
                        Ok(effect) => {
                            if let Effect::Probe { entry_id, source } = effect {
                                let probe = Arc::clone(&probe);
                                let handle = jobs.spawn(async move { (instance_id, entry_id, probe.inspect(source).await) });
                                handles.insert((instance_id, entry_id), handle);
                            }
                            send_state(bot, &sessions, instance_id, hello.then_some(from.as_str()), Some(request_id)).await?;
                            #[cfg(feature = "playback")]
                            if hello && let Some(audio)=audio.as_mut() { audio.snapshot(instance_id,Some(&from),None); }
                        }
                        Err(message) => send_error(bot, instance_id, &from, Some(request_id), message).await?,
                    }
                }
                Some(ServerMessage::BundleWanted { sha256 }) => {
                    if manifest().activity.is_some_and(|a| a.sha256 == sha256) {
                        bot.offer_bundle(BUNDLE.as_bytes()).await?;
                    }
                }
                #[cfg(feature = "playback")]
                Some(message @ (ServerMessage::ActivityAudioToken {..} | ServerMessage::ActivityAudioError {..}))=>{
                    if let Some(audio)=audio.as_mut() { audio.gateway(message).await?; }
                }
                Some(ServerMessage::Error { message }) => eprintln!("The host rejected a music service request ({}).",if message.starts_with("rate limited") {"rate limit"} else {"permission or session"}),
                Some(_) => {}
            },
            completed = jobs.join_next(), if !jobs.is_empty() => {
                match completed {
                    Some(Ok((instance_id, entry_id, report))) => {
                        handles.remove(&(instance_id, entry_id));
                        if sessions.get_mut(&instance_id).is_some_and(|s| s.complete(entry_id, report)) {
                            send_state(bot, &sessions, instance_id, None, None).await?;
                        }
                    }
                    Some(Err(error)) if error.is_cancelled() => {}
                    Some(Err(_)) => return Err("a music source worker failed".into()),
                    None => {}
                }
            }
            _completion=next_source(#[cfg(feature="soundcloud")] &mut sources)=>{
                #[cfg(feature="soundcloud")]
                if let Some(audio)=audio.as_mut() {sources.complete(_completion,&sessions,audio,bot).await?;}
            }
            event=next_audio(#[cfg(feature = "playback")] audio.as_deref_mut())=>{
                #[cfg(feature = "playback")]
                if let Some(audio)=audio.as_mut() {
                    let event=event?;
                    if matches!(event,super::playback::Event::Shutdown) { return Ok(()); }
                    audio.apply(event).await?;
                }
                #[cfg(not(feature = "playback"))]
                event?;
            }
        }
        #[cfg(feature = "playback")]
        if let Some(audio) = audio.as_mut() {
            while let Some(message) = audio.pop() {
                if audio.stopping() {
                    return Ok(());
                }
                tokio::time::timeout(Duration::from_secs(5), bot.send(&message)).await??;
            }
        }
        #[cfg(feature = "soundcloud")]
        {
            for (id, from, request) in sources.retain(&sessions) {
                send_error(
                    bot,
                    id,
                    &from,
                    Some(request),
                    "Source preparation was canceled because the queue or access changed.",
                )
                .await?;
            }
            if let Some(audio) = audio.as_ref() {
                sources.advance(&sessions, audio, bot).await?;
            }
        }
        handles.retain(|(instance, entry), handle| {
            if sessions.get(instance).is_some_and(|s| s.pending(*entry)) {
                true
            } else {
                handle.abort();
                false
            }
        });
    }
}

pub fn sync(
    sessions: &mut HashMap<Id, Session>,
    channel: Id,
    instances: Vec<ActivityInstance>,
    app_key: &str,
) -> Vec<Id> {
    sessions.retain(|id, s| {
        s.instance.channel_id != channel
            || instances
                .iter()
                .any(|i| i.id == *id && i.app == app_key && !i.participants.is_empty())
    });
    let mut changed = Vec::new();
    for instance in instances
        .into_iter()
        .filter(|i| i.channel_id == channel && i.app == app_key && !i.participants.is_empty())
    {
        let id = instance.id;
        if let Some(session) = sessions.get_mut(&id) {
            session.update(instance);
        } else {
            if sessions.len() >= MAX_SESSIONS
                || sessions.values().any(|s| s.instance.channel_id == channel)
            {
                continue;
            }
            sessions.insert(id, Session::new(instance));
        }
        changed.push(id);
    }
    changed
}

async fn send_state(
    bot: &mut Bot,
    sessions: &HashMap<Id, Session>,
    instance: Id,
    to: Option<&str>,
    ack: Option<Id>,
) -> Result<()> {
    if let Some(session) = sessions.get(&instance) {
        let data = serde_json::to_string(&session.snapshot(ack))?;
        if data.len() > MAX_ACTIVITY_DATA {
            return Err("music snapshot exceeds the activity frame limit".into());
        }
        tokio::time::timeout(
            Duration::from_secs(5),
            bot.activity_send(instance, to, &data),
        )
        .await??;
    }
    Ok(())
}

async fn send_error(
    bot: &mut Bot,
    instance: Id,
    to: &str,
    request_id: Option<Id>,
    message: &str,
) -> Result<()> {
    tokio::time::timeout(
        Duration::from_secs(5),
        bot.activity_send(
            instance,
            Some(to),
            &json!({"t":"error", "request_id":request_id, "message":message}).to_string(),
        ),
    )
    .await??;
    Ok(())
}
