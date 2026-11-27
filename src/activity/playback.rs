use super::keys::PublicationKeys;
use crate::audio::{
    RATE,
    player::{Clock, Phase, Player},
    transport::{MediaError, Publisher},
};
use discordia_bot::{
    BotIdentity,
    protocol::{
        ActivityAudioRole, ActivityInstance, ActivityRole, ClientMessage, Id, ServerMessage,
        activity_audio_epoch,
    },
};
use futures_util::{StreamExt, stream::FuturesUnordered};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

const MAX_ROOMS: usize = 4;
const MAX_REQUESTS: usize = 128;

#[derive(Debug)]
pub struct CleanupFailure;

impl std::fmt::Display for CleanupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "music cleanup could not be verified; service stopped to avoid duplicate publishers",
        )
    }
}
impl std::error::Error for CleanupFailure {}

#[derive(Clone)]
pub struct Library(Arc<Vec<PathBuf>>);

impl Library {
    pub async fn open(files: Vec<PathBuf>) -> Result<Self, &'static str> {
        if files.is_empty() || files.len() > 16 {
            return Err("configure between one and 16 local audio files");
        }
        for file in &files {
            let metadata = tokio::fs::metadata(file)
                .await
                .map_err(|_| "cannot read a configured local audio file")?;
            if !metadata.is_file() || metadata.len() == 0 || metadata.len() > 512 * 1024 * 1024 {
                return Err("configured audio must be a nonempty regular file of at most 512 MiB");
            }
        }
        Ok(Self(Arc::new(files)))
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Control {
    Play,
    Pause,
    Next,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Command {
    t: String,
    request_id: Id,
    action: Control,
}

struct PendingKey {
    from: String,
    request: Id,
    epoch: String,
}

struct RoomState {
    instance: ActivityInstance,
    keys: Option<PublicationKeys>,
    grant: Option<(Id, Instant)>,
    generation: Id,
    worker: bool,
    stopping: bool,
    live: bool,
    cancel: Option<watch::Sender<bool>>,
    controls: Option<mpsc::Sender<Control>>,
    waiting: Vec<PendingKey>,
    recent: VecDeque<(String, Id)>,
    phase: &'static str,
    index: usize,
    playing: bool,
    position: u64,
    resume: Option<Box<Resume>>,
}

pub struct Resume {
    player: Player,
    index: usize,
}

pub enum Event {
    State {
        instance: Id,
        generation: Id,
        phase: &'static str,
        index: usize,
        position: u64,
    },
    Finished(Id, Id, bool, Option<&'static str>, Box<Resume>),
    Tick,
    Shutdown,
}

type Completion = (Id, Id, bool, Option<&'static str>, Box<Resume>);

struct WorkerConfig {
    url: String,
    token: String,
    key: [u8; 32],
    files: Arc<Vec<PathBuf>>,
    index: usize,
    playing: bool,
    instance: Id,
    generation: Id,
    resume: Option<Box<Resume>>,
}

pub struct Playback<'a> {
    identity: &'a BotIdentity,
    library: Library,
    rooms: HashMap<Id, RoomState>,
    jobs: FuturesUnordered<JoinHandle<Completion>>,
    events: mpsc::Receiver<Event>,
    reporter: mpsc::Sender<Event>,
    shutdown: watch::Receiver<bool>,
    outbox: VecDeque<ClientMessage>,
    timer: tokio::time::Interval,
    unsafe_cleanup: bool,
}

impl<'a> Playback<'a> {
    pub fn new(
        identity: &'a BotIdentity,
        library: Library,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        let (reporter, events) = mpsc::channel(128);
        let mut timer = tokio::time::interval(Duration::from_secs(1));
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        Self {
            identity,
            library,
            rooms: HashMap::new(),
            jobs: FuturesUnordered::new(),
            events,
            reporter,
            shutdown,
            outbox: VecDeque::new(),
            timer,
            unsafe_cleanup: false,
        }
    }

    pub fn pop(&mut self) -> Option<ClientMessage> {
        self.outbox.pop_front()
    }

    pub fn stopping(&self) -> bool {
        *self.shutdown.borrow()
    }

    pub fn cleanup_failed(&self) -> bool {
        self.unsafe_cleanup
    }

    fn send(&mut self, instance: Id, to: Option<&str>, data: Value) {
        self.outbox.push_back(ClientMessage::ActivitySend {
            instance_id: instance,
            to: to.map(str::to_string),
            data: data.to_string(),
        });
    }

    pub fn snapshot(&mut self, instance: Id, to: Option<&str>, ack: Option<Id>) {
        let state = self.rooms.get(&instance);
        self.send(
            instance,
            to,
            json!({"t":"playback_state", "available":true, "ack":ack,
            "phase":state.map_or("stopped",|s| s.phase), "track":state.map_or(0,|s|s.index),
            "tracks":self.library.0.len(), "position_secs":state.map_or(0,|s|s.position), "publication":state.map(|s|s.generation)}),
        );
    }

    fn request_grant(&mut self, id: Id) -> Result<(), &'static str> {
        let state = self.rooms.get_mut(&id).ok_or("music session unavailable")?;
        let keys = PublicationKeys::new(state.instance.clone())?;
        let (generation, _) = keys.material().ok_or("music audio unavailable")?;
        let request = Id::new_v4();
        state.keys = Some(keys);
        state.generation = generation;
        state.grant = Some((request, Instant::now()));
        state.phase = "connecting";
        state.stopping = false;
        self.outbox.push_back(ClientMessage::RequestActivityAudio {
            instance_id: id,
            request_id: request,
            epoch: activity_audio_epoch(&state.instance),
            role: ActivityAudioRole::Publisher,
        });
        Ok(())
    }

    fn ensure(&mut self, instance: &ActivityInstance) -> Result<(), &'static str> {
        if !self.rooms.contains_key(&instance.id) {
            if self.rooms.len() >= MAX_ROOMS {
                return Err("this service supports four simultaneous local audio sessions");
            }
            self.rooms.insert(
                instance.id,
                RoomState {
                    instance: instance.clone(),
                    keys: None,
                    grant: None,
                    generation: Id::nil(),
                    worker: false,
                    stopping: false,
                    live: true,
                    cancel: None,
                    controls: None,
                    waiting: Vec::new(),
                    recent: VecDeque::new(),
                    phase: "stopped",
                    index: 0,
                    playing: false,
                    position: 0,
                    resume: None,
                },
            );
        }
        let state = self
            .rooms
            .get(&instance.id)
            .ok_or("music session unavailable")?;
        if !state.worker && state.grant.is_none() && state.keys.is_none() {
            self.request_grant(instance.id)?;
        }
        Ok(())
    }

    fn stop_room(&mut self, id: Id) {
        let Some(state) = self.rooms.get_mut(&id) else {
            return;
        };
        let publication = state
            .keys
            .as_ref()
            .and_then(|k| k.material())
            .map(|(id, _)| id);
        state.keys = None;
        state.grant = None;
        state.stopping = true;
        if let Some(cancel) = &state.cancel {
            cancel.send_replace(true);
        }
        if let Some(publication) = publication {
            self.send(
                id,
                None,
                json!({"__dxaudio":"stop", "publication":publication}),
            );
        }
    }

    pub async fn sync(
        &mut self,
        channel: Id,
        instances: &[ActivityInstance],
    ) -> Result<(), &'static str> {
        let ids: Vec<Id> = self
            .rooms
            .iter()
            .filter(|(_, s)| s.instance.channel_id == channel)
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            let next = instances.iter().find(|i| {
                i.id == id && i.app == self.identity.pubkey() && !i.participants.is_empty()
            });
            let changed = self.rooms.get(&id).is_some_and(|s| {
                next.is_none_or(|i| activity_audio_epoch(i) != activity_audio_epoch(&s.instance))
            });
            if !changed {
                continue;
            }
            self.stop_room(id);
            let Some(state) = self.rooms.get_mut(&id) else {
                continue;
            };
            state.waiting.clear();
            state.live = next.is_some();
            if let Some(next) = next {
                state.instance = next.clone();
            }
            if !state.worker {
                if state.live {
                    self.request_grant(id)?;
                } else {
                    self.discard_resume(id).await?;
                    self.rooms.remove(&id);
                }
            }
        }
        Ok(())
    }

    pub fn dispatch(&mut self, instance: &ActivityInstance, from: &str, data: &str) -> bool {
        let Ok(value) = serde_json::from_str::<Value>(data) else {
            return false;
        };
        if value.get("__dxaudio").is_some() {
            let request = value
                .get("request_id")
                .and_then(Value::as_str)
                .and_then(|v| v.parse::<Id>().ok());
            let epoch = value.get("epoch").and_then(Value::as_str).unwrap_or("");
            if value.get("__dxaudio").and_then(Value::as_str) != Some("key_request")
                || request.is_none_or(|r| r.is_nil())
                || instance.role_of(from).is_none()
                || epoch != activity_audio_epoch(instance)
            {
                return true;
            }
            let Some(request) = request else { return true };
            if let Err(message) = self.ensure(instance) {
                self.error(instance.id, from, request, message);
                return true;
            }
            if let Some(state) = self.rooms.get_mut(&instance.id) {
                if state.waiting.len() >= MAX_REQUESTS
                    || state.waiting.iter().filter(|p| p.from == from).count() >= 2
                {
                    self.error(
                        instance.id,
                        from,
                        request,
                        "too many pending listening requests",
                    );
                    return true;
                }
                state.waiting.push(PendingKey {
                    from: from.into(),
                    request,
                    epoch: epoch.into(),
                });
            }
            self.flush_keys(instance.id);
            return true;
        }
        if value.get("t").and_then(Value::as_str) != Some("playback") {
            return false;
        }
        let Ok(command) = serde_json::from_value::<Command>(value) else {
            return true;
        };
        if command.t != "playback" || command.request_id.is_nil() {
            return true;
        }
        if instance.role_of(from) != Some(ActivityRole::Player) {
            self.error(
                instance.id,
                from,
                command.request_id,
                "only participating players can control local audio",
            );
            return true;
        }
        if let Err(message) = self.ensure(instance) {
            self.error(instance.id, from, command.request_id, message);
            return true;
        }
        let Some(state) = self.rooms.get_mut(&instance.id) else {
            return true;
        };
        if state.stopping {
            self.error(
                instance.id,
                from,
                command.request_id,
                "audio is reconnecting; retry shortly",
            );
            return true;
        }
        if !state
            .recent
            .iter()
            .any(|(key, id)| key == from && *id == command.request_id)
        {
            if let Some(controls) = state.controls.as_ref().filter(|_| !state.stopping)
                && controls.try_send(command.action).is_err()
            {
                self.error(
                    instance.id,
                    from,
                    command.request_id,
                    "audio controls are busy; retry shortly",
                );
                return true;
            }
            match command.action {
                Control::Play => state.playing = true,
                Control::Pause => state.playing = false,
                Control::Next if state.controls.is_none() || state.stopping => {
                    state.index = (state.index + 1).min(self.library.0.len() - 1)
                }
                Control::Next => {}
            }
            if state.recent.len() == 64 {
                state.recent.pop_front();
            }
            state.recent.push_back((from.into(), command.request_id));
        }
        self.snapshot(instance.id, None, Some(command.request_id));
        true
    }

    fn error(&mut self, instance: Id, to: &str, request: Id, message: &'static str) {
        self.send(
            instance,
            Some(to),
            json!({"__dxaudio":"error", "request_id":request}),
        );
        self.send(
            instance,
            Some(to),
            json!({"t":"error", "request_id":request, "message":message}),
        );
    }

    fn flush_keys(&mut self, id: Id) {
        let Some(state) = self.rooms.get_mut(&id).filter(|s| s.worker && !s.stopping) else {
            return;
        };
        let waiting = std::mem::take(&mut state.waiting);
        let Some(keys) = state.keys.as_ref() else {
            return;
        };
        for request in waiting {
            let data = match keys.seal(
                self.identity,
                &request.from,
                request.request,
                &request.epoch,
            ) {
                Ok((context, ciphertext)) => {
                    json!({"__dxaudio":"key", "context":context, "ciphertext":ciphertext})
                }
                Err(_) => json!({"__dxaudio":"error", "request_id":request.request}),
            };
            self.outbox.push_back(ClientMessage::ActivitySend {
                instance_id: id,
                to: Some(request.from),
                data: data.to_string(),
            });
        }
    }

    pub async fn gateway(&mut self, message: ServerMessage) -> Result<(), &'static str> {
        match message {
            ServerMessage::ActivityAudioToken {
                instance_id,
                request_id,
                epoch,
                role,
                livekit_url,
                token,
                expires_in_secs,
                ..
            } => {
                let Some(state) = self.rooms.get_mut(&instance_id).filter(|s| {
                    s.grant.is_some_and(|(r, at)| {
                        r == request_id && at.elapsed() < Duration::from_secs(15)
                    }) && role == ActivityAudioRole::Publisher
                        && activity_audio_epoch(&s.instance) == epoch
                        && !s.worker
                        && s.live
                }) else {
                    return Ok(());
                };
                if !(1..=60).contains(&expires_in_secs) {
                    self.fail(instance_id, "audio credentials are unavailable")
                        .await?;
                    return Ok(());
                }
                let Some((generation, key)) = state.keys.as_ref().and_then(|k| k.material()) else {
                    return Ok(());
                };
                let key = *key.as_bytes();
                let files = Arc::clone(&self.library.0);
                let reporter = self.reporter.clone();
                let (cancel, stop) = watch::channel(false);
                let (controls, commands) = mpsc::channel(16);
                let index = state.index;
                let playing = state.playing;
                let resume = state.resume.take();
                state.cancel = Some(cancel);
                state.controls = Some(controls);
                state.worker = true;
                state.grant = None;
                self.jobs.push(tokio::spawn(async move {
                    let (closed, error, resume) = worker(
                        WorkerConfig {
                            url: livekit_url,
                            token: token.into_inner(),
                            key,
                            files,
                            index,
                            playing,
                            instance: instance_id,
                            generation,
                            resume,
                        },
                        stop,
                        commands,
                        reporter,
                    )
                    .await;
                    (instance_id, generation, closed, error, resume)
                }));
                self.flush_keys(instance_id);
            }
            ServerMessage::ActivityAudioError {
                instance_id,
                request_id,
                ..
            } if self
                .rooms
                .get(&instance_id)
                .is_some_and(|s| s.grant.is_some_and(|(id, _)| id == request_id)) =>
            {
                self.fail(instance_id, "the host could not authorize local audio")
                    .await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn discard_resume(&mut self, id: Id) -> Result<(), &'static str> {
        if let Some(mut resume) = self.rooms.get_mut(&id).and_then(|s| s.resume.take())
            && resume.player.stop().await.is_err()
        {
            self.unsafe_cleanup = true;
            return Err("the local decoder did not stop cleanly");
        }
        Ok(())
    }

    async fn fail(&mut self, id: Id, message: &'static str) -> Result<(), &'static str> {
        self.stop_room(id);
        self.discard_resume(id).await?;
        let Some(state) = self.rooms.get_mut(&id) else {
            return Ok(());
        };
        state.phase = "failed";
        let waiting = std::mem::take(&mut state.waiting);
        for p in waiting {
            self.error(id, &p.from, p.request, message);
        }
        self.snapshot(id, None, None);
        Ok(())
    }

    pub async fn next(&mut self) -> Result<Event, &'static str> {
        if *self.shutdown.borrow() {
            return Ok(Event::Shutdown);
        }
        tokio::select! {
            _=self.shutdown.changed()=>Ok(Event::Shutdown),
            Some(event)=self.events.recv()=>Ok(event),
            Some(completed)=self.jobs.next(), if !self.jobs.is_empty()=>{
                let (id,generation,closed,error,resume)=match completed {Ok(value)=>value,Err(_)=>{self.unsafe_cleanup=true;return Err("audio worker failed unexpectedly");}};
                Ok(Event::Finished(id,generation,closed,error,resume))
            }
            _=self.timer.tick()=>Ok(Event::Tick),
        }
    }

    pub async fn apply(&mut self, event: Event) -> Result<(), &'static str> {
        match event {
            Event::State {
                instance,
                generation,
                phase,
                index,
                position,
            } => {
                if let Some(s) = self
                    .rooms
                    .get_mut(&instance)
                    .filter(|s| s.generation == generation && !s.stopping)
                {
                    s.phase = phase;
                    s.index = index;
                    s.position = position;
                    if phase == "ended" {
                        s.playing = false;
                    }
                    self.snapshot(instance, None, None);
                }
            }
            Event::Finished(id, generation, closed, error, mut resume) => {
                let Some(s) = self
                    .rooms
                    .get_mut(&id)
                    .filter(|s| s.generation == generation)
                else {
                    if resume.player.stop().await.is_err() {
                        self.unsafe_cleanup = true;
                        return Err("a superseded decoder did not stop cleanly");
                    }
                    return Ok(());
                };
                s.worker = false;
                s.cancel = None;
                s.controls = None;
                if !closed {
                    self.unsafe_cleanup = true;
                    let _decoder_stopped = resume.player.stop().await;
                    return Err("an old music room could not be closed; refusing replacement");
                }
                let restarting = s.live && s.stopping && error.is_none();
                if restarting {
                    s.index = resume.index;
                    s.position = resume.player.position_frames() / u64::from(RATE);
                    if resume.player.phase() == Phase::Ended {
                        s.playing = false;
                    }
                    s.resume = Some(resume);
                } else if resume.player.stop().await.is_err() {
                    self.unsafe_cleanup = true;
                    return Err("the local decoder did not stop cleanly");
                }
                if !s.live {
                    self.rooms.remove(&id);
                } else if restarting {
                    self.request_grant(id)?;
                } else {
                    self.fail(id, error.unwrap_or("audio publication stopped"))
                        .await?;
                }
            }
            Event::Tick => {
                let stale: Vec<Id> = self
                    .rooms
                    .iter()
                    .filter(|(_, s)| {
                        s.grant
                            .is_some_and(|(_, at)| at.elapsed() >= Duration::from_secs(15))
                    })
                    .map(|(id, _)| *id)
                    .collect();
                for id in stale {
                    self.fail(id, "audio authorization timed out").await?;
                }
            }
            Event::Shutdown => {}
        }
        Ok(())
    }

    pub fn stop_all(&mut self) {
        let ids: Vec<Id> = self.rooms.keys().copied().collect();
        for id in ids {
            self.stop_room(id);
            if let Some(state) = self.rooms.get_mut(&id) {
                state.live = false;
            }
        }
    }

    pub async fn close(&mut self) -> Result<(), &'static str> {
        let mut decoders_clean = true;
        for state in self.rooms.values_mut() {
            if let Some(cancel) = &state.cancel {
                cancel.send_replace(true);
            }
            state.keys = None;
            if let Some(mut resume) = state.resume.take() {
                decoders_clean &= resume.player.stop().await.is_ok();
            }
        }
        let result = tokio::time::timeout(Duration::from_secs(45), async {
            let mut clean = decoders_clean;
            while let Some(result) = self.jobs.next().await {
                match result {
                    Ok((_, _, closed, _, mut resume)) => {
                        clean &= closed;
                        clean &= resume.player.stop().await.is_ok();
                    }
                    Err(_) => clean = false,
                }
            }
            clean
        })
        .await;
        self.rooms.clear();
        if matches!(result, Ok(true)) {
            Ok(())
        } else {
            self.unsafe_cleanup = true;
            Err("music publication cleanup did not finish cleanly")
        }
    }
}

impl Drop for Playback<'_> {
    fn drop(&mut self) {
        for state in self.rooms.values_mut() {
            if let Some(cancel) = &state.cancel {
                cancel.send_replace(true);
            }
        }
    }
}

async fn worker(
    config: WorkerConfig,
    mut stop: watch::Receiver<bool>,
    mut controls: mpsc::Receiver<Control>,
    reporter: mpsc::Sender<Event>,
) -> (bool, Option<&'static str>, Box<Resume>) {
    let WorkerConfig {
        url,
        token,
        key,
        files,
        mut index,
        playing,
        instance,
        generation,
        resume,
    } = config;
    let (mut player, source_index) = match resume {
        Some(resume) => {
            let Resume { player, index } = *resume;
            (player, index)
        }
        None => (Player::default(), index),
    };
    let mut publisher = match Publisher::connect(&url, &token, key).await {
        Ok(p) => p,
        Err(MediaError::Close) => {
            return (
                false,
                Some("could not close the music publisher"),
                Box::new(Resume {
                    player,
                    index: source_index,
                }),
            );
        }
        Err(_) => {
            return (
                true,
                Some("could not connect the music publisher"),
                Box::new(Resume {
                    player,
                    index: source_index,
                }),
            );
        }
    };
    let run=async {
        if *stop.borrow() { return Ok(()); }
        if source_index!=index || matches!(player.phase(),Phase::Idle|Phase::Failed) {player.load(files[index].clone()).await.map_err(|_|"could not decode the configured local audio")?;}
        if playing { if player.phase()==Phase::Ended {player.load(files[index].clone()).await.map_err(|_|"could not restart local audio")?;} player.resume(); } else { player.pause(); }
        let mut clock=Clock::default(); let mut last=None;
        loop {
            tokio::select! { biased;
                _=stop.changed()=>return Ok(()),
                command=controls.recv()=>match command {
                    None=>return Ok(()),
                    Some(Control::Play)=>{
                        if player.phase()==Phase::Ended { player.load(files[index].clone()).await.map_err(|_|"could not restart local audio")?; }
                        player.resume();
                    }
                    Some(Control::Pause)=>player.pause(),
                    Some(Control::Next)=>{
                        let paused=player.phase()==Phase::Paused;
                        if index+1<files.len() { index+=1; player.load(files[index].clone()).await.map_err(|_|"could not load the next local audio file")?; if paused { player.pause(); } }
                    }
                },
                tick=clock.tick()=>{
                    tick.map_err(|_|"the music publication clock stalled")?;
                    let frame=player.next_frame().await.map_err(|_|"could not decode the configured local audio")?;
                    publisher.send(&frame).await.map_err(|_|"music publication disconnected")?;
                    if player.phase()==Phase::Ended && index+1<files.len() {
                        index+=1; player.load(files[index].clone()).await.map_err(|_|"could not load the next local audio file")?;
                    }
                    let phase=match player.phase() { Phase::Idle=>"stopped", Phase::Buffering=>"buffering",Phase::Playing=>"playing",Phase::Paused=>"paused",Phase::Ended=>"ended",Phase::Failed=>"failed" };
                    let snapshot=(phase,index,player.position_frames()/u64::from(RATE));
                    if last!=Some(snapshot) && reporter.try_send(Event::State {instance,generation,phase,index,position:snapshot.2}).is_ok() { last=Some(snapshot); }
                }
            }
        }
    }.await;
    let closed = publisher.close().await.is_ok();
    (closed, run.err(), Box::new(Resume { player, index }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use discordia_bot::{
        activity_audio::KeyContext,
        protocol::{ActivityParticipant, ActivityRole},
    };

    fn instance(app: &BotIdentity, owner: &BotIdentity) -> ActivityInstance {
        ActivityInstance {
            id: Id::new_v4(),
            channel_id: Id::new_v4(),
            app: app.pubkey().into(),
            max_players: 16,
            participants: vec![ActivityParticipant {
                pubkey: owner.pubkey().into(),
                role: ActivityRole::Player,
            }],
        }
    }
    fn library() -> Library {
        Library(Arc::new(vec![
            PathBuf::from("operator-selected-one.wav"),
            PathBuf::from("operator-selected-two.wav"),
        ]))
    }
    fn key_request(instance: &ActivityInstance, request: Id) -> String {
        json!({"__dxaudio":"key_request","request_id":request,"epoch":activity_audio_epoch(instance)}).to_string()
    }

    #[tokio::test]
    async fn a_player_who_is_not_the_host_can_play_pause_and_advance() {
        let app = BotIdentity::generate();
        let owner = BotIdentity::generate();
        let guest = BotIdentity::generate();
        let mut current = instance(&app, &owner);
        current.participants.push(ActivityParticipant {
            pubkey: guest.pubkey().into(),
            role: ActivityRole::Player,
        });
        assert_ne!(current.host(), Some(guest.pubkey()));
        let (_stop, receiver) = watch::channel(false);
        let mut audio = Playback::new(&app, library(), receiver);
        for action in ["play", "pause", "next"] {
            let command =
                json!({"t":"playback","request_id":Id::new_v4(),"action":action}).to_string();
            assert!(audio.dispatch(&current, guest.pubkey(), &command));
            if action == "play" {
                assert!(audio.rooms[&current.id].playing);
            }
        }
        assert!(!audio.rooms[&current.id].playing);
        assert_eq!(audio.rooms[&current.id].index, 1);
        assert_eq!(audio.rooms[&current.id].recent.len(), 3);
        assert!(!audio.outbox.iter().any(|m| match m {
            ClientMessage::ActivitySend { data, .. } =>
                serde_json::from_str::<Value>(data).unwrap()["t"] == "error",
            _ => false,
        }));
    }

    #[tokio::test]
    async fn permission_checks_and_command_deduplication_precede_audio_work() {
        let app = BotIdentity::generate();
        let owner = BotIdentity::generate();
        let outsider = BotIdentity::generate();
        let current = instance(&app, &owner);
        let (_stop, receiver) = watch::channel(false);
        let mut audio = Playback::new(&app, library(), receiver);
        let request = Id::new_v4();
        let next = json!({"t":"playback","request_id":request,"action":"next"}).to_string();
        audio.dispatch(&current, outsider.pubkey(), &next);
        assert!(audio.rooms.is_empty());
        audio.outbox.clear();
        audio.dispatch(&current, owner.pubkey(), &next);
        audio.dispatch(&current, owner.pubkey(), &next);
        assert_eq!(audio.rooms[&current.id].index, 1);
        assert_eq!(audio.rooms[&current.id].recent.len(), 1);
        assert_eq!(
            audio
                .outbox
                .iter()
                .filter(|m| matches!(m, ClientMessage::RequestActivityAudio { .. }))
                .count(),
            1
        );
        let bad=json!({"t":"playback","request_id":Id::new_v4(),"action":"play","path":"not-an-operator-file"}).to_string();
        audio.dispatch(&current, owner.pubkey(), &bad);
        assert!(!audio.rooms[&current.id].playing);
    }

    #[tokio::test]
    async fn rekey_waits_for_old_worker_cleanup_and_rejects_late_grants_and_events() {
        let app = BotIdentity::generate();
        let owner = BotIdentity::generate();
        let guest = BotIdentity::generate();
        let original = instance(&app, &owner);
        let (_stop, receiver) = watch::channel(false);
        let mut audio = Playback::new(&app, library(), receiver);
        audio.ensure(&original).unwrap();
        let old = audio.rooms[&original.id].generation;
        let old_request = audio.rooms[&original.id].grant.unwrap().0;
        let old_key = *audio.rooms[&original.id]
            .keys
            .as_ref()
            .unwrap()
            .material()
            .unwrap()
            .1
            .as_bytes();
        audio.rooms.get_mut(&original.id).unwrap().worker = true;
        audio.rooms.get_mut(&original.id).unwrap().grant = None;
        let mut changed = original.clone();
        changed.participants.push(ActivityParticipant {
            pubkey: guest.pubkey().into(),
            role: ActivityRole::Spectator,
        });
        audio.outbox.clear();
        audio
            .sync(original.channel_id, &[changed.clone()])
            .await
            .unwrap();
        let request = Id::new_v4();
        audio.dispatch(&changed, guest.pubkey(), &key_request(&changed, request));
        assert!(audio.rooms[&original.id].keys.is_none());
        assert!(audio.rooms[&original.id].grant.is_none());
        assert!(
            audio
                .outbox
                .iter()
                .all(|m| !matches!(m, ClientMessage::RequestActivityAudio { .. }))
        );
        audio
            .apply(Event::Finished(
                original.id,
                old,
                true,
                None,
                Box::new(Resume {
                    player: Player::default(),
                    index: 0,
                }),
            ))
            .await
            .unwrap();
        let fresh = audio.rooms[&original.id].generation;
        assert_ne!(old, fresh);
        audio
            .gateway(ServerMessage::ActivityAudioToken {
                instance_id: original.id,
                request_id: old_request,
                epoch: activity_audio_epoch(&original),
                role: ActivityAudioRole::Publisher,
                livekit_url: "ws://127.0.0.1:9".into(),
                livekit_urls: vec![],
                token: "unused-stale-test-token".to_string().into(),
                expires_in_secs: 60,
                ice_servers: vec![],
            })
            .await
            .unwrap();
        assert!(!audio.rooms[&original.id].worker);
        audio
            .apply(Event::State {
                instance: original.id,
                generation: old,
                phase: "playing",
                index: 0,
                position: 42,
            })
            .await
            .unwrap();
        assert_eq!(audio.rooms[&original.id].phase, "connecting");
        audio.rooms.get_mut(&original.id).unwrap().worker = true;
        audio.rooms.get_mut(&original.id).unwrap().grant = None;
        audio.flush_keys(original.id);
        let envelope = audio
            .outbox
            .iter()
            .find_map(|m| match m {
                ClientMessage::ActivitySend { data, .. } => {
                    let value: Value = serde_json::from_str(data).unwrap();
                    (value["__dxaudio"] == "key").then_some(value)
                }
                _ => None,
            })
            .unwrap();
        let context: KeyContext = serde_json::from_value(envelope["context"].clone()).unwrap();
        let received = guest
            .open_activity_audio_key(
                app.pubkey(),
                &context,
                envelope["ciphertext"].as_str().unwrap(),
            )
            .unwrap();
        assert_ne!(*received.as_bytes(), old_key);
        assert_eq!(context.request, *request.as_bytes());
    }

    #[tokio::test]
    async fn failed_close_prevents_replacement_and_room_counts_are_bounded() {
        let app = BotIdentity::generate();
        let owner = BotIdentity::generate();
        let (_stop, receiver) = watch::channel(false);
        let mut audio = Playback::new(&app, library(), receiver);
        let current = instance(&app, &owner);
        audio.ensure(&current).unwrap();
        let old = audio.rooms[&current.id].generation;
        assert!(
            audio
                .apply(Event::Finished(
                    current.id,
                    old,
                    false,
                    None,
                    Box::new(Resume {
                        player: Player::default(),
                        index: 0
                    })
                ))
                .await
                .is_err()
        );
        assert!(audio.cleanup_failed());
        for _ in 1..MAX_ROOMS {
            audio.ensure(&instance(&app, &owner)).unwrap();
        }
        assert!(audio.ensure(&instance(&app, &owner)).is_err());
        audio.close().await.unwrap();
        assert!(audio.rooms.is_empty());
    }

    #[tokio::test]
    async fn local_library_rejects_directories_empty_files_and_unbounded_lists() {
        let dir = tempfile::tempdir().unwrap();
        assert!(Library::open(vec![dir.path().into()]).await.is_err());
        let file = dir.path().join("empty.wav");
        tokio::fs::write(&file, []).await.unwrap();
        assert!(Library::open(vec![file.clone()]).await.is_err());
        assert!(Library::open(vec![file; 17]).await.is_err());
    }
}
