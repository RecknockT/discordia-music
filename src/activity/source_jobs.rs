use super::{
    playback::{Library, Playback},
    session::Session,
};
use crate::{
    soundcloud::Prepared,
    source::{Kind, Provider},
};
use discordia_bot::{
    Bot,
    protocol::{ActivityRole, Id, activity_audio_epoch},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};
use tokio::task::{AbortHandle, JoinSet};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub type Completion = Option<
    std::result::Result<
        (Id, Id, std::result::Result<Prepared, &'static str>),
        tokio::task::JoinError,
    >,
>;
struct Pending {
    entry: Id,
    request: Id,
    from: String,
    epoch: String,
    abort: AbortHandle,
}
#[derive(Default)]
pub struct Sources {
    jobs: JoinSet<(Id, Id, std::result::Result<Prepared, &'static str>)>,
    pending: HashMap<Id, Pending>,
    current: HashMap<Id, Id>,
    recent: VecDeque<(Id, String, Id)>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Select {
    t: String,
    request_id: Id,
    entry_id: Id,
}

impl Sources {
    pub async fn next(&mut self) -> Completion {
        if self.jobs.is_empty() {
            std::future::pending().await
        } else {
            self.jobs.join_next().await
        }
    }
    pub async fn dispatch(
        &mut self,
        session: &Session,
        from: &str,
        data: &str,
        audio: &mut Playback<'_>,
        bot: &mut Bot,
    ) -> Result<bool> {
        let value: Value = match serde_json::from_str(data) {
            Ok(value) => value,
            Err(_) => return Ok(false),
        };
        let select = value["t"] == "play_source";
        let next = value["t"] == "playback"
            && value["action"] == "next"
            && self.current.contains_key(&session.instance.id);
        if !select && !next {
            return Ok(false);
        }
        let request = value["request_id"]
            .as_str()
            .and_then(|s| s.parse::<Id>().ok())
            .filter(|id| !id.is_nil());
        let Some(request) = request else {
            return Ok(true);
        };
        if session.instance.role_of(from) != Some(ActivityRole::Player) {
            error(
                bot,
                session.instance.id,
                from,
                request,
                "only players select songs",
            )
            .await?;
            return Ok(true);
        }
        if self
            .recent
            .iter()
            .any(|(id, key, r)| *id == session.instance.id && key == from && *r == request)
        {
            audio.snapshot(session.instance.id, Some(from), Some(request));
            return Ok(true);
        }
        let entry = if select {
            match serde_json::from_value::<Select>(value) {
                Ok(command) if command.t == "play_source" && command.request_id == request => {
                    Some(command.entry_id)
                }
                _ => None,
            }
        } else {
            self.current
                .get(&session.instance.id)
                .and_then(|id| session.next_soundcloud(*id))
        };
        let Some(entry) = entry else {
            error(
                bot,
                session.instance.id,
                from,
                request,
                "there is no next playable SoundCloud track",
            )
            .await?;
            return Ok(true);
        };
        self.start(session, from, request, entry, audio, bot)
            .await?;
        Ok(true)
    }
    async fn start(
        &mut self,
        session: &Session,
        from: &str,
        request: Id,
        entry: Id,
        audio: &Playback<'_>,
        bot: &mut Bot,
    ) -> Result<()> {
        let id = session.instance.id;
        let Some(track) = session.entry(entry).filter(|e| {
            e.source.provider() == Provider::Soundcloud && e.source.kind() == Kind::Track
        }) else {
            error(
                bot,
                id,
                from,
                request,
                "select a SoundCloud track still present in this queue",
            )
            .await?;
            return Ok(());
        };
        let Some(adapter) = audio.soundcloud.clone() else {
            error(
                bot,
                id,
                from,
                request,
                "the operator has not configured SoundCloud playback",
            )
            .await?;
            return Ok(());
        };
        if self.pending.contains_key(&id) || self.jobs.len() >= 2 {
            error(
                bot,
                id,
                from,
                request,
                "another source is being prepared; retry shortly",
            )
            .await?;
            return Ok(());
        }
        let source = track.source.clone();
        let abort = self
            .jobs
            .spawn(async move { (id, request, adapter.prepare(&source).await) });
        self.pending.insert(
            id,
            Pending {
                entry,
                request,
                from: from.into(),
                epoch: activity_audio_epoch(&session.instance),
                abort,
            },
        );
        Ok(())
    }
    pub fn retain(&mut self, sessions: &HashMap<Id, Session>) -> Vec<(Id, String, Id)> {
        let mut canceled = Vec::new();
        self.pending.retain(|id, p| {
            let keep = sessions.get(id).is_some_and(|s| {
                s.entry(p.entry).is_some()
                    && s.instance.role_of(&p.from) == Some(ActivityRole::Player)
                    && activity_audio_epoch(&s.instance) == p.epoch
            });
            if !keep {
                p.abort.abort();
                if sessions
                    .get(id)
                    .is_some_and(|s| s.instance.role_of(&p.from).is_some())
                {
                    canceled.push((*id, p.from.clone(), p.request));
                }
            }
            keep
        });
        self.current.retain(|id, _| sessions.contains_key(id));
        canceled
    }
    pub async fn complete(
        &mut self,
        completion: Completion,
        sessions: &HashMap<Id, Session>,
        audio: &mut Playback<'_>,
        bot: &mut Bot,
    ) -> Result<()> {
        let Some(completion) = completion else {
            return Ok(());
        };
        let (id, request, result) = match completion {
            Ok(value) => value,
            Err(error) if error.is_cancelled() => return Ok(()),
            Err(_) => return Err("SoundCloud source worker failed".into()),
        };
        if !self.pending.get(&id).is_some_and(|p| p.request == request) {
            return Ok(());
        }
        let pending = self
            .pending
            .remove(&id)
            .ok_or("source request disappeared")?;
        let Some(session) = sessions.get(&id).filter(|s| {
            s.entry(pending.entry).is_some()
                && s.instance.role_of(&pending.from) == Some(ActivityRole::Player)
                && activity_audio_epoch(&s.instance) == pending.epoch
        }) else {
            return Ok(());
        };
        match result {
            Ok(track) => {
                if let Err(message) = audio
                    .replace(&session.instance, Library::prepared(track))
                    .await
                {
                    if audio.cleanup_failed() {
                        return Err(message.into());
                    }
                    error(bot, id, &pending.from, request, message).await?;
                    return Ok(());
                }
                self.current.insert(id, pending.entry);
                self.recent.push_back((id, pending.from, request));
                while self.recent.len() > 64 {
                    self.recent.pop_front();
                }
                audio.snapshot(id, None, Some(request));
            }
            Err(message) => error(bot, id, &pending.from, request, message).await?,
        }
        Ok(())
    }
    pub async fn advance(
        &mut self,
        sessions: &HashMap<Id, Session>,
        audio: &Playback<'_>,
        bot: &mut Bot,
    ) -> Result<()> {
        let ended: Vec<_> = self
            .current
            .iter()
            .filter(|(id, _)| audio.ended(**id) && !self.pending.contains_key(id))
            .map(|(id, entry)| (*id, *entry))
            .collect();
        for (id, entry) in ended {
            self.current.remove(&id);
            if let Some(session) = sessions.get(&id)
                && let Some(next) = session.next_soundcloud(entry)
                && let Some(host) = session.instance.host()
            {
                self.start(session, host, Id::new_v4(), next, audio, bot)
                    .await?;
            }
        }
        Ok(())
    }
}
async fn error(bot: &mut Bot, id: Id, from: &str, request: Id, message: &str) -> Result<()> {
    tokio::time::timeout(
        Duration::from_secs(5),
        bot.activity_send(
            id,
            Some(from),
            &json!({"t":"error","request_id":request,"message":message}).to_string(),
        ),
    )
    .await??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activity::session::{Command, Effect};
    use discordia_bot::{
        BotIdentity,
        protocol::{ActivityInstance, ActivityParticipant},
    };
    #[tokio::test]
    async fn removing_a_track_cancels_its_pending_download() {
        let owner = BotIdentity::generate();
        let app = BotIdentity::generate();
        let id = Id::new_v4();
        let mut session = Session::new(ActivityInstance {
            id,
            channel_id: Id::new_v4(),
            app: app.pubkey().into(),
            max_players: 16,
            participants: vec![ActivityParticipant {
                pubkey: owner.pubkey().into(),
                role: ActivityRole::Player,
            }],
        });
        let Effect::Probe { entry_id, .. } = session
            .apply(
                owner.pubkey(),
                Command::Add {
                    request_id: Id::new_v4(),
                    url: "https://soundcloud.com/test/song".into(),
                },
                true,
            )
            .unwrap()
        else {
            panic!("expected source probe")
        };
        let mut sources = Sources::default();
        let request = Id::new_v4();
        let abort = sources.jobs.spawn(std::future::pending());
        sources.pending.insert(
            id,
            Pending {
                entry: entry_id,
                request,
                from: owner.pubkey().into(),
                epoch: activity_audio_epoch(&session.instance),
                abort,
            },
        );
        let mut sessions = HashMap::from([(id, session)]);
        assert!(sources.retain(&sessions).is_empty());
        assert_eq!(sources.pending.len(), 1);
        sessions
            .get_mut(&id)
            .unwrap()
            .apply(
                owner.pubkey(),
                Command::Remove {
                    request_id: Id::new_v4(),
                    entry_id,
                },
                true,
            )
            .unwrap();
        assert_eq!(sources.retain(&sessions).len(), 1);
        assert!(sources.pending.is_empty());
        assert!(matches!(sources.next().await,Some(Err(error)) if error.is_cancelled()));
    }
}
