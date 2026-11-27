# Discordia Music — Music Lab

An optional, installable music activity with a separate Rust service for
[Discordia](https://github.com/ssotomayor/discordia). Nothing runs or installs
automatically in a Discordia community.

**Experimental:** shared metadata queue and opt-in local audio playback work.
SoundCloud playback has an experimental opt-in API connector; it still needs
live validation with operator credentials. Other provider audio and playlist
expansion are not implemented. Android/iOS activity
windows are not supported yet.

## Run and install

Requires Rust and Cargo. From this repository:

```powershell
cargo run --locked --features activity --bin discordia-music-activity -- --server 'ws://127.0.0.1:9000'
```

Loopback is for a Discordia server on the same computer. From another computer,
use that host's `quic://…` share address or its TLS-proxied WebSocket URL.

The service prints its public app key. A community administrator pastes it into
**Integrations → Install an app** and approves its capabilities. Members then
open Music Lab from the activity picker. The operator keeps this service
running; listeners do not install or run it themselves.

Keep `music.key` private and backed up: it identifies the installed application
across restarts. `--key-file` / `MUSIC_KEY_FILE` selects another location.
Never commit this file. Ctrl+C stops the service.

## Local audio preview

Requires a compatible Discordia client/host with the experimental
`activity-audio` build feature and an operator-owned SFU. It is off by default.
For an embedded desktop host, build Discordia with `--features activity-audio`.

```powershell
cargo run --locked --features playback --bin discordia-music-activity -- --server 'ws://127.0.0.1:9000' --audio-file 'C:\Music\first.wav' --audio-file 'C:\Music\second.mp3'
```

This mode uses a separate `music-local.key` identity by default. Install its
printed app key and explicitly approve both Instance and Audio capabilities.
Each participant presses Listen; all players may play, pause and advance.
Spectators may listen and change their own volume. Queue administration remains
host-only. No microphone is captured.

The operator supplies 1–16 regular files, each at most 512 MiB. The activity page
cannot choose file paths. These files form a local test sequence, separate from
the public-link metadata queue. Playback and queue state are ephemeral; restarting
the service resets them. Membership/role changes rotate the publication and key,
while preserving the decoder position after confirmed cleanup. Cleanup failures
stop automatic reconnect rather than risking duplicate publishers.

## SoundCloud connector (experimental)

The service, not each listener, needs a registered SoundCloud application.
Set `SOUNDCLOUD_CLIENT_ID` and `SOUNDCLOUD_CLIENT_SECRET` in its private environment;
never put credentials in the activity bundle or chat. Then run:

```powershell
cargo run --locked --features soundcloud --bin discordia-music-activity -- --server 'ws://127.0.0.1:9000' --soundcloud
```

Install the printed Audio app key as above. Players add a public SoundCloud
track to the queue and press its Play button; listeners press Listen. Players
share pause/resume/next. At song end, the service selects the next SoundCloud
track in queue order. Other providers remain metadata-only and are never
silently substituted. Albums, playlists, private tracks, previews and live audio
are not supported in this connector.

The adapter caches/refreshes API tokens, follows bounded provider-only HTTPS
redirects and finite MP3 HLS playlists, and backs off on rate limits. Preparation
is limited to two jobs (one per session), 120 seconds, 30-minute tracks, 512
segments and 128 MiB per prepared track. It finishes preparation before changing
the song; song changes preserve the audio publication and listener keys.
Membership changes still rotate keys. Removing a pending track or losing access
cancels its preparation. Prepared files are temporary and kept alive through
native decoder teardown. A real SoundCloud track has **not been validated** yet:
there are no registered app credentials in the test environment.

## Current capabilities

| Area | Status |
|---|---|
| Queue | Shared, bounded; player additions and host administration |
| YouTube, Spotify | Public link validation and track metadata lookup |
| SoundCloud | Metadata; experimental configured API playback, live validation pending |
| TIDAL | Link validation; no metadata adapter |
| Playlist expansion / provider audio | Not implemented; recognizing a link does not authorize retransmission |
| Local audio | Opt-in WAV/PCM, MP3, Ogg/Vorbis; shared play/pause/next |
| Transport | Separate encrypted LiveKit music room and per-participant NIP-44 keys |
| Default install | Metadata only, Instance permission; no audio permission |

## Development

SDK/native patches use pinned Discordia Git revisions. The integration-test
server includes the authorized spectator key-request broker. This repository
owns the service/bundle; Discordia owns installation, permissions and trusted
client reception. No music accounts or provider playback integration exists.

On Windows, keep the native checkout/build path short (legacy C++ path limits).
Linux native builds need LLVM 21, `libasound2-dev`, `libglib2.0-dev`, `libssl-dev`
and `pkg-config`; CI installs them.

```powershell
$env:LIVEKIT_BUNDLE_SKIP='1'
cargo fmt --all --check
cargo test --locked --features activity,audio,audio-keys
cargo clippy --locked --all-features --all-targets -- -D warnings
cargo test --locked --all-features
node --test tests/activity_ui.cjs
```

`LIVEKIT_BUNDLE_SKIP` is only for tests, not a host needing its bundled SFU.
The ignored full local-activity test can launch an explicitly selected LiveKit
1.13.9 binary on loopback and verifies its exit:

```powershell
$env:MUSIC_TEST_SFU_BINARY='C:\Tools\livekit-server.exe'
cargo test --locked --all-features --test activity_media -- --ignored --test-threads=1
```

It generates tones, with no microphone/speaker or real account usage. A supplied
loopback `MUSIC_TEST_SFU_URL` with `MUSIC_TEST_API_KEY` / `MUSIC_TEST_API_SECRET`
uses that existing test SFU without stopping it. Two native SDK receivers are validated. On Windows, build Discordia's
`activity_audio_probe` example and set `MUSIC_TEST_WEBVIEW_PROBE` to that binary
for an additional real WebView2 PCM/decryption/cleanup check. It uses the same
local-origin configuration as the app, muted generated tones and private stdin
credentials. This validates the receiver window, not the complete activity
installation/consent workflow in the desktop GUI.
See [architecture and test details](docs/ARCHITECTURE.md).
