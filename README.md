# Discordia Music — Music Lab

An optional, installable music activity with a separate Rust service for
[Discordia](https://github.com/ssotomayor/discordia). Nothing runs or installs
automatically in a Discordia community.

**Experimental:** shared metadata queue and opt-in local audio playback work.
Provider audio and playlist expansion are not implemented. Android/iOS activity
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

## Current capabilities

| Area | Status |
|---|---|
| Queue | Shared, bounded; player additions and host administration |
| YouTube, Spotify, SoundCloud | Public link validation and track metadata lookup |
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
uses that existing test SFU without stopping it. Two native SDK receivers are
validated; full desktop GUI/WebView playback still needs manual verification.
See [architecture and test details](docs/ARCHITECTURE.md).
