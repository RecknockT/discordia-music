# Discordia Music — Music Lab

An optional, installable music activity with a separate Rust service for
[Discordia](https://github.com/ssotomayor/discordia). Nothing runs or installs
automatically in a Discordia community.

**Experimental: shared queue and public metadata work; end-to-end music
playback is not connected yet.** Native decoding, encrypted audio transport
and scoped key envelopes are separate tested building blocks. Android/iOS
activity windows are not supported yet.

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

## Current capabilities

| Area | Status |
|---|---|
| Queue | Shared, bounded, with player/host permissions and command deduplication |
| YouTube, Spotify, SoundCloud | Public link validation and track metadata lookup |
| TIDAL | Link validation; no metadata adapter |
| Playlist expansion / provider audio | Not implemented; recognizing a link does not grant audio or retransmission rights |
| Local audio engine | Optional WAV/PCM, MP3 and Ogg/Vorbis decoding, stereo resampling and playback state |
| Native transport | Optional isolated encrypted LiveKit publication; not invoked by activity commands yet |
| Key delivery | Optional scoped NIP-44 envelopes and publication-key rotation; not wired to the activity yet |

The signed manifest requests `Instance` only. Audio permission and the trusted
receiver are being developed in Discordia; installing this prototype does not
enable streaming or grant microphone access. No music account integration exists.

## Development

Discordia's SDK and native WebRTC patches are pinned to a Git revision, rather
than sibling folders or an unpinned branch. The server dependency is used by
integration tests. Native `transport` builds require the same C++ toolchain as
Discordia. This service and its bundle live here; Discordia retains installation,
permissions, the gateway and trusted client audio reception.

On Windows, keep the checkout/build path short for the native `transport`
feature: legacy C++ tools can reject extracted SDK headers at 260 characters.

```powershell
$env:LIVEKIT_BUNDLE_SKIP='1'  # Tests do not launch the bundled SFU.
cargo fmt --all --check
cargo test --locked --features activity,audio,audio-keys
cargo clippy --locked --features activity,audio,audio-keys --all-targets -- -D warnings
node --test tests/activity_ui.cjs
```

Do not use `LIVEKIT_BUNDLE_SKIP` for a Discordia host that needs its bundled
voice server. The ignored native media test requires an explicitly configured
loopback test SFU and generated stereo tones; it does not use microphones or
speakers. See [architecture and test details](docs/ARCHITECTURE.md).
