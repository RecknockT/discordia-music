# Discordia Music

Read `README.md` and `docs/ARCHITECTURE.md` before changing the service.

- Rust external service, explicitly installed as an activity. Never execute
  provider code or audio processing inside the Discordia gateway.
- Production code must not panic on input or block the Tokio socket loop.
- Comments explain why, maximum two lines. Bound queues, downloads and workers.
- Do not commit identities (`music.key`), credentials, audio files or build outputs.
- Keep metadata recognition distinct from verified provider playback; never
  silently replace one provider's track with another.
- Keep native music rooms, grants and E2EE keys separate from voice. Close the
  previous publication before replacing it after a membership change.
- SDK and native patches come from pinned Discordia Git revisions. General
  gateway/client permission changes belong in Discordia, not copies here.
- Run `cargo fmt --all --check`, Clippy with `-D warnings`,
  `cargo test --features activity,audio,audio-keys` and
  `node --test tests/activity_ui.cjs`. Use `LIVEKIT_BUNDLE_SKIP=1` for these
  tests. Native `transport` is optional and has a separate C++ toolchain.
- Native SFU tests use generated tones and an explicitly configured loopback
  server. Stop and verify exit of temporary processes started for testing.
