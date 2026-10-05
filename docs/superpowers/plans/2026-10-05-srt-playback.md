# SRT listener playback implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan inline, task-by-task.

**Goal:** Serve authorized SRT callers from shared media workers over an optional global listener.

**Architecture:** A private dynamically loaded libsrt adapter owns nonblocking sockets. An async viewer task parses the Stream ID, acquires the existing policy grant and fenced worker, then sends bounded shared TS messages. Startup and cancellation own all sockets and viewer tasks.

**Tech Stack:** Rust, Tokio, independent system libsrt 1.5, React, Playwright, independent FFmpeg clients.

**Spec:** `docs/srt-playback.md`

## Global Constraints

- Linux IPv4/IPv6; public libsrt 1.5 ABI; no official Flussonic components or production changes.
- Opt-in startup listener; latency 1–10000 ms, default120; viewer slots1–4096, default128.
- Optional 10–79 printable ASCII byte passphrase; enforced AES-128; secrets never enter diagnostics.
- At most1316 bytes/message, nonblocking send, bounded buffers; send stalls2s, media stalls10s.
- Stream IDs at most512 UTF-8 bytes; r/m/u/s/a only; publishing rejected; authorize before workers/media.
- Isolated inline work, one fresh final review; full Rust/browser and exact candidate CI before main.

## Review Focus

- An accepted handshake with denied or stalled authorization must emit no media or start a worker.
- Unsafe ABI calls must preserve library/socket lifetime, address alignment and exact pointer lengths.
- A lagging receiver or disconnect must release its slot independently without interrupting another viewer.
- Disable/replacement or revocation during a pending send must fence every subsequent media write.
- Client-supplied session IDs, secret-bearing library logs and token placeholder URLs must not leak credentials.

### Task 1: Independent listener adapter and Stream ID parsing

**Files:** Create `src/srt_playback.rs` with module exports, `src/srt_playback/native.rs`, `src/srt_playback/selection.rs`; modify `src/lib.rs`; add direct `libloading` and `libc` dependencies and lockfile; create `tests/srt_listener_native.rs` and unit tests.

**Interfaces:** `Settings::new(u32,usize,String) -> io::Result<Settings>`; private native `Listener::bind`, `address`, `accept -> Result<Option<(Socket,SocketAddr)>>`; socket `stream_id`, `try_send`, `is_connected`; owned drop closes each socket. Parser `Selection::parse(&str) -> Result<Selection,&'static str>` returns name/token only.

- [x] Write parser/settings/address tests for duplicates, unknown fields, publish mode, controls, 512-byte boundary, Unicode, invalid passphrases and latency/slot limits; run expecting missing modules.
- [x] Implement strict selection/settings and isolated C ABI adapter with documented unsafe call invariants, runtime version guard, nonblocking sockets and disabled library logs.
- [x] Add independent FFmpeg handshake/receive primitive qualification with ephemeral ports and exact IDs; run expecting PASS; commit adapter.

### Task 2: Authorized shared delivery, startup and friendly runtime UI

**Files:** Extend `src/srt_playback.rs`; create `src/server/ts_access.rs`, `tests/srt_playback.rs`; modify `src/lib.rs`, `src/main.rs`, `src/server.rs`, `src/cluster.rs`/uplink metrics as needed, `web/src/main.tsx`, browser tests, README/spec.

**Interfaces:** `serve(Listener,Arc<App>,CancellationToken)` owns finite viewer tasks; `App::ts_admit`/`ts_current` return fenced worker/grant without RTP constraints. App exposes sanitized listener state and SRT egress. CLI uses optional `--srt-play-listen`, latency/limit and hidden environment passphrase.

- [ ] Write owned receiver tests for six codec combinations, encryption denial, two viewers/one worker, denied/stalled callbacks, session revoke, worker replacement, slot recovery, shutdown and authenticated CDN pull; run expecting missing playback.
- [ ] Implement grant/worker fencing and direct bounded TS fan-out, byte accounting and process aggregate metrics; wire startup validation/bind and fatal listener/shutdown handling.
- [ ] Extend saved Superdesign Config/output direction and browser cases for readable listener status and placeholder URL; implement normal UI, run targeted checks expecting PASS.
- [ ] Run formatting, warnings-denied clippy, full Rust then full browser checks; obtain one fresh review, resolve material findings with RED/GREEN, require exact-head CI.
- [ ] Fast-forward and publish main, refresh the owned preview with private config hash preserved, archive evidence and clean the merged worktree.
