# Continuous MPEG-TS buffering

FlussoniX shares one unread MPEG-TS backlog per stream across HTTP/HTTPS playback, SRT playback, HTTP/SRT/RTSP publishing, shared TS packaging and MPEG-TS RTP/SRTP output. The backlog holds up to 4 MiB of payload and 32,768 chunks. FFmpeg read sizes vary, so a GOP burst can span many chunks without losing data merely because it crosses 64 reads.

Viewers receive the original bytes in order. Consumed chunks are released as soon as every interested viewer advances or leaves; a stream without TS viewers retains no TS backlog. A new subscription starts at the current live position. Reading a chunk retains its immutable payload independently of the queue.

The byte budget is shared, not multiplied by viewer count. Queue metadata and transport socket buffers also consume memory. A subscriber that exceeds the backlog gets an internal lag error and its current delivery ends; FlussoniX never silently skips missing TS. Other viewers continue independently. Existing authorization revocation, worker cancellation and publishing retry policies remain in effect.

This buffer absorbs short delivery bursts. It does not shape network bandwidth or provide DVR replay, and very high bitrate streams can exhaust the finite budget sooner.

Native HTTP MPEG-TS deliveries emit one structured `native_ts_delivery_end` event on the `flussonix::native_ts` logging target at INFO level. To capture these events while keeping other logs at WARN, set `RUST_LOG=warn,flussonix::native_ts=info` before starting FlussoniX. The reasons distinguish `queue_lagged`, `authorization_revoked`, `worker_stopped`, `queue_closed`, and `body_dropped`. A body drop without a selected terminal branch does not prove a client disconnect or a network error.

Events include a process-local delivery ID, worker PID, elapsed time, missed-record count, byte counters, retained queue size, and cancellation flags. They omit stream names, viewer addresses, URLs, tokens and credentials. `bytes_yielded` counts bytes passed to the HTTP body, not bytes acknowledged by the client. Worker-byte and queue snapshots describe the worker at observation time; they are not an exact socket-backlog measurement. No per-packet logs are emitted. Media bytes, HTTP status/headers and cancellation policy are unchanged.
