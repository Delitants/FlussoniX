# Continuous MPEG-TS buffering

FlussoniX shares one unread MPEG-TS backlog per stream across HTTP/HTTPS playback, SRT playback, HTTP/SRT/RTSP publishing, shared TS packaging and MPEG-TS RTP/SRTP output. The backlog holds up to 4 MiB of payload and 32,768 chunks. FFmpeg read sizes vary, so a GOP burst can span many chunks without losing data merely because it crosses 64 reads.

Viewers receive the original bytes in order. Consumed chunks are released as soon as every interested viewer advances or leaves; a stream without TS viewers retains no TS backlog. A new subscription starts at the current live position. Reading a chunk retains its immutable payload independently of the queue.

The byte budget is shared, not multiplied by viewer count. Queue metadata and transport socket buffers also consume memory. Viewers that exceed the backlog receive an explicit lag error and their current delivery ends; FlussoniX never silently skips missing TS. Other viewers continue independently. Existing authorization revocation, worker cancellation and publishing retry policies remain in effect.

This buffer absorbs short delivery bursts. It does not shape network bandwidth or provide DVR replay, and very high bitrate streams can exhaust the finite budget sooner.
