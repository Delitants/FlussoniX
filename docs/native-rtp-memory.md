# RTP delivery memory

Each stream keeps a keyframe bootstrap so a new RTSP/RTP viewer can start
decoding immediately. Its separate live delivery queue is now retained only
while at least one RTP receiver exists. Streams watched through HTTP or native
M4F/M4S no longer fill an unused RTP delivery queue. Closing the last RTP
receiver releases that queue; media already held by a response remains valid.

When no RTP receiver exists, packetization also skips the separate contiguous
live GOP copy. Codec validation, packet sequence numbers, and the keyframe
bootstrap still advance, so a later viewer can join normally. The 64 MiB
producer limit remains enforced even when this copy is omitted. This reduces
allocation work; sustained RSS must be measured separately.

Active receivers retain the existing shared 4096-record / 64 MiB bounds and
lag detection. Clean EOF still lets existing receivers drain their queued
packets. The bootstrap, codec validation, RTP packetization, authorization,
and other native queues are unchanged. Freed allocator memory may remain
resident until reused or reclaimed by [idle memory cleanup](idle-memory-cleanup.md).

The issue appeared during an eight-stream native cluster load test: M4F
publishes complete GOP batches, which filled unused RTP queues over several
minutes. Per-frame M4S publication reached its record limit earlier and used
less memory. This was bounded retention, rather than evidence of an unlimited
leak. This change does not establish a production capacity or memory target.

Regression coverage exercises unused output retention, last-receiver release,
continued delivery to another receiver, late bootstrap, clean EOF, and actual
payload ownership after a receiver is dropped. Existing RTP tests cover large
GOP bursts, fragmentation, and slow-reader lag.
