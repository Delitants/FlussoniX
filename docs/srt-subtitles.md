# Regional subtitles over encrypted SRT

The shared playback listener and caller destinations carry the worker's MPEG-TS
subtitle policy. Dedicated copy-mode qualification uses independently authored
CEA-608 and CEA-708 commands alongside European DVB and teletext tracks, through
real encrypted localhost SRT connections and independent FFmpeg receivers.
Both H.264 and HEVC Main 8-bit fixtures use AAC audio and copy-mode workers;
the HEVC fixture uses independently encoded video and authored prefix-SEI
caption commands, with no B-frames.
No Flussonic component or reference-server media is used.

`tests/srt_subtitles.rs` covers twenty-four combinations: two video codecs, two SRT
output directions, two North American caption families, and three policy
configurations. Every configuration contains the European tracks as well:

| HLS subtitles | Original subtitle tracks | SRT result |
| --- | --- | --- |
| Convert to selectable WebVTT | Preserve | Original embedded captions and separate DVB/teletext remain; selected CEA captions also appear as HLS WebVTT |
| Filter out | Preserve | Original captions and DVB/teletext remain on SRT; HLS policy does not strip other outputs |
| Filter out | Drop | Separate DVB/teletext descriptors and payloads disappear from SRT; embedded video captions remain |

**Original subtitle tracks** controls separate tracks. It does not globally
remove embedded video captions. **HLS subtitles** controls HLS delivery only.
This stage adds transport evidence for the existing controls, without new API
fields or UI settings.

The independent receiver records raw SRT payload bytes through FFmpeg's data
input/output path, without MPEG-TS demuxing or rebuilding program tables. It
quits normally after a twelve-second live sample and must finish successfully.
Independent FFprobe verifies the actual source and receiver video codec and AAC
audio, so an AVC fallback cannot qualify an HEVC case. Independent video
extraction from that raw capture compares every distinct authored GA94 caption
command with the source, and verifies the
source contains the stated 608 or 708 packet types. Separate-track assertions
check language/page descriptors, repeated exact encoded PES bodies, and absence
of orphaned payloads in the filtered cases, reassembling PES across transport
packets even for PIDs absent from the PMT. A deliberately fragmented, unannounced
PES fixture must fail this filtering assertion. Independent FFmpeg strictly
decodes H.264 or HEVC video and AAC audio from a separate copy containing complete
PES. A wall-clock sample may stop inside the last PES on a PID; only terminal PES not
proven complete and a trailing partial transport packet are omitted from that decode
copy. Raw caption, descriptor and orphan assertions inspect the untouched capture.
Strict decoding requires successful process exit and an empty error log; some
FFmpeg versions report decoder errors without returning a failing exit code.
A deterministic negative test verifies exactly which terminal audio packet is
omitted and requires an interior packet loss to remain a strict decoding failure
after this terminal handling. Playback uses a real viewer
token and attaches before feeding early live caption events; all ports, streams,
secrets and media belong to the tests. Shutdown joins the listener and stops
the worker.

This qualifies original carriage, not subtitle rendering in every SRT player.
The European DVB fixture is a clear-page display set; teletext uses the owned
page888 fixture. It does not establish advanced DVB objects, broader teletext
character sets or OCR accuracy. HLS conversion alongside SRT is exercised for
the CEA family; European conversion has its separate HLS qualification.

CPU/GPU caption retention over SRT, HEVC regional-caption B-frame and Main10
profiles, MPEG audio with regional subtitles, SRT input-to-output subtitle round
trips, source-to-CDN-to-SRT carriage, WAN loss and sustained throughput remain
unqualified. Other established video/audio SRT
tests do not imply those subtitle combinations are covered.
