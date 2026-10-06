# Regional subtitles from a source through a CDN to SRT

An encrypted shared SRT playback listener on a native CDN can serve a discovered
source stream with its original broadcast subtitles. Select **MPEG-TS · broadcast
subtitles** for the CDN's source relationship. Configure subtitle controls on
the source; discovery carries those controls to the CDN. The private pull uses
the peer credential, while the public SRT caller supplies its viewer token.
The [cluster MPEG-TS path](cluster-subtitles.md) and [SRT listener](srt-playback.md)
retain their existing configuration and API.

`tests/cluster_srt_subtitles.rs` qualifies twelve local copy-mode combinations:
H.264 and HEVC Main 8-bit without B-frames, AAC audio, CEA-608 and CEA-708, and
three source policies. Every fixture also carries independently authored DVB
clear-page and teletext page888 tracks.

| Source HLS subtitles | Source original tracks | CDN SRT result |
| --- | --- | --- |
| Convert to selectable WebVTT | Preserve | Original CEA commands and separate European tracks survive; the CDN independently produces selected CEA WebVTT |
| Filter out | Preserve | All originals survive; HLS filtering remains output-specific |
| Filter out | Drop | DVB/teletext descriptors and payloads are absent; embedded CEA video captions survive |

Two local App/HTTP servers have separate source/CDN configurations and media
directories, with real TCP private pulls, native encrypted SRT sockets and
independent FFmpeg processes. The CDN has no local stream or input entry;
the source must supply discovery and viewer policy. A paced source publication
begins feeding after its private subscriber attaches. The independent SRT
receiver captures untouched bytes for twelve seconds and exits normally.

Source and receiver codecs are independently verified with FFprobe. Distinct
authored GA94 bodies, regional packet types, exact repeated European PES and
language/page descriptors must survive. Filtered cases scan reassembled PES
on every captured PID, including PIDs absent from the PMT. Strict AV decoding
requires success and empty error-level stderr, using a separate terminal-complete
copy; raw transport assertions inspect the original capture. These shared
oracles and their incomplete-tail/interior-loss and fragmented-orphan negative
tests also remain in the [direct SRT qualification](srt-subtitles.md).

An independent HTTP observer requires one private MPEG-TS subscription with
the peer header and no viewer query. Source and CDN each retain one shared
worker; the source subscription creates no public viewer grant. A wrong-token
SRT caller must reach real source discovery, close without media, create no
worker or authorization session on either node, and make no private media
request. Shutdown joins the SRT listener, releases attached viewers and the
source subscription, stops workers and joins the HTTP servers. The existing
30-second authorization cache is separate from attached viewer counts.

This is a local native-cluster listener profile. It does not qualify caller
push from a CDN, SRT input-to-output, CPU/GPU caption retention, HEVC B-frames
or Main10, regional MPEG audio, private HTTPS in this subtitle combination,
mixed Flussonic clusters, failover/reconnect, WAN loss, sustained throughput or
scale. SRT listener playback has no HTTP LB redirect step; LB admission and
selection have their separate existing tests. Distinct command carriage does
not prove rendering or exact timing/order/repetition counts in SRT players.
European HLS conversion, advanced DVB/teletext/OCR and broader authorization
properties retain their separate qualification limits.

No runtime, API, UI, dependency or workflow change was required for this profile.
All listeners, secrets and media are test-owned; no vendor component or
production/demo/CDN-host configuration is involved.
