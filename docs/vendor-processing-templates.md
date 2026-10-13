# Vendor output processing

Create a template in **Media → Templates** with the desired codecs and subtitle
settings. In **Cluster → Source servers**, edit a Flussonic source and choose it
under **Processing template**. The editor shows the selected processing settings
without JSON. Leaving this selection empty keeps copy output and default subtitle
filtering.

Processing includes transcoding, media stall timeout, original subtitle
preserve/drop, HLS passthrough/convert/drop and caption selectors. Existing template
controls support CEA-608 channels, CEA-708 services, European teletext/DVB pages and
native text track IDs. DVB conversion requires installed OCR models. Template
inputs, viewer authorization, activation and publishing settings are not inherited
by this processing selection. Configure viewer authorization on the source.

CPU and supported GPU profiles use the existing independent FFmpeg pipeline and
GPU readiness checks. GPU caption conversion is not qualified and is rejected for
selected vendor processing. Original native text cannot be preserved while
transcoding; detected text fails explicitly instead of being silently lost. Use
filtering or supported HLS conversion for that combination.

The API extension is a source field:

```json
{"flussonix_source_kind":"flussonic","flussonix_output_template":"processing"}
```

The referenced template must exist. Invalid edits and deletion of a referenced
template fail atomically; clear the source selection first. Valid source/template
edits close old input/output generations and viewers reconnect through current
authorization. Old HLS/subtitle resources do not authorize access or survive a
replacement generation. Equivalent source lease renewal retains the current
worker's conversion state.

Each stream has one shared processing generation across HTTP/HTTPS HLS, TS, M4S
and M4F viewers. A new generation owns fresh conversion state. Cluster credentials
stay with the independent private source client and never enter FFmpeg arguments.
Source kind changes require explicitly clearing vendor processing selection.

Owned tests verify CPU AAC decoding, native text preservation/filtering,
English/German WebVTT, source/profile isolation, edits, authorization and child
cleanup. Encoder syntax/selector acceptance is not new GPU hardware or broadcast
interoperability qualification. Native LB placement into vendor-backed FlussoniX CDNs is described
[here](vendor-cdn-placement.md), including vendor discovery on the LB. Processing
runs on the CDN; it is never applied on the LB. Vendor failover, static activation
and RTSP/SRT delivery remain pending.
