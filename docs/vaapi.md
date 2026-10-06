# Internal VAAPI encoding

Select VAAPI H.264 or HEVC in stream/template Processing. The independent FFmpeg worker decodes in software, uploads NV12 frames and encodes once for its shared outputs. A selected hardware profile must initialize successfully before a running worker is replaced; there is no CPU fallback.

`transcoder.encoder`: `h264_vaapi` or `hevc_vaapi`. `vaapi_device` defaults to `/dev/dri/renderD128`; `low_power` defaults false. The daemon needs device permissions and an independent working libVA driver. No Flussonic components are used.

`vaapi_rc`: `cqp` (default) or `cbr`. CQP uses `qp` 0–51, default 24, and forbids `vb`. Lower QP improves quality and increases bitrate. CBR uses `vb` 100–50000 kb/s, default 900, and forbids `qp`. Hardware mode support varies. Readiness checks use the actual selected device, low-power setting and rate-control parameters, expire only through bounded cache eviction or daemon restart, and time out after five seconds. Capability reports check the default profiles, independently of saved stream overrides.

Video and audio choices inherit separately from templates. Changing video family clears inherited hardware options; changing VAAPI mode clears its inherited conflicting rate parameter. Explicit incompatible settings are rejected. Audio supports independent AAC, MPEG Layer II, MP3 and copy.

This first implementation uses 8-bit software decode and hardware upload; hardware decode and 10-bit profiles are pending. GPU HLS caption conversion remains unqualified and rejected; separate track/drop controls retain their existing behavior. Initialization alone does not prove delivered media or sustained throughput. The opt-in `vaapi_media` test qualifies actual worker H.264 output through independent decoding, including native M4S/M4F, TS HLS and fMP4 HLS, with AAC/MP2/MP3 audio. Run under a working independently supplied libVA environment:

```
cargo test --locked --test vaapi_media -- --ignored --test-threads=1
```
