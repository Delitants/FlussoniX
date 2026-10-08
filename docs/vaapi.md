# Internal VAAPI encoding

Select VAAPI H.264 or HEVC in stream/template Processing. The independent FFmpeg worker decodes in software, uploads NV12 frames and encodes once for its shared outputs. A selected hardware profile must initialize successfully before a running worker is replaced; there is no CPU fallback.

`transcoder.encoder`: `h264_vaapi` or `hevc_vaapi`. `vaapi_device` defaults to `/dev/dri/renderD128`; `low_power` defaults false. The daemon needs device permissions and an independent working libVA driver. No Flussonic components are used.

`vaapi_rc`: `cqp` (default) or `cbr`. CQP uses `qp` 0–51, default 24, and forbids `vb`. Lower QP improves quality and increases bitrate. CBR uses `vb` 100–50000 kb/s, default 900, and forbids `qp`. Hardware mode support varies. Readiness checks use the actual selected device, low-power setting and rate-control parameters, expire only through bounded cache eviction or daemon restart, and time out after five seconds. Capability reports check the default profiles, independently of saved stream overrides.

Video and audio choices inherit separately from templates. Changing video family clears inherited hardware options; changing VAAPI mode clears its inherited conflicting rate parameter. Explicit incompatible settings are rejected. Audio supports independent AAC, MPEG Layer II, MP3 and copy.

This first implementation uses 8-bit software decode and hardware upload; hardware decode and 10-bit profiles are pending. GPU HLS caption conversion remains unqualified and rejected; separate track/drop controls retain their existing behavior. Initialization alone does not prove delivered media or sustained throughput. The opt-in `vaapi_media` test qualifies actual worker H.264 output through independent decoding, including native M4S/M4F, TS HLS and fMP4 HLS, with AAC/MP2/MP3 audio. Run under a working independently supplied libVA environment:

```
cargo test --locked --test vaapi_media -- --ignored --test-threads=1
```

The same Intel H.264 CQP profile now has independent HTTP/HTTPS publishing and
GPU→CPU→GPU replacement qualification with AAC/MP2/MP3. See the [named hardware,
test command and limits](http-ts-push.md#intel-gpu-qualification). Hardware tests
remain opt-in; this does not install a driver or qualify HEVC on this host.

The [compressed HTTPS upstream publishing profile](http-ts-push.md#compressed-upstream-decoding)
qualifies software decoding of independently encoded H.264/MP3 and HEVC/Layer II
sources before Intel H.264 hardware encoding and AAC/Layer II/MP3 HTTP/HTTPS
publishing. HEVC input decoding does not imply HEVC hardware encoding support.

## Dependency and encoder readiness in the admin UI

Config shows GPU transcoding readiness for NVIDIA and VAAPI H.264/HEVC. Selecting
GPU video encoding in stream or template Processing shows that encoder's default
profile result. Checks run through the configured independent FFmpeg as the
FlussoniX service account, using a synthetic frame rather than a stream source.
A successful initialization means the encoder can run with the tested profile;
it does not measure capacity or promise that every custom profile will work.
Custom devices and settings retain their own checks before worker replacement.

The authenticated `/flussonix/api/v1/capabilities` response retains `status` and
adds an optional `diagnostic` code to each GPU profile when a known dependency or
hardware failure is recognized. The UI explains missing/not executable FFmpeg,
missing encoder support, runtime libraries, inaccessible render devices, VAAPI
driver initialization, NVIDIA driver/API failures and unsupported encoder profiles.
Unknown failures remain generic. Raw FFmpeg messages, paths from those messages
and environment contents are never returned. Probes drain stderr with at most
16 KiB retained and retain the five-second deadline and child reaping.

Results are cached for the daemon lifetime (VAAPI also has bounded profile-cache
eviction). Restart FlussoniX after installing drivers or changing permissions.
Reloading the browser does not invalidate server checks. FlussoniX does not install
packages or silently switch a failed GPU selection to CPU.

For an Ubuntu host using an Intel GPU, independently install its matching userspace
stack, for example:

```sh
sudo apt-get install --no-install-recommends intel-media-va-driver libigdgmm12 vainfo
vainfo --display drm --device /dev/dri/renderD128
```

Package names and driver coverage vary by distribution and hardware. The service
account needs access to the render device; containers need the device and matching
userspace libraries exposed. Intel iHD requires GMM. AMD uses a different VAAPI
driver; NVIDIA requires its own compatible driver. No official Flussonic component
is required. `vainfo` is an optional administrator tool, not a FlussoniX dependency.

On the development host, the system Ubuntu Intel media driver 25.3.0+dfsg1-1 and
GMM 22.8.1+ds1-1 initialize the default H.264 CQP24 profile on UHD Graphics 600.
The default HEVC encoder profile fails with no usable encoding entrypoint. This
is a result for that hardware/profile, not a statement about HEVC input decoding
or all possible driver modes.
