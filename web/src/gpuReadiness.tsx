import React from 'react';
type Profile={encoder:string;codec:string;status:string;diagnostic?:string};
type Transcoding={gpu_profiles?:Profile[];vaapi_profiles?:Profile[]};
const guidance:Record<string,string>={
 ffmpeg_missing:'FFmpeg was not found. Install independent FFmpeg and check the configured executable.',
 ffmpeg_not_executable:'FlussoniX cannot execute FFmpeg. Check the executable permissions for the service account.',
 runtime_library_missing:'A required runtime library could not be loaded. Install the matching driver dependencies; Intel iHD requires GMM.',
 encoder_missing:'This FFmpeg build does not include the selected encoder. Install a build with VAAPI or NVENC support.',
 nvidia_driver_unavailable:'The NVIDIA driver could not be loaded or is incompatible. Install a compatible NVIDIA driver and expose the GPU to the service.',
 device_permission_denied:'The GPU check was denied access. Check render-device permissions for the FlussoniX service account.',
 device_unavailable:'The render device could not be opened. Check the device path and expose it to the service or container.',
 driver_initialization_failed:'The VAAPI driver could not initialize. Check the driver installation and its dependencies. Intel iHD uses the Intel media driver and GMM.',
 encoder_unsupported:'The GPU or driver does not support this encoder profile. Choose another codec or supported hardware settings.'
};
export function GPUReadiness({transcoding,error,encoder}:{transcoding?:Transcoding;error?:string;encoder?:string}){
 const profiles=[...(transcoding?.gpu_profiles||[]),...(transcoding?.vaapi_profiles||[])].filter(p=>!encoder||p.encoder===encoder);
 return <section role="region" aria-label="GPU transcoding readiness" className="gpu-readiness">
  <h2>GPU transcoding readiness</h2>
  {error?<p role="status">GPU readiness could not be loaded. Reload the page to try again.</p>:!profiles.length?<p role="status">Checking GPU encoders and dependencies…</p>:<ul>{profiles.map(p=><li key={p.encoder} data-testid={'gpu-'+p.encoder}>
   <strong>{p.encoder.endsWith('_vaapi')?'VAAPI':'NVIDIA'} · {p.codec}</strong> — {p.status==='available'?'Ready for GPU encoding':'GPU encoding unavailable'}
   {p.status!=='available'&&<p>{guidance[p.diagnostic||'']||(p.status==='timed_out'?'The encoder check timed out. Check the GPU and driver health.':'The encoder could not initialize. Check FFmpeg, the GPU driver and service permissions.')}</p>}
  </li>)}</ul>}
  <p className="muted">Default profile checks run as the FlussoniX service account. VAAPI uses /dev/dri/renderD128, constant quality 24 and low power off. Custom VAAPI devices and settings are checked at stream startup. NVIDIA checks use default encoder settings. Initialization does not measure streaming capacity.</p>
  <p className="muted">Restart FlussoniX after installing drivers or changing device permissions to refresh cached checks. GPU failures never switch silently to CPU.</p>
 </section>;
}
