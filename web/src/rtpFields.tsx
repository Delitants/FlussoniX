import React from 'react';
import {Field,type Item} from './forms';
export const directRTP=(url:string)=>/^(srtp|rtp):\/\//.test(url);
const owns=(v:Item,k:string)=>Object.prototype.hasOwnProperty.call(v,k);
function option(row:Item,key:string,value:any):Item {const options={...row.flussonix_rtp};if(value===''||value===undefined)delete options[key];else options[key]=value;const next={...row};if(Object.keys(options).length)next.flussonix_rtp=options;else delete next.flussonix_rtp;return next}
export function RTPInputFields({row,onChange,suffix=''}:{row:Item,onChange:(v:Item)=>void,suffix?:string}) {
 if(!directRTP(row.url||''))return null;
 const opts=row.flussonix_rtp||{};
 return <div className="form-grid rtp-input-options">
  <Field label={'RTP input profile'+suffix} help="MPEG-TS carries broadcast subtitle tracks. Elementary RTP uses a validated static SDP file; each media track has its own RTP/RTCP pair."><select value={opts.profile||'mp2t'} onChange={e=>{let next=option(row,'profile',e.target.value==='mp2t'?undefined:e.target.value);if(e.target.value==='mp2t')next=option(next,'sdp_file',undefined);onChange(next)}}><option value="mp2t">MPEG-TS / PT33</option><option value="elementary">Elementary audio/video · SDP</option></select></Field>
  {opts.profile==='elementary'&&<Field label={'RTP input SDP file'+suffix} help="Absolute path on this server to a regular, server-owned SDP file (up to 16 KiB), without group/world write permission. Its first media IP and port must match the input URL. H.264, HEVC, AAC-LC, MP2 and MP3; Use RTP/SAVP for encrypted input or RTP/AVP for plaintext. No inline keys or external references."><input value={opts.sdp_file||''} placeholder="/etc/flussonix/sdp/channel.sdp" onChange={e=>onChange(option(row,'sdp_file',e.target.value))}/></Field>}
  <Field label={'RTP interface IP'+suffix} help="Local IPv4 interface. Required for multicast; with a wildcard bind, restricts reception to this address. Otherwise it must match the input URL."><input value={opts.interface||''} placeholder="192.168.1.10" onChange={e=>onChange(option(row,'interface',e.target.value))}/></Field>
  <Field label={'RTP source IP'+suffix} help="Optional exact sender IP filter. The first valid sender and SSRC are pinned until restart."><input value={opts.source_ip||''} onChange={e=>onChange(option(row,'source_ip',e.target.value))}/></Field>
  <Field label={'RTP jitter (milliseconds)'+suffix} help="Reorder packets for up to 0–1000 ms; default 50. Queue is bounded to 64 packets."><input type="number" min="0" max="1000" placeholder="50" value={opts.jitter_ms??''} onChange={e=>onChange(option(row,'jitter_ms',e.target.value===''?'':Number(e.target.value)))}/></Field>
  {row.url?.startsWith('srtp://')&&<KeyField label={'SRTP key file'+suffix} value={opts.key_file||''} onChange={v=>onChange(option(row,'key_file',v))}/>}
 </div>;
}
function KeyField({label,value,onChange}:{label:string,value:string,onChange:(v:string)=>void}) {return <Field label={label} help="Absolute path on this node to an owner-only file containing a base64 30-byte key and salt. AES_CM_128_HMAC_SHA1_80; must match the peer. Changes take effect on restart. Secure receivers need a fresh sender generation; automatic rollover-state transfer for late joins is not supported."><input value={value} placeholder="/etc/flussonix/keys/channel.key" autoComplete="off" onChange={e=>onChange(e.target.value)}/></Field>}
export function rtpSummary(rows:Item[]):React.ReactNode {return rows?.length?rows.map((p,i)=><div key={i}>{i+1}. {p.url} · {p.disabled?'Disabled':'Enabled'} · {p.url?.startsWith('srtp://')?'Encrypted':'Plaintext'} · {p.flussonix_rtp?.profile==='elementary'?'Elementary audio/video · SDP':'MPEG-TS / PT33'}</div>):'No destinations'}
export function DirectOutputFields({value,inherited,onChange}:{value:Item,inherited?:Item[],onChange:(v:Item[]|undefined)=>void}) {
 const mode=owns(value,'flussonix_rtp_outputs')?(value.flussonix_rtp_outputs?.length?'override':'none'):'inherit';const rows:Item[]=value.flussonix_rtp_outputs||[];
 const edit=(i:number,row:Item)=>onChange(rows.map((r,n)=>n===i?row:r));
 return <fieldset><legend>RTP and SRTP destinations</legend><p className="muted">Send processed MPEG-TS (PT33) or elementary audio/video. Each track uses a consecutive RTP/RTCP pair. Enabled destinations keep the stream active. Elementary output reserves up to 16 ports and provides an SDP download on the Output tab. Separate subtitles require MPEG-TS. Secure elementary destinations use key-free SAVP descriptors and a matching receiver-local key.</p>
  <Field label="Direct transport destinations"><select value={mode} onChange={e=>onChange(e.target.value==='inherit'?undefined:e.target.value==='none'?[]:inherited?.length?structuredClone(inherited):[{url:''}])}><option value="inherit">{value.template?'Use template destinations':'No destinations (default)'}</option><option value="override">Override destinations</option><option value="none">No destinations</option></select></Field>
  {mode==='inherit'&&value.template&&<div className="muted">{rtpSummary(inherited||[])}</div>}
  {mode==='override'&&<>{rows.map((row,i)=><div className="form-details" key={i}><h3>RTP destination {i+1}</h3><div className="form-grid">
   <Field label={`RTP destination URL ${i+1}`} help="Literal receiver IP and port (1024–65534). No query parameters or credentials."><input value={row.url||''} placeholder="rtp://192.168.1.20:5004" onChange={e=>{const next={...row,url:e.target.value};edit(i,e.target.value.startsWith('srtp://')?next:option(next,'key_file',undefined));}}/></Field>
   <Field label={`RTP destination profile ${i+1}`}><select value={row.flussonix_rtp?.profile||'mp2t'} onChange={e=>edit(i,option(row,'profile',e.target.value==='mp2t'?undefined:e.target.value))}><option value="mp2t">MPEG-TS / PT33</option><option value="elementary">Elementary audio/video · SDP</option></select></Field>
   <Field label={`RTP interface IP ${i+1}`} help="Optional IPv4 source interface. Required for multicast."><input value={row.flussonix_rtp?.interface||''} onChange={e=>edit(i,option(row,'interface',e.target.value))}/></Field>
   <Field label={`RTP multicast TTL ${i+1}`}><input type="number" min="1" max="255" placeholder="16" value={row.flussonix_rtp?.ttl??''} onChange={e=>edit(i,option(row,'ttl',e.target.value===''?'':Number(e.target.value)))}/></Field>
   <Field label={`RTP maximum bitrate (Mbps) ${i+1}`} help="Pacing limit, 1–10000 Mbps; default 100."><input type="number" min="1" max="10000" placeholder="100" value={row.max_mbps??''} onChange={e=>{const next={...row};if(e.target.value==='')delete next.max_mbps;else next.max_mbps=Number(e.target.value);edit(i,next)}}/></Field>
   {row.url?.startsWith('srtp://')&&<KeyField label={`SRTP key file ${i+1}`} value={row.flussonix_rtp?.key_file||''} onChange={v=>edit(i,option(row,'key_file',v))}/>}
   <Field label={`RTP destination enabled ${i+1}`}><input type="checkbox" checked={!row.disabled} onChange={e=>edit(i,{...row,disabled:!e.target.checked})}/></Field>
  </div><button type="button" onClick={()=>onChange(rows.filter((_,n)=>n!==i))}>Remove RTP destination {i+1}</button></div>)}<button type="button" disabled={rows.length>=4} onClick={()=>onChange([...rows,{url:''}])}>Add RTP destination</button></>}
 </fieldset>;
}
export function rtpError(value:Item):string|undefined {
 for(const row of [...(value.inputs||[]),...(value.flussonix_rtp_outputs||[])]) {
  if(!directRTP(row.url||''))continue;
  if(/[\s\x00-\x1f\x7f]/.test(row.url)||row.url.split('://')[1]?.replace(/\/$/,'').includes('/'))return 'RTP URL cannot contain whitespace or a path.';
  try {const u=new URL(row.url);const host=u.hostname.replace(/^\[|\]$/g,'');const ipv4=/^(\d{1,3}\.){3}\d{1,3}$/.test(host)&&host.split('.').every(n=>Number(n)<=255);if(!ipv4&&!host.includes(':')||!u.port||Number(u.port)<1024||Number(u.port)>65534||u.search||u.hash||u.username||u.password||!['','/'].includes(u.pathname))return 'RTP URL needs a literal IP and port 1024–65534, without path, query or credentials.';}catch{return 'Enter a complete RTP or SRTP IP address and port.'}
  const o=row.flussonix_rtp||{};for(const [k,min,max] of [['jitter_ms',0,1000],['ttl',1,255]] as const)if(owns(o,k)&&(!Number.isInteger(o[k])||o[k]<min||o[k]>max))return `RTP ${k==='ttl'?'TTL':'jitter'} must be a whole number from ${min} to ${max}.`;
  if(o.profile==='elementary'){if(Number(new URL(row.url).port)>65520)return 'Elementary RTP needs a base port from 1024 to 65520.';if(value.inputs?.includes(row)&&!o.sdp_file?.startsWith('/'))return 'Elementary RTP input needs an absolute SDP file path on this node.';}
  if(row.url.startsWith('srtp://')&&!o.key_file?.startsWith('/'))return 'SRTP requires an absolute key file path on this node.';
  if(owns(row,'max_mbps')&&(!Number.isInteger(row.max_mbps)||row.max_mbps<1||row.max_mbps>10000))return 'RTP maximum bitrate must be 1–10000 Mbps.';
 }
}
