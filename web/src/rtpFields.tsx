import React from 'react';
import {Field,type Item} from './forms';

export const directRTP=(url:string)=>/^(srtp|rtp):\/\//.test(url);
const owns=(v:Item,k:string)=>Object.prototype.hasOwnProperty.call(v,k);
const secureKeys=['key_file','track_key_files','expected_ssrcs','track_keys','allow_legacy_reused_key'];
function option(row:Item,key:string,value:any):Item {
 const options={...row.flussonix_rtp};
 if(value===''||value===undefined)delete options[key];else options[key]=value;
 const next={...row};
 if(Object.keys(options).length)next.flussonix_rtp=options;else delete next.flussonix_rtp;
 return next;
}
function clear(row:Item,keys:string[]):Item {
 let next=row;
 for(const key of keys)next=option(next,key,undefined);
 return next;
}
export const withoutSRTP=(row:Item)=>clear(row,secureKeys);
function setList(row:Item,key:string,index:number,value:any,minimum:any):Item {
 const entries=[...(row.flussonix_rtp?.[key]||[minimum])];
 entries[index]=value;
 return option(row,key,entries);
}
function path(value:unknown):boolean {return typeof value==='string'&&value.startsWith('/')&&value.length>1}
function nonzero(value:unknown):boolean {return Number.isInteger(value)&&Number(value)>0&&Number(value)<=4294967295}

function KeyField({label,value,onChange}:{label:string,value:string,onChange:(v:string)=>void}) {
 return <Field label={label} help="Absolute path on this node to an owner-only file with a base64 30-byte SRTP key and salt. Never paste key bytes into this form."><input value={value} placeholder="/etc/flussonix/keys/channel.key" autoComplete="off" onChange={e=>onChange(e.target.value)}/></Field>;
}
function SharedInputKeys({row,onChange,suffix}:{row:Item,onChange:(v:Item)=>void,suffix:string}) {
 const opts=row.flussonix_rtp||{};
 const count=Math.max(1,opts.expected_ssrcs?.length||0);
 return <>
  <KeyField label={'SRTP key file'+suffix} value={opts.key_file||''} onChange={v=>onChange(option(row,'key_file',v))}/>
  {opts.profile==='elementary'&&<>
   <p className="muted">For multiple SDP tracks, enter each sender SSRC in SDP media order. Rotate the SSRCs together with the key after a receiver restart.</p>
   {Array.from({length:count},(_,index)=><div className="form-details" key={index}>
    <Field label={`Expected SSRC ${index+1}${suffix}`} help="Nonzero sender ID from this track's a=ssrc SDP line."><input type="number" min="1" max="4294967295" value={opts.expected_ssrcs?.[index]||''} onChange={e=>onChange(setList(row,'expected_ssrcs',index,e.target.value===''?0:Number(e.target.value),0))}/></Field>
    {count>1&&<button type="button" onClick={()=>onChange(option(row,'expected_ssrcs',(opts.expected_ssrcs||[]).filter((_:number,i:number)=>i!==index)))}>Remove expected SSRC {index+1}</button>}
   </div>)}
   <button type="button" disabled={count>=8} onClick={()=>onChange(option(row,'expected_ssrcs',[...(opts.expected_ssrcs||[0]),0]))}>Add expected SSRC</button>
  </>}
 </>;
}
function SeparateInputKeys({row,onChange,suffix}:{row:Item,onChange:(v:Item)=>void,suffix:string}) {
 const opts=row.flussonix_rtp||{};
 const entries:string[]=opts.track_key_files?.length?opts.track_key_files:[''];
 return <>
  <p className="muted">One different key file per SDP media track, in SDP order. Rotate all track keys together after a receiver restart.</p>
  {entries.map((entry,index)=><div className="form-details" key={index}>
   <KeyField label={`Track key file ${index+1}${suffix}`} value={entry} onChange={v=>onChange(setList(row,'track_key_files',index,v,''))}/>
   {entries.length>1&&<button type="button" onClick={()=>onChange(option(row,'track_key_files',entries.filter((_,i)=>i!==index)))}>Remove track key {index+1}</button>}
  </div>)}
  <button type="button" disabled={entries.length>=8} onClick={()=>onChange(option(row,'track_key_files',[...entries,'']))}>Add track key</button>
 </>;
}
export function RTPInputFields({row,onChange,suffix=''}:{row:Item,onChange:(v:Item)=>void,suffix?:string}) {
 const [bufferText,setBufferText]=React.useState<string|undefined>();
 if(!directRTP(row.url||''))return null;
 const opts=row.flussonix_rtp||{};
 const secure=row.url?.startsWith('srtp://');
 const elementary=opts.profile==='elementary';
 const mode=opts.track_key_files?'separate':'shared';
 const changeProfile=(profile:string)=>{
  let next=option(row,'profile',profile==='mp2t'?undefined:profile);
  if(profile==='mp2t')next=clear(next,['sdp_file','track_key_files','expected_ssrcs']);
  onChange(next);
 };
 const changeMode=(value:string)=>{
  let next=clear(row,['key_file','track_key_files','expected_ssrcs']);
  if(value==='separate')next=option(next,'track_key_files',['']);
  onChange(next);
 };
 return <div className="form-grid rtp-input-options">
  <Field label={'RTP input profile'+suffix} help="MPEG-TS carries broadcast subtitles. Elementary RTP uses a static SDP; each track has its own RTP/RTCP pair."><select value={opts.profile||'mp2t'} onChange={e=>changeProfile(e.target.value)}><option value="mp2t">MPEG-TS / PT33</option><option value="elementary">Elementary audio/video · SDP</option></select></Field>
  {elementary&&<Field label={'RTP input SDP file'+suffix} help="Absolute path to a server-owned SDP file. It must match the input IP, port and each encrypted track's source identity."><input value={opts.sdp_file||''} placeholder="/etc/flussonix/sdp/channel.sdp" onChange={e=>onChange(option(row,'sdp_file',e.target.value))}/></Field>}
  <Field label={'RTP interface IP'+suffix} help="Local IPv4 interface, required for multicast."><input value={opts.interface||''} placeholder="192.168.1.10" onChange={e=>onChange(option(row,'interface',e.target.value))}/></Field>
  <Field label={'RTP source IP'+suffix} help="Optional exact sender IP filter."><input value={opts.source_ip||''} onChange={e=>onChange(option(row,'source_ip',e.target.value))}/></Field>
  <Field label={'RTP jitter (milliseconds)'+suffix} help="Reorder packets for up to 0–1000 ms; default 50."><input type="number" min="0" max="1000" placeholder="50" value={opts.jitter_ms??''} onChange={e=>onChange(option(row,'jitter_ms',e.target.value===''?'':Number(e.target.value)))}/></Field>
  <Field label={'RTP receive buffer (MiB)'+suffix} help="Optional public socket buffer per input track, from 0.0625 to 32 MiB."><input type="text" inputMode="decimal" placeholder="System default" value={bufferText??(opts.receive_buffer_bytes===undefined?'':opts.receive_buffer_bytes/1048576)} onChange={e=>{const text=e.target.value;setBufferText(text);onChange(option(row,'receive_buffer_bytes',text===''?'':/^(?:\d+(?:\.\d*)?|\.\d+)$/.test(text)?Math.round(Number(text)*1048576):text))}} onBlur={()=>{if(bufferText===''||typeof opts.receive_buffer_bytes==='number')setBufferText(undefined)}}/></Field>
  {secure&&<>
   {elementary&&<Field label={'SRTP input protection'+suffix} help="Shared key requires distinct expected SSRCs. Separate keys bind each SDP track to its own key file."><select value={mode} onChange={e=>changeMode(e.target.value)}><option value="shared">Shared key + expected SSRCs</option><option value="separate">Different key per track</option></select></Field>}
   {mode==='separate'&&elementary?<SeparateInputKeys row={row} onChange={onChange} suffix={suffix}/>:<SharedInputKeys row={row} onChange={onChange} suffix={suffix}/>}
   <Field label={'Allow reused SRTP receiver key'+suffix} help="Legacy compatibility only. By default a receiver restart requires a fresh key and an initialized owner-only receiver ledger. Reusing a key weakens replay protection."><input type="checkbox" checked={opts.allow_legacy_reused_key===true} onChange={e=>onChange(option(row,'allow_legacy_reused_key',e.target.checked?true:undefined))}/></Field>
  </>}
 </div>;
}
export function rtpSummary(rows:Item[]):React.ReactNode {
 return rows?.length?rows.map((row,i)=><div key={i}>{i+1}. {row.url} · {row.disabled?'Disabled':'Enabled'} · {row.url?.startsWith('srtp://')?'Encrypted':'Plaintext'} · {row.flussonix_rtp?.profile==='elementary'?'Elementary audio/video · SDP':'MPEG-TS / PT33'}</div>):'No destinations';
}
function OutputTrackRows({row,onChange,destination}:{row:Item,onChange:(v:Item)=>void,destination:number}) {
 const entries:Item[]=row.flussonix_rtp?.track_keys?.length?row.flussonix_rtp.track_keys:[{track_id:0,media:'audio',encoding:'',key_file:''}];
 const edit=(index:number,key:string,value:any)=>onChange(option(row,'track_keys',entries.map((entry,i)=>i===index?{...entry,[key]:value}:entry)));
 return <>
  <p className="muted">Map every active packetizer track by ID, media and codec. Use different key material per track; a track change stops this destination until the map is updated.</p>
  {entries.map((entry,index)=><div className="form-details" key={index}>
   <Field label={`SRTP track ID ${destination}.${index+1}`}><input type="number" min="1" max="4294967295" value={entry.track_id||''} onChange={e=>edit(index,'track_id',e.target.value===''?0:Number(e.target.value))}/></Field>
   <Field label={`SRTP track media ${destination}.${index+1}`}><select value={entry.media||'audio'} onChange={e=>edit(index,'media',e.target.value)}><option value="audio">Audio</option><option value="video">Video</option></select></Field>
   <Field label={`SRTP track encoding ${destination}.${index+1}`} help="Use the active output codec, such as H264/90000, H265/90000, MPEG4-GENERIC/48000/2 or MPA/90000."><input value={entry.encoding||''} placeholder="MPEG4-GENERIC/48000/2" list={`srtp-encodings-${destination}`} onChange={e=>edit(index,'encoding',e.target.value)}/></Field>
   <KeyField label={`SRTP track key file ${destination}.${index+1}`} value={entry.key_file||''} onChange={v=>edit(index,'key_file',v)}/>
   {entries.length>1&&<button type="button" onClick={()=>onChange(option(row,'track_keys',entries.filter((_,i)=>i!==index)))}>Remove SRTP track {destination}.{index+1}</button>}
  </div>)}
  <datalist id={`srtp-encodings-${destination}`}><option value="H264/90000"/><option value="H265/90000"/><option value="MPEG4-GENERIC/48000/2"/><option value="MPA/90000"/></datalist>
  <button type="button" disabled={entries.length>=8} onClick={()=>onChange(option(row,'track_keys',[...entries,{track_id:0,media:'audio',encoding:'',key_file:''}]))}>Add SRTP track {destination}</button>
 </>;
}
export function DirectOutputFields({value,inherited,onChange,currentTracks}:{value:Item,inherited?:Item[],onChange:(v:Item[]|undefined)=>void,currentTracks?:Item[]}) {
 const mode=owns(value,'flussonix_rtp_outputs')?(value.flussonix_rtp_outputs?.length?'override':'none'):'inherit';
 const rows:Item[]=value.flussonix_rtp_outputs||[];
 const edit=(i:number,row:Item)=>onChange(rows.map((entry,n)=>n===i?row:entry));
 return <fieldset><legend>RTP and SRTP destinations</legend>
  <p className="muted">Send processed MPEG-TS (PT33) or elementary audio/video. Each track uses a consecutive RTP/RTCP pair. Elementary output provides a key-free SDP download on the Output tab.</p>
  {currentTracks?.length?<p className="muted">Current output tracks: {currentTracks.map(track=>`${track.id} · ${track.media} · ${track.encoding}`).join('; ')}</p>:null}
  <Field label="Direct transport destinations"><select value={mode} onChange={e=>onChange(e.target.value==='inherit'?undefined:e.target.value==='none'?[]:inherited?.length?structuredClone(inherited):[{url:''}])}><option value="inherit">{value.template?'Use template destinations':'No destinations (default)'}</option><option value="override">Override destinations</option><option value="none">No destinations</option></select></Field>
  {mode==='inherit'&&value.template&&<div className="muted">{rtpSummary(inherited||[])}</div>}
  {mode==='override'&&<>{rows.map((row,i)=>{
   const opts=row.flussonix_rtp||{};
   const secure=row.url?.startsWith('srtp://');
   const elementary=opts.profile==='elementary';
   const perTrack=!!opts.track_keys;
   const profile=(selected:string)=>{let next=option(row,'profile',selected==='mp2t'?undefined:selected);if(selected==='mp2t')next=clear(next,['track_keys']);edit(i,next)};
   const protection=(selected:string)=>{let next=clear(row,['key_file','track_keys']);if(selected==='per-track')next=option(next,'track_keys',[{track_id:0,media:'audio',encoding:'',key_file:''}]);edit(i,next)};
   return <div className="form-details" key={i}><h3>RTP destination {i+1}</h3><div className="form-grid">
    <Field label={`RTP destination URL ${i+1}`} help="Literal receiver IP and port (1024–65534), without query parameters or credentials."><input value={row.url||''} placeholder="rtp://192.168.1.20:5004" onChange={e=>edit(i,e.target.value.startsWith('srtp://')?{...row,url:e.target.value}:withoutSRTP({...row,url:e.target.value}))}/></Field>
    <Field label={`RTP destination profile ${i+1}`}><select value={opts.profile||'mp2t'} onChange={e=>profile(e.target.value)}><option value="mp2t">MPEG-TS / PT33</option><option value="elementary">Elementary audio/video · SDP</option></select></Field>
    <Field label={`RTP interface IP ${i+1}`} help="Optional IPv4 source interface; required for multicast."><input value={opts.interface||''} onChange={e=>edit(i,option(row,'interface',e.target.value))}/></Field>
    <Field label={`RTP multicast TTL ${i+1}`}><input type="number" min="1" max="255" placeholder="16" value={opts.ttl??''} onChange={e=>edit(i,option(row,'ttl',e.target.value===''?'':Number(e.target.value)))}/></Field>
    <Field label={`RTP maximum bitrate (Mbps) ${i+1}`}><input type="number" min="1" max="10000" placeholder="100" value={row.max_mbps??''} onChange={e=>{const next={...row};if(e.target.value==='')delete next.max_mbps;else next.max_mbps=Number(e.target.value);edit(i,next)}}/></Field>
    {secure&&elementary&&<Field label={`SRTP destination protection ${i+1}`} help="One shared key or a distinct key for each named output track."><select value={perTrack?'per-track':'shared'} onChange={e=>protection(e.target.value)}><option value="shared">Shared key</option><option value="per-track">Different key per track</option></select></Field>}
    {secure&&(perTrack&&elementary?<OutputTrackRows row={row} onChange={v=>edit(i,v)} destination={i+1}/>:<KeyField label={`SRTP key file ${i+1}`} value={opts.key_file||''} onChange={v=>edit(i,option(row,'key_file',v))}/>)}
    <Field label={`RTP destination enabled ${i+1}`}><input type="checkbox" checked={!row.disabled} onChange={e=>edit(i,{...row,disabled:!e.target.checked})}/></Field>
   </div><button type="button" onClick={()=>onChange(rows.filter((_,n)=>n!==i))}>Remove RTP destination {i+1}</button></div>;
  })}<button type="button" disabled={rows.length>=4} onClick={()=>onChange([...rows,{url:''}])}>Add RTP destination</button></>}
 </fieldset>;
}
function encoding(value:unknown,media:unknown):boolean {
 if(typeof value!=='string')return false;
 if(media==='video')return ['H264/90000','H265/90000'].includes(value);
 if(media!=='audio')return false;
 if(value==='MPA/90000')return true;
 const match=/^MPEG4-GENERIC\/(\d+)\/([1-8])$/.exec(value);
 return !!match&&['96000','88200','64000','48000','44100','32000','24000','22050','16000','12000','11025','8000','7350'].includes(match[1]);
}
export function rtpError(value:Item):string|undefined {
 for(const row of [...(value.inputs||[]),...(value.flussonix_rtp_outputs||[])]) {
  if(!directRTP(row.url||''))continue;
  if(/[\s\x00-\x1f\x7f]/.test(row.url)||row.url.split('://')[1]?.replace(/\/$/,'').includes('/'))return 'RTP URL cannot contain whitespace or a path.';
  try {const url=new URL(row.url);const host=url.hostname.replace(/^\[|\]$/g,'');const ipv4=/^(\d{1,3}\.){3}\d{1,3}$/.test(host)&&host.split('.').every((number:string)=>Number(number)<=255);if((!ipv4&&!host.includes(':'))||!url.port||Number(url.port)<1024||Number(url.port)>65534||url.search||url.hash||url.username||url.password||!['','/'].includes(url.pathname))return 'RTP URL needs a literal IP and port 1024–65534, without path, query or credentials.';}catch{return 'Enter a complete RTP or SRTP IP address and port.'}
  const opts=row.flussonix_rtp||{};
  const input=value.inputs?.includes(row);
  const elementary=opts.profile==='elementary';
  const secure=row.url.startsWith('srtp://');
  for(const [key,min,max] of [['jitter_ms',0,1000],['ttl',1,255]] as const)if(owns(opts,key)&&(!Number.isInteger(opts[key])||opts[key]<min||opts[key]>max))return `RTP ${key==='ttl'?'TTL':'jitter'} must be a whole number from ${min} to ${max}.`;
  if(owns(opts,'receive_buffer_bytes')){if(!input)return 'RTP receive buffer applies only to inputs.';if(!Number.isInteger(opts.receive_buffer_bytes)||opts.receive_buffer_bytes<65536||opts.receive_buffer_bytes>33554432)return 'RTP receive buffer must be from 0.0625 to 32 MiB.';}
  if(elementary){if(Number(new URL(row.url).port)>65520)return 'Elementary RTP needs a base port from 1024 to 65520.';if(input&&!path(opts.sdp_file))return 'Elementary RTP input needs an absolute SDP file path on this node.';}
  if(!secure&&secureKeys.some(key=>owns(opts,key)))return 'Plaintext RTP cannot keep SRTP key or receiver options.';
  if(secure){
   if(owns(opts,'allow_legacy_reused_key')&&(!input||typeof opts.allow_legacy_reused_key!=='boolean'))return 'Legacy receiver-key reuse applies only to secure inputs.';
   if(input){
    if(owns(opts,'track_keys'))return 'Output track keys cannot be used on an SRTP input.';
    if(elementary&&owns(opts,'track_key_files')){
     if(owns(opts,'key_file')||owns(opts,'expected_ssrcs'))return 'Choose one SRTP input protection mode.';
     const keys=opts.track_key_files;
     if(!Array.isArray(keys)||keys.length<1||keys.length>8||keys.some((entry:unknown)=>!path(entry))||new Set(keys).size!==keys.length)return 'Enter 1–8 distinct absolute track key file paths.';
    }else{
     if(!path(opts.key_file))return 'SRTP requires an absolute key file path on this node.';
     if(owns(opts,'track_key_files'))return 'Track key files require elementary SRTP.';
     if(owns(opts,'expected_ssrcs')){
      const ids=opts.expected_ssrcs;
      if(!elementary||!Array.isArray(ids)||ids.length<1||ids.length>8||ids.some((id:unknown)=>!nonzero(id))||new Set(ids).size!==ids.length)return 'Enter 1–8 distinct nonzero expected SSRCs for elementary SRTP.';
     }
    }
   }else{
    if(owns(opts,'track_key_files')||owns(opts,'expected_ssrcs')||owns(opts,'allow_legacy_reused_key'))return 'Receiver SRTP options cannot be used on a destination.';
    if(!elementary&&owns(opts,'track_keys'))return 'Per-track keys require elementary SRTP output.';
    if(elementary&&owns(opts,'track_keys')){
     if(owns(opts,'key_file'))return 'Choose one SRTP destination protection mode.';
     const keys=opts.track_keys;
     if(!Array.isArray(keys)||keys.length<1||keys.length>8||keys.some((entry:Item)=>!nonzero(entry?.track_id)||!encoding(entry?.encoding,entry?.media)||!path(entry?.key_file))||new Set(keys.map((entry:Item)=>entry.track_id)).size!==keys.length||new Set(keys.map((entry:Item)=>entry.key_file)).size!==keys.length)return 'Enter 1–8 output tracks with distinct IDs and absolute key paths.';
    }else if(!path(opts.key_file))return 'SRTP requires an absolute key file path on this node.';
   }
  }
  if(owns(row,'max_mbps')&&(!Number.isInteger(row.max_mbps)||row.max_mbps<1||row.max_mbps>10000))return 'RTP maximum bitrate must be 1–10000 Mbps.';
 }
}
