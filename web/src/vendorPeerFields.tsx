import React from 'react';
import {Field,type Item} from './forms';
export function PeerTypeField({value,onChange}:{value:Item,onChange:(v:Item)=>void}) {
 return <Field label="Peer type" help="Flussonic destinations use HTTP/HTTPS viewer redirects and their own viewer policy."><select value={value.flussonix_peer_kind||'native'} onChange={e=>{const next:Item={...value,flussonix_peer_kind:e.target.value};for(const key of ['cluster_key','flussonix_stream_map','flussonix_rtsp_url','flussonix_rtsps_url','private_payload_url'])delete next[key];if(e.target.value==='flussonic')next.flussonix_stream_map=[];onChange(next)}}><option value="native">FlussoniX</option><option value="flussonic">Flussonic</option></select></Field>;
}
export function VendorPeerMappings({value,onChange}:{value:Item,onChange:(v:Item)=>void}) {
 const rows=value.flussonix_stream_map||[];
 const update=(index:number,key:string,text:string)=>onChange({...value,flussonix_stream_map:rows.map((row:Item,i:number)=>i===index?{...row,[key]:text}:row)});
 return <fieldset><legend>Stream mappings</legend><p className="muted">Choose streams already available on this CDN, including configured streams waiting for a viewer. Map each LB name to its CDN name. The CDN independently checks viewer access.</p>{rows.map((row:Item,i:number)=><div key={i} className="form-grid"><Field label={`LB stream name ${i+1}`}><input value={row.name||''} maxLength={256} placeholder="region/news" onChange={e=>update(i,'name',e.target.value)}/></Field><Field label={`CDN stream name ${i+1}`}><input value={row.remote_name||''} maxLength={256} placeholder="news" onChange={e=>update(i,'remote_name',e.target.value)}/></Field><button type="button" onClick={()=>onChange({...value,flussonix_stream_map:rows.filter((_:Item,n:number)=>n!==i)})}>Remove mapping {i+1}</button></div>)}<button type="button" disabled={rows.length>=128} onClick={()=>onChange({...value,flussonix_stream_map:[...rows,{name:'',remote_name:''}]})}>Add stream mapping</button><p className="muted">Capacity holds apply to this LB only. Multiple LBs need shared capacity coordination; Flussonic does not redeem FlussoniX admission tickets.</p></fieldset>;
}
const validName=(s:unknown)=>typeof s==='string'&&s.length>0&&new TextEncoder().encode(s).length<=256&&!/[\\?#\x00-\x1f\x7f-\x9f]/.test(s)&&s.split('/').every(p=>p&&p!=='.'&&p!=='..');
export function vendorPeerError(value:Item):string|undefined {
 if(value.flussonix_peer_kind!==undefined&&!['native','flussonic'].includes(value.flussonix_peer_kind))return 'Choose FlussoniX or Flussonic as the peer type.';
 if(value.flussonix_peer_kind!=='flussonic')return value.flussonix_stream_map!==undefined?'Stream mappings require a Flussonic peer.':undefined;
 if(!value.cluster_key)return 'Cluster key is required for a Flussonic peer. It does not authorize viewers.';
 for(const [key,label] of [['api_url','Management'],['public_payload_url','Public delivery']]){try{const u=new URL(value[key]);if(!['http:','https:'].includes(u.protocol)||!u.hostname||u.username||u.password||u.search||u.hash||u.port==='0')return `${label} URL must use HTTP or HTTPS without credentials, query or fragment.`;}catch{return `${label} URL is required.`;}}
 if(!Array.isArray(value.flussonix_stream_map)||!value.flussonix_stream_map.length||value.flussonix_stream_map.length>128)return 'Add 1 to 128 stream mappings.';
 if(value.flussonix_stream_map.some((r:Item)=>!validName(r.name)||!validName(r.remote_name)))return 'Each mapping needs valid LB and CDN stream names.';
 if(new Set(value.flussonix_stream_map.map((r:Item)=>r.name)).size!==value.flussonix_stream_map.length)return 'Each LB stream name may appear only once.';
}
