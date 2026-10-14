import React,{useState} from 'react';
import {Field,SettingsSummary,sourceEndpoint,type Item} from './forms';
import {GPUReadiness} from './gpuReadiness';

const policyFields=['on_play','flussonix_token_sha256','group_config'];
const supported=new Set(['url','api_url','private_payload_url','cluster_key','prefix','only','except','flussonix_tls_ca','flussonix_media_tls_ca','flussonix_source_kind','flussonix_transport','flussonix_output_template','flussonix_source_group','flussonix_content_id',...policyFields]);
const owns=(v:Item,k:string)=>Object.hasOwn(v,k);
const validName=(name:string)=>!!name&&new TextEncoder().encode(name).length<=256&&!name.split('/').some(p=>!p||p==='.'||p==='..')&&!/[\\?#\x00-\x1f\x7f-\x9f]/.test(name);
const httpURL=(raw:string)=>{try{const u=new URL(raw);return ['http:','https:'].includes(u.protocol)&&!!u.hostname;}catch{return false}};
const transport=(v:Item)=>v.url?.startsWith('m4s:')||v.url?.startsWith('m4ss:')?'m4s':'m4f';
export function unsupportedVendorFields(value:Item):string[]{
 return Object.keys(value).filter(k=>!supported.has(k)||k==='flussonix_transport'&&value[k]!==transport(value));
}
export function vendorSourceError(value:Item):string|undefined {
 if(value.flussonix_source_kind!==undefined&&!['native','flussonic'].includes(value.flussonix_source_kind))return 'Choose Native FlussoniX or Flussonic as the source type.';
 if(value.flussonix_source_kind!=='flussonic')return policyFields.some(k=>owns(value,k))?'Clear vendor viewer policies before switching to a native source.':owns(value,'flussonix_output_template')?'Clear vendor processing template before switching to a native source.':owns(value,'flussonix_content_id')?'Clear vendor content identity before switching to a native source.':undefined;
 if(owns(value,'flussonix_output_template')&&(typeof value.flussonix_output_template!=='string'||!validName(value.flussonix_output_template)))return 'Choose a valid processing template.';
 if(!owns(value,'url')||!sourceEndpoint(value.url))return 'Flussonic sources require an M4F or M4S source address.';
 if(unsupportedVendorFields(value).length)return 'Remove unsupported native settings before saving a Flussonic source.';
 if(owns(value,'flussonix_source_group')!==owns(value,'flussonix_content_id'))return 'Vendor failover requires both a group and content identity.';
 for(const field of ['flussonix_source_group','flussonix_content_id'])if(owns(value,field)&&!(/^[A-Za-z0-9._-]{1,128}$/).test(value[field]))return 'Replica identifiers require 1 to 128 letters, digits, dots, underscores or hyphens.';
 if(typeof value.cluster_key!=='string'||!value.cluster_key)return 'Cluster key is required for a Flussonic source. It does not authorize viewers.';
 const policyError=(policy:Item,label:string):string|undefined=>{
  if(owns(policy,'flussonix_token_sha256')&&!/^[a-f\d]{64}$/i.test(policy.flussonix_token_sha256))return `${label} token SHA256 must contain exactly 64 hexadecimal characters.`;
  if(!owns(policy,'on_play')||policy.on_play==='true')return;
  const a=typeof policy.on_play==='string'?{url:policy.on_play}:policy.on_play;
  if(!a||typeof a.url!=='string'||(a.url.startsWith('auth://')?!validName(a.url.slice(7)):!httpURL(a.url)))return `${label} callback must use HTTP or HTTPS, or choose a named backend.`;
  if(a.max_sessions!==undefined&&(!Number.isInteger(a.max_sessions)||a.max_sessions<1))return `${label} maximum sessions must be a positive integer.`;
  if(a.session_keys&&(!Array.isArray(a.session_keys)||!a.session_keys.includes('name')||!a.session_keys.includes('proto')||a.session_keys.length>32||a.session_keys.some((k:string)=>!['name','proto','ip','token'].includes(k))))return `${label} session identity must include stream name and protocol, with at most 32 keys.`;
 };
 const error=policyError(value,'Viewer');if(error)return error;
 if(value.group_config!==undefined){
  if(!value.group_config||Array.isArray(value.group_config)||typeof value.group_config!=='object'||Object.keys(value.group_config).length>128)return 'Use at most 128 group overrides.';
  for(const [name,policy] of Object.entries(value.group_config) as [string,Item][]){
   if(!validName(name))return 'Group names must be valid stream-style names of at most 256 UTF-8 bytes.';
   if(!policy||typeof policy!=='object'||Array.isArray(policy)||Object.keys(policy).some(k=>!['name','on_play','flussonix_token_sha256'].includes(k))||owns(policy,'name')&&policy.name!==name)return 'Group overrides may contain only viewer policies.';
   const error=policyError(policy,`Group ${name}`);if(error)return error;
  }
 }
}
export function SourceTypeField({value,onChange,locked}:{value:Item,onChange:(v:Item)=>void,locked:boolean}) {
 return <Field label="Source type" help={locked&&!owns(value,'url')?"This existing node-name identity is fixed. Create a source URL entry to use Flussonic.":"Native is the default. Flussonic sources require a server URL, Cluster key and a separate local viewer policy."}><select value={value.flussonix_source_kind||'native'} onChange={e=>{
  const next:Item={...value,flussonix_source_kind:e.target.value};
  if(e.target.value==='flussonic'&&!owns(next,'url')&&!locked){delete next.hostname;next.url='';}
  for(const k of ['api_url','private_payload_url'])if(next[k]==='')delete next[k];
  onChange(next);
 }}><option value="native">Native FlussoniX</option><option value="flussonic" disabled={locked&&!owns(value,'url')}>Flussonic</option>{!['native','flussonic',undefined].includes(value.flussonix_source_kind)&&<option value={value.flussonix_source_kind}>Unsupported source type</option>}</select></Field>;
}
export function VendorPolicyReset({value,onChange}:{value:Item,onChange:(v:Item)=>void}) {
 if(value.flussonix_source_kind==='flussonic')return null;
 return <>{policyFields.some(k=>owns(value,k))&&<section className="panel padded"><p>Vendor viewer policies are still present. Clear them explicitly to finish switching this source to native discovery.</p><button type="button" onClick={()=>{const next={...value};for(const k of policyFields)delete next[k];onChange(next)}}>Clear vendor viewer policies</button></section>}
 {owns(value,'flussonix_content_id')&&<section className="panel padded"><p>A vendor content identity is still present. Clear it explicitly before using native discovery. The failover group remains editable below.</p><button type="button" onClick={()=>{const next={...value};delete next.flussonix_content_id;onChange(next)}}>Clear vendor content identity</button></section>}
 {owns(value,'flussonix_output_template')&&<section className="panel padded"><p>A vendor processing template is still selected. Clear it explicitly before using native discovery.</p><button type="button" onClick={()=>{const next={...value};delete next.flussonix_output_template;onChange(next)}}>Clear vendor processing template</button></section>}</>;
}
function PolicyFields({value,onChange,backends,label,group=false}:{value:Item,onChange:(v:Item)=>void,backends:Item[],label:string,group?:boolean}) {
 const raw=typeof value.on_play==='string'?{url:value.on_play}:value.on_play||{};
 const mode=!owns(value,'on_play')?'inherit':value.on_play==='true'?'allow':raw.url?.startsWith('auth://')?'backend':'url';
 const set=(k:string,v:unknown)=>{const next={...value};if(v===undefined||v==='')delete next[k];else next[k]=v;onChange(next)};
 const keys=raw.session_keys||['name','proto','ip','token'];
 const authSet=(k:string,v:unknown)=>{const next={...raw};if(v===undefined)delete next[k];else next[k]=v;set('on_play',next)};
 return <><div className="form-grid">
  <Field label={`${label} policy`} help={group?'Unset fields inherit source policy. Clearing a group token retains an inherited source token guard.':'Without an applicable callback, explicit allow policy or token guard, playback is denied. Cluster credentials never grant viewer access.'}><select value={mode} onChange={e=>set('on_play',e.target.value==='inherit'?undefined:e.target.value==='allow'?'true':{...(mode==='backend'||mode==='url'?raw:{}),url:e.target.value==='backend'?'auth://'+(backends[0]?.name||''):'https://'})}><option value="inherit">{group?'Use source policy':'No callback (token guard required)'}</option><option value="allow">Allow without callback</option><option value="backend">Named backend</option><option value="url">Callback URL</option></select></Field>
  {mode==='backend'&&<Field label={`${label} backend`}><select value={raw.url.slice(7)} onChange={e=>authSet('url','auth://'+e.target.value)}><option value="">Choose a backend</option>{backends.map(b=><option key={b.name} value={b.name}>{b.name}</option>)}</select></Field>}
  {mode==='url'&&<Field label={`${label} callback URL`}><input value={raw.url||''} onChange={e=>authSet('url',e.target.value)}/></Field>}
  {(mode==='url'||mode==='backend')&&<Field label={`${label} maximum sessions`} help="Optional per-stream viewer limit on this node."><input type="number" min="1" value={raw.max_sessions??''} onChange={e=>authSet('max_sessions',e.target.value===''?undefined:Number(e.target.value))}/></Field>}
  <Field label={`${label} token SHA256`} help="Optional 64-character SHA256 hash. The token guard applies before callback authorization."><input type="password" autoComplete="off" value={value.flussonix_token_sha256||''} onChange={e=>set('flussonix_token_sha256',e.target.value)}/></Field>
 </div>{(mode==='backend'||mode==='url')&&<details className="form-details"><summary>{label} session identity</summary><p className="muted">Ordered keys determine which requests share a session. Stream name and protocol are required.</p>{keys.map((key:string,i:number)=><div className="input-row" key={i}><Field label={`${label} session key ${i+1}`}><select value={key} onChange={e=>authSet('session_keys',keys.map((k:string,n:number)=>n===i?e.target.value:k))}>{['name','proto','ip','token'].map(k=><option key={k} value={k}>{k==='name'?'Stream name':k==='proto'?'Protocol':k==='ip'?'Client IP':'Viewer token'}</option>)}</select></Field><button type="button" disabled={!i} aria-label={`Move ${label.toLowerCase()} session key ${i+1} up`} onClick={()=>{const next=[...keys];[next[i-1],next[i]]=[next[i],next[i-1]];authSet('session_keys',next)}}>↑</button><button type="button" onClick={()=>authSet('session_keys',keys.filter((_:string,n:number)=>n!==i))}>Remove</button></div>)}<button type="button" disabled={keys.length>=32} onClick={()=>authSet('session_keys',[...keys,['name','proto','ip','token'].find(k=>!keys.includes(k))||'token'])}>Add {label.toLowerCase()} session key</button></details>}</>;
}
export function VendorSourcePolicies({value,onChange,backends,templates,capabilities}:{value:Item,onChange:(v:Item)=>void,backends:Item[],templates:Item[],capabilities:Item}) {
 const [name,setName]=useState('');const [error,setError]=useState('');
 const groups:Item=value.group_config||{};const unsupported=unsupportedVendorFields(value);
 const setGroups=(next:Item)=>{const source={...value};if(Object.keys(next).length)source.group_config=next;else delete source.group_config;onChange(source)};
 const selected=templates.find(t=>t.name===value.flussonix_output_template);
 const processing=Object.fromEntries(['transcoder','flussonix_input_timeout','flussonix_subtitle_tracks','flussonix_hls_subtitles','flussonix_hls_captions'].filter(k=>selected&&owns(selected,k)).map(k=>[k,selected![k]]));
 return <>
  <p className="muted">HTTP/HTTPS delivery: HLS, MPEG-TS, M4S and M4F. FlussoniX load balancers can discover these sources and route viewers to FlussoniX CDNs. Selected static streams stay active without viewers on CDN and standalone nodes. Equivalent source groups recover automatically while static ownership or recent authorized viewer demand remains. Vendor RTSP/SRT delivery is not available yet.</p>
  <section role="region" aria-label="Vendor output processing"><fieldset><legend>Output processing</legend><Field label="Processing template" help="Configure CPU/GPU encoding and HLS passthrough, conversion or filtering in Media → Templates. Only processing settings are selected; source credentials and viewer authorization stay separate."><select value={value.flussonix_output_template||''} onChange={e=>{const next={...value};if(e.target.value)next.flussonix_output_template=e.target.value;else delete next.flussonix_output_template;onChange(next)}}><option value="">Copy / default subtitles</option>{templates.map(t=><option key={t.name} value={t.name}>{t.name}</option>)}{value.flussonix_output_template&&!selected&&<option value={value.flussonix_output_template}>{value.flussonix_output_template} (unavailable)</option>}</select></Field>
  {selected?<SettingsSummary value={processing} empty="Copy output and default subtitle filtering apply."/>:<p className="muted">{value.flussonix_output_template?'The selected template is unavailable. Select an existing template before saving.':'Copy output and default subtitle filtering apply.'}</p>}
  {selected&&/_(nvenc|vaapi)$/.test(selected.transcoder?.encoder||'')&&<GPUReadiness transcoding={capabilities.transcoding} error={capabilities.error} encoder={selected.transcoder.encoder}/>}
  <p className="muted">USA captions, European DVB/teletext and native text selectors use the existing template controls. Preserving native text with transcoding is unsupported; choose filtering or HLS conversion. GPU caption conversion is not qualified.</p>
  </fieldset></section>
  {!!unsupported.length&&<section className="panel padded"><p>Unsupported native settings: {unsupported.map(k=>({public_payload_url:'Public delivery URL',flussonix_transport:'Mismatched source transport'} as Item)[k]||k.replaceAll('_',' ')).join(', ')}.</p><button type="button" onClick={()=>{const next={...value};for(const k of unsupported)delete next[k];onChange(next)}}>Remove unsupported native settings</button></section>}
  <fieldset><legend>Viewer authorization</legend><PolicyFields value={value} onChange={onChange} backends={backends} label="Viewer"/></fieldset>
  <fieldset className="vendor-policies"><legend>Group policy overrides</legend><p className="muted">Match upstream group names without renaming streams. Unset fields inherit source policy; conflicting policies from matching groups deny playback.</p>
   {Object.entries(groups).map(([group,policy])=><fieldset key={group}><legend>Group {group}</legend><PolicyFields value={policy} onChange={next=>setGroups({...groups,[group]:next})} backends={backends} label={`Group ${group}`} group/><button type="button" aria-label={`Remove group ${group}`} onClick={()=>{const next={...groups};delete next[group];setGroups(next)}}>Remove group</button></fieldset>)}
   <div className="input-row"><Field label="New group name"><input maxLength={256} value={name} onChange={e=>{setName(e.target.value);setError('')}}/></Field><button type="button" disabled={Object.keys(groups).length>=128} onClick={()=>{if(!validName(name)){setError('Enter a valid group name.');return;}if(owns(groups,name)){setError('That group already has an override.');return;}setGroups({...groups,[name]:{}});setName('');setError('')}}>Add group override</button></div>{error&&<p className="error" role="status">{error}</p>}
  </fieldset>
 </>;
}
