import React from 'react';
export type Item = Record<string, any>;
export type Kind = 'streams' | 'templates' | 'sources' | 'peers' | 'auth_backends';
export const kindLabel: Record<Kind,string> = {streams:'stream',templates:'template',sources:'source',peers:'peer',auth_backends:'auth backend'};
const owns=(v:Item,k:string)=>Object.prototype.hasOwnProperty.call(v,k);
export function Field({label,help,children}:{label:string,help?:string,children:React.ReactNode}) {
 const id=React.useId();
 return <div className="field"><label htmlFor={id}>{label}</label>{React.isValidElement(children)?React.cloneElement(children as React.ReactElement<{id:string;'aria-describedby'?:string}>,{id,'aria-describedby':help?id+'-help':undefined}):children}{help&&<small id={id+'-help'} className="field-help">{help}</small>}</div>;
}
export function ConfigurationFields({kind,value,onChange,templates,backends,locked=false}:{kind:Kind,value:Item,onChange:(v:Item)=>void,templates:Item[],backends:Item[],locked?:boolean}) {
 const set=(key:string,v:any)=>{const next={...value};if(v===undefined||v==='')delete next[key];else next[key]=v;onChange(next)};
 const updateInput=(i:number,url:string)=>set('inputs',(value.inputs||[]).map((v:Item,n:number)=>n===i?{...v,url}:v));
 const media=kind==='streams'||kind==='templates';
 const rawAuth=typeof value.on_play==='string'?{url:value.on_play}:value.on_play||{};
 const authURL=rawAuth.url||'';
 const authMode=!authURL?'inherit':authURL.startsWith('auth://')?'backend':'url';
 const authSet=(v:Item)=>set('on_play',{...rawAuth,...v});
 const template=value.template;
 return <>
 <div className="form-grid">
  <Field label={kind==='streams'?'Stream name':kind==='templates'?'Template name':kind==='auth_backends'?'Backend name':'Node name'} help={locked?'The existing name is fixed. Create a new entry to use another name.':media?'Use a unique name. Slashes may organize streams into groups.':undefined}>
   <input required readOnly={locked} value={value.name||value.hostname||''} onChange={e=>set(media||kind==='auth_backends'?'name':'hostname',e.target.value)}/>
  </Field>
  {media&&<Field label="Title"><input value={value.title||''} onChange={e=>set('title',e.target.value)}/></Field>}
 </div>
 {media?<>
  {kind==='streams'&&<div className="form-grid"><Field label="Template"><select value={template||''} onChange={e=>set('template',e.target.value||undefined)}><option value="">None</option>{templates.map(t=><option key={t.name} value={t.name}>{t.name}</option>)}</select></Field><Field label="Activation" help="Static streams run continuously. On-demand streams start when requested."><select value={owns(value,'static')?value.static?'static':'ondemand':'inherit'} onChange={e=>set('static',e.target.value==='inherit'?undefined:e.target.value==='static')}><option value="inherit">{template?'Use template setting':'Use server default (static)'}</option><option value="static">Always running</option><option value="ondemand">On demand</option></select></Field></div>}
  {kind==='templates'&&<div className="form-grid"><Field label="Activation"><select value={owns(value,'static')?value.static?'static':'ondemand':'inherit'} onChange={e=>set('static',e.target.value==='inherit'?undefined:e.target.value==='static')}><option value="inherit">Use server default (static)</option><option value="static">Always running</option><option value="ondemand">On demand</option></select></Field></div>}
  <fieldset><legend>Inputs</legend><p className="muted">Inputs are tried in order. Use testsrc:// for an owned test stream.</p>
   {template&&<Field label="Input settings"><select value={owns(value,'inputs')?'override':'inherit'} onChange={e=>set('inputs',e.target.value==='inherit'?undefined:[{url:''}])}><option value="inherit">Use template inputs</option><option value="override">Set stream inputs</option></select></Field>}
   {(!template||owns(value,'inputs'))&&<>{(value.inputs||[]).map((v:Item,i:number)=><div className="input-row" key={i}><Field label={(value.inputs||[]).length===1?'Input URL':`Input URL ${i+1}`}><input value={v.url||''} placeholder="m4s://origin.example/channel" onChange={e=>updateInput(i,e.target.value)}/></Field><button type="button" aria-label={`Move input ${i+1} up`} disabled={!i} onClick={()=>{const a=[...value.inputs];[a[i-1],a[i]]=[a[i],a[i-1]];set('inputs',a)}}>↑</button><button type="button" aria-label={`Remove input ${i+1}`} onClick={()=>set('inputs',value.inputs.filter((_:Item,n:number)=>n!==i))}>Remove</button></div>)}<button type="button" onClick={()=>set('inputs',[...(value.inputs||[]),{url:''}])}>{value.inputs?.length?'Add fallback input':'Add input'}</button></>}
  </fieldset>
  <fieldset><legend>Processing</legend><div className="form-grid"><Field label="Transcoding" help={template?'Inherit keeps the template encoder. Copy creates an explicit passthrough override.':'NVIDIA encoding requires supported hardware and drivers.'}><select value={owns(value,'transcoder')?value.transcoder?.encoder||'libx264':'inherit'} onChange={e=>set('transcoder',e.target.value==='inherit'?undefined:e.target.value==='copy'?{encoder:'copy'}:{encoder:e.target.value,vb:value.transcoder?.vb||900})}><option value="inherit">{template?'Use template settings':'Copy stream codecs (default)'}</option><option value="copy">Copy stream codecs</option><option value="libx264">CPU · H.264</option><option value="h264_nvenc">NVIDIA GPU · H.264</option></select></Field>
   {value.transcoder&&value.transcoder.encoder!=='copy'&&<Field label="Video bitrate (kb/s)"><input type="number" min="100" max="50000" value={value.transcoder.vb??900} onChange={e=>set('transcoder',{...value.transcoder,vb:e.target.value===''?'':Number(e.target.value)})}/></Field>}
   <Field label="Stream availability"><select value={owns(value,'disabled')?value.disabled?'disabled':'enabled':'inherit'} onChange={e=>set('disabled',e.target.value==='inherit'?undefined:e.target.value==='disabled')}><option value="inherit">{template?'Use template setting':'Enabled by default'}</option><option value="enabled">Enabled</option><option value="disabled">Disabled</option></select></Field>
  </div></fieldset>
  <fieldset><legend>Viewer authorization</legend><div className="form-grid"><Field label="Authorization policy" help={template?'Use template policy retains inherited viewer protection.':'No callback is used unless a policy is configured.'}><select value={authMode} onChange={e=>set('on_play',e.target.value==='inherit'?undefined:{url:e.target.value==='backend'?'auth://'+(backends[0]?.name||''):'https://'})}><option value="inherit">{template?'Use template policy':'No callback'}</option><option value="backend">Named backend</option><option value="url">Callback URL</option></select></Field>
   {authMode==='backend'&&<Field label="Authorization backend"><select value={authURL.slice(7)} onChange={e=>authSet({url:'auth://'+e.target.value})}><option value="">Choose a backend</option>{backends.map(b=><option key={b.name} value={b.name}>{b.name}</option>)}</select></Field>}
   {authMode==='url'&&<Field label="Callback URL"><input value={authURL} onChange={e=>authSet({url:e.target.value})}/></Field>}
   {authMode!=='inherit'&&<Field label="Maximum viewer sessions" help="Optional limit for this stream on this node."><input type="number" min="1" value={rawAuth.max_sessions??''} onChange={e=>{const next={...rawAuth};if(!e.target.value)delete next.max_sessions;else next.max_sessions=Number(e.target.value);set('on_play',next)}}/></Field>}
   <Field label="Viewer token SHA256" help={template?'An optional SHA256 hash override. Leaving it unset retains any template guard.':'Optional SHA256 hash of the required viewer token.'}><input type="password" autoComplete="off" value={value.flussonix_token_sha256||''} onChange={e=>set('flussonix_token_sha256',e.target.value)}/></Field>
  </div>
  {authMode!=='inherit'&&<details className="form-details"><summary>Session identity</summary><p className="muted">The ordered keys determine which requests share a session. Name and protocol are required.</p>{(rawAuth.session_keys||['name','proto','ip','token']).map((k:string,i:number)=><div className="input-row" key={i}><Field label={`Session key ${i+1}`}><select value={k} onChange={e=>authSet({session_keys:(rawAuth.session_keys||['name','proto','ip','token']).map((v:string,n:number)=>n===i?e.target.value:v)})}>{['name','proto','ip','token'].map(key=><option key={key} value={key}>{key==='proto'?'Protocol':key==='ip'?'Client IP':key==='name'?'Stream name':'Viewer token'}</option>)}</select></Field><button type="button" disabled={!i} aria-label={`Move session key ${i+1} up`} onClick={()=>{const a=[...(rawAuth.session_keys||['name','proto','ip','token'])];[a[i-1],a[i]]=[a[i],a[i-1]];authSet({session_keys:a})}}>↑</button><button type="button" onClick={()=>authSet({session_keys:(rawAuth.session_keys||['name','proto','ip','token']).filter((_:string,n:number)=>n!==i)})}>Remove</button></div>)}<button type="button" onClick={()=>authSet({session_keys:[...(rawAuth.session_keys||['name','proto','ip','token']),'token']})}>Add identity key</button></details>}
  </fieldset>
  <div className="form-grid"><Field label="Comment"><input value={value.comment||''} onChange={e=>set('comment',e.target.value)}/></Field><Field label="Display position"><input type="number" value={value.position??''} onChange={e=>set('position',e.target.value===''?undefined:Number(e.target.value))}/></Field></div>
 </>:kind==='auth_backends'?<Field label="Authorization URL" help="Absolute HTTP or HTTPS URL for viewer checks."><input value={value.url||''} onChange={e=>set('url',e.target.value)}/></Field>:<fieldset><legend>Node endpoints</legend><div className="form-grid">
  <Field label="Management URL" help="Endpoint for authenticated discovery and telemetry."><input placeholder="http://node.example:18210" value={value.api_url||''} onChange={e=>set('api_url',e.target.value)}/></Field>
  <Field label="Public delivery URL" help="Viewer redirects use this address."><input placeholder="https://cdn.example" value={value.public_payload_url||''} onChange={e=>set('public_payload_url',e.target.value)}/></Field>
  <Field label="Private media URL" help="Use the LAN endpoint for source pulls. Defaults to management URL."><input placeholder="http://172.16.0.7:18210" value={value.private_payload_url||''} onChange={e=>set('private_payload_url',e.target.value)}/></Field>
  <Field label="Cluster key" help="Optional per-node key; otherwise uses this node's startup peer key."><input type="password" autoComplete="off" value={value.cluster_key||''} onChange={e=>set('cluster_key',e.target.value)}/></Field>
  {kind==='peers'&&<Field label="New viewer admission"><select value={value.drain?'drain':'accept'} onChange={e=>set('drain',e.target.value==='drain')}><option value="accept">Accept new viewers</option><option value="drain">Drain (stop new viewers)</option></select></Field>}
 </div></fieldset>}
 </>;
}
export function validateFields(kind:Kind,value:Item):string|undefined {
 const name=value.name||value.hostname;
 if(!name||name.split('/').some((s:string)=>!s||s==='.'||s==='..')||/[\\?#\x00-\x1f]/.test(name))return 'Enter a valid, unique name.';
 if(kind==='streams'||kind==='templates') {
  if(value.inputs?.some((i:Item)=>!i.url?.includes('://')))return 'Each input needs a complete source URL, including its protocol.';
  if(value.transcoder?.encoder!=='copy'&&value.transcoder?.vb!==undefined&&(!Number.isInteger(value.transcoder.vb)||value.transcoder.vb<100||value.transcoder.vb>50000))return 'Video bitrate must be between 100 and 50,000 kb/s.';
  if(value.flussonix_token_sha256&&!/^[a-f\d]{64}$/i.test(value.flussonix_token_sha256))return 'Viewer token SHA256 must contain exactly 64 hexadecimal characters.';
  const a=typeof value.on_play==='string'?{url:value.on_play}:value.on_play;
  if(a){if(a.url==='auth://'||!a.url)return 'Choose an authorization backend or enter a callback URL.';if(!a.url.startsWith('auth://')&&!httpURL(a.url))return 'Callback URL must use HTTP or HTTPS.';if(a.max_sessions!==undefined&&(!Number.isInteger(a.max_sessions)||a.max_sessions<1))return 'Maximum viewer sessions must be a positive integer.';if(a.session_keys&&(!a.session_keys.includes('name')||!a.session_keys.includes('proto')))return 'Session identity must include stream name and protocol.';}
 } else {
  for(const f of kind==='auth_backends'?['url']:['api_url','public_payload_url','private_payload_url']) {
   if((f==='api_url'||f==='url'||value[f])&&!httpURL(value[f]))return `${f==='url'?'Authorization':f==='api_url'?'Management':f==='public_payload_url'?'Public delivery':'Private media'} URL must use HTTP or HTTPS.`;
  }
  if(value.cluster_key&&(value.cluster_key.length<12||/[\r\n]/.test(value.cluster_key)))return 'Cluster key must contain at least 12 characters and no line breaks.';
 }
}
function httpURL(raw:string):boolean {try{const u=new URL(raw);return ['http:','https:'].includes(u.protocol)&&!!u.hostname}catch{return false}}
const labels:Record<string,string>={name:'Name',hostname:'Node',title:'Title',comment:'Comment',position:'Display position',template:'Template',static:'Activation',disabled:'Availability',inputs:'Inputs',transcoder:'Transcoding',on_play:'Authorization',flussonix_token_sha256:'Token guard',api_url:'Management URL',public_payload_url:'Public delivery URL',private_payload_url:'Private media URL',cluster_key:'Cluster key',drain:'Admission',status:'State',pid:'Worker process',bytes_in:'Media bytes',online_clients:'Live connections',uptime:'Uptime',input_protocol:'Upstream protocol',flussonix_transport:'Source transport'};
function valueText(key:string,v:any):React.ReactNode {
 if(key==='cluster_key'||key==='flussonix_token_sha256')return v?'Configured':'Not configured';
 if(key==='inputs')return v?.length?v.map((i:Item,n:number)=><div key={n}>{n+1}. {safeURL(i.url)}</div>):'No explicit inputs';
 if(key==='transcoder')return v?.encoder==='copy'?'Copy stream codecs':`${v?.encoder==='h264_nvenc'?'NVIDIA GPU':'CPU H.264'}${v?.vb?' · '+v.vb+' kb/s':''}`;
 if(key==='on_play'){const u=typeof v==='string'?v:v?.url;return <>{safeURL(u)}{v?.max_sessions&&<div>Limit: {v.max_sessions} sessions</div>}</>}
 if(key==='static')return v?'Always running':'On demand';if(key==='disabled')return v?'Disabled':'Enabled';if(key==='drain')return v?'Draining':'Accepting new viewers';
 if(key==='uptime')return v+'s';if(typeof v==='boolean')return v?'Enabled':'Disabled';
 return typeof v==='object'?'Configured':String(v??'—');
}
function safeURL(raw:string):string {if(!raw)return 'Not configured';try{const u=new URL(raw);if(u.username||u.password){u.username='hidden';u.password='hidden'}const keys:string[]=[];u.searchParams.forEach((_,k)=>keys.push(k));for(const k of keys)if(/token|key|password|secret/i.test(k))u.searchParams.set(k,'hidden');return u.toString()}catch{return raw}}
export function SettingsSummary({value,empty='No explicit overrides. Template and server defaults apply.'}:{value:Item,empty?:string}) {
 const entries=Object.entries(value).filter(([k])=>!['stats','config_on_disk','$reset','flussonix_peer_key'].includes(k));
 return entries.length?<dl className="settings-summary">{entries.map(([k,v])=><React.Fragment key={k}><dt>{labels[k]||k.replaceAll('_',' ')}</dt><dd>{valueText(k,v)}</dd></React.Fragment>)}</dl>:<p className="muted">{empty}</p>;
}
export function Capabilities({value}:{value:Item}) {
 return <div className="capability-grid"><section><h2>Inputs</h2><ul>{(value.input||[]).map((v:string)=><li key={v}>{v}</li>)}</ul></section><section><h2>Outputs</h2><ul>{(value.output||[]).map((v:string)=><li key={v}>{v}</li>)}</ul></section><section><h2>Not yet supported</h2><ul>{(value.unimplemented||[]).map((v:string)=><li key={v}>{v}</li>)}</ul></section><section><h2>Processing and cluster</h2><p>CPU: {value.transcoding?.cpu}</p><p>GPU: {value.transcoding?.gpu}</p><p>{value.cluster}</p></section></div>;
}
