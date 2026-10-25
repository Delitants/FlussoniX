import React,{useEffect,useState} from 'react';

type Request = (url:string,method?:string,body?:unknown)=>Promise<any>;
type Package = {name:string;channels:string[]};
type Subscriber = {name:string;token:string;packages:string[];max_sessions?:number|null;flussonix_expires_at?:string|null};
type Editor = {kind:'package'|'subscriber';original:string;name:string;selected:string[];limit:string;expires:string;token:string;rotate:boolean};
type Cluster = {mode?:string;ready?:boolean;reason?:string};
const api='/streamer/api/v3/iptv';
const validName=(name:string)=>name.length>0&&new TextEncoder().encode(name).length<=256&&!name.split('/').some(part=>!part||part==='.'||part==='..')&&!/[\\?#\x00-\x1f\x7f]/.test(name);
const itemPath=(name:string)=>encodeURIComponent(name);
const utcField=(value?:string|null)=>value?.slice(0,16)||'';

export function IptvPage({request,publicOrigin,managementRole,cluster,onBack}:{request:Request;publicOrigin?:string|null;managementRole?:string;cluster?:Cluster;onBack:()=>void}){
 const remote=cluster?.mode==='client'||cluster?.mode==='missing_authority';
 const canEdit=managementRole==='edit'&&!remote;
 const[enabled,setEnabled]=useState<boolean|null>(null);
 const[packages,setPackages]=useState<Package[]>([]),[subscribers,setSubscribers]=useState<Subscriber[]>([]),[channels,setChannels]=useState<string[]>([]);
 const[packageNext,setPackageNext]=useState<string|null>(null),[subscriberNext,setSubscriberNext]=useState<string|null>(null);
 const[tab,setTab]=useState<'packages'|'subscribers'>('packages');
 const[editor,setEditor]=useState<Editor|null>(null),[shownToken,setShownToken]=useState<string|null>(null);
 const[error,setError]=useState(''),[formError,setFormError]=useState(''),[notice,setNotice]=useState(''),[busy,setBusy]=useState(false);

 async function refresh(){
  try{
   const status=await request(api).then(()=>true).catch((cause:Error)=>{
    if(cause.message.includes('IPTV is disabled'))return false;
    throw cause;
   });
   setEnabled(status);
   const streams=await request('/streamer/api/v3/streams?limit=1000');
   setChannels((streams.streams||[]).map((row:{name:string})=>row.name).sort());
   if(status){
    const [packPage,subPage]=await Promise.all([request(api+'/packages?limit=1000'),request(api+'/subscribers?limit=1000')]);
    setPackages(packPage.packages||[]);setPackageNext(packPage.next||null);
    setSubscribers(subPage.subscribers||[]);setSubscriberNext(subPage.next||null);
   }else{setPackages([]);setSubscribers([]);setPackageNext(null);setSubscriberNext(null);}
   setError('');
  }catch(cause){setError((cause as Error).message);}
 }
 useEffect(()=>{if(!remote)void refresh()},[remote]);
 async function more(kind:'packages'|'subscribers',cursor:string){
  try{const page=await request(`${api}/${kind}?limit=1000&cursor=${encodeURIComponent(cursor)}`);
   if(kind==='packages'){setPackages(previous=>[...previous,...page.packages]);setPackageNext(page.next||null)}
   else{setSubscribers(previous=>[...previous,...page.subscribers]);setSubscriberNext(page.next||null)}
  }catch(cause){setError((cause as Error).message)}
 }
 async function toggleModule(){
  setBusy(true);setError('');try{
   await request(api,enabled?'DELETE':'PUT',enabled?undefined:{});
   await refresh();setEditor(null);setShownToken(null);
   setNotice(enabled?'IPTV disabled. Existing subscriber viewers have been revoked.':'IPTV enabled. Packages and subscribers are ready to configure.');
  }catch(cause){setError((cause as Error).message)}finally{setBusy(false)}
 }
 function open(kind:'package'|'subscriber',row?:Package|Subscriber){
  setFormError('');setNotice('');setShownToken(null);
  setEditor({kind,original:row?.name||'',name:row?.name||'',selected:row?(kind==='package'?(row as Package).channels:(row as Subscriber).packages):[],limit:kind==='subscriber'&&row&&'max_sessions'in row&&row.max_sessions?String(row.max_sessions):'',expires:kind==='subscriber'&&row&&'flussonix_expires_at'in row?utcField(row.flussonix_expires_at):'',token:'',rotate:false});
 }
 async function save(){
  if(!editor)return;
  if(!validName(editor.name)){setFormError('Enter a valid name (up to 256 bytes, with no empty path parts).');return}
  if(!editor.original&&((editor.kind==='package'?packages:subscribers).some(row=>row.name===editor.name))){setFormError('That name already exists. Edit the existing entry.');return}
  if(editor.kind==='subscriber'&&editor.limit&&(!/^[0-9]+$/.test(editor.limit)||Number(editor.limit)<1||!Number.isSafeInteger(Number(editor.limit)))){setFormError('Maximum sessions must be a positive whole number.');return}
  if(editor.kind==='subscriber'&&editor.expires&&!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(editor.expires)){setFormError('Enter a UTC expiration date and time.');return}
  if(editor.kind==='subscriber'&&editor.expires&&Number.isNaN(Date.parse(editor.expires+':00Z'))){setFormError('Enter a valid UTC expiration date and time.');return}
  if(editor.kind==='subscriber'&&editor.token&&new TextEncoder().encode(editor.token).length>4096){setFormError('Subscriber token is too long.');return}
  const body:Record<string,unknown>=editor.kind==='package'?{channels:editor.selected}:{packages:editor.selected,max_sessions:editor.limit?Number(editor.limit):null,flussonix_expires_at:editor.expires?editor.expires+':00Z':null};
  if(editor.kind==='subscriber'){
   if(editor.token)body.token=editor.token;
   else if(editor.rotate)body.token=null;
  }
  setBusy(true);setFormError('');try{
   await request(`${api}/${editor.kind==='package'?'packages':'subscribers'}/${itemPath(editor.name)}`,'PUT',body);
   setEditor(null);setShownToken(null);await refresh();
   setNotice(editor.kind==='package'?'Package saved. Subscriber access updates immediately.':editor.original?'Subscriber saved. Changes to access close old viewers.':'Subscriber saved. A token was generated if none was entered; reveal or copy it deliberately.');
  }catch(cause){setFormError((cause as Error).message)}finally{setBusy(false)}
 }
 async function remove(kind:'package'|'subscriber',name:string){
  if(!window.confirm(`Delete ${name}? Subscriber access and active viewers may change immediately.`))return;
  setBusy(true);setError('');try{await request(`${api}/${kind==='package'?'packages':'subscribers'}/${itemPath(name)}`,'DELETE');await refresh();setShownToken(null);setNotice(`${kind==='package'?'Package':'Subscriber'} deleted. Affected viewers have been revoked.`)}
  catch(cause){setError((cause as Error).message)}finally{setBusy(false)}
 }
 async function copy(value:string,what:string){
  try{await navigator.clipboard.writeText(value);setNotice(`${what} copied to clipboard.`)}
  catch{setError(`Could not copy ${what.toLowerCase()}. Use Reveal token instead.`)}
 }
 const choices=editor?.kind==='package'?[...new Set([...channels,...editor.selected])].sort():[...new Set([...packages.map(row=>row.name),...(editor?.selected||[])])].sort();
 const playlist=(subscriber:Subscriber)=>publicOrigin&&canEdit&&subscriber.token!=='[redacted]'?new URL(`/tv/playlists/${itemPath(subscriber.name)}?token=${encodeURIComponent(subscriber.token)}`,publicOrigin).href:null;
 return <section className="iptv-page">
  <div className="page-heading"><div><div className="breadcrumb">System / Config / IPTV</div><h1>IPTV</h1></div><div className="heading-actions"><button onClick={onBack}>← Configuration</button>{canEdit&&enabled!==null&&<button className={enabled?'danger':'primary'} disabled={busy} onClick={toggleModule}>{enabled?'Disable IPTV':'Enable IPTV'}</button>}</div></div>
  {error&&<div className="error" role="alert">{error}</div>}{notice&&<div className="notice" role="status">{notice}</div>}
  {cluster&&cluster.ready===false&&<div className="error" role="alert">{cluster.reason||'IPTV cluster admission is unavailable.'}</div>}
  <div className="panel padded iptv-intro"><h2>Subscriber access</h2><p>{remote?'Subscriber catalog and playlists are managed by the IPTV authority.':enabled===null?'Loading IPTV status…':enabled?'IPTV is enabled. Assign channels to packages, then packages to subscribers.':'IPTV is disabled. Enable it to manage packages and subscribers.'}</p><p className="muted">{cluster?.mode==='authority'?'Subscriber data is saved in an owner-only file on this authority. Session limits count FlussoniX nodes only.':remote?'This node does not use a local subscriber file for viewer access.':'Subscriber data and tokens are saved in a separate owner-only file. Limits apply separately on each node.'}</p>{!remote&&!publicOrigin&&<p className="muted">Set the IPTV public origin at startup to offer subscriber playlist links.</p>}{!remote&&publicOrigin?.startsWith('http://')&&<p className="iptv-http-warning" role="alert">This HTTP origin sends subscriber tokens without encryption. Use HTTPS for public or untrusted networks.</p>}</div>
  {enabled&&<><div className="tabs"><button className={tab==='packages'?'selected':''} onClick={()=>{setShownToken(null);setTab('packages')}}>Packages</button><button className={tab==='subscribers'?'selected':''} onClick={()=>{setShownToken(null);setTab('subscribers')}}>Subscribers</button></div>
   {tab==='packages'?<section className="panel"><div className="panel-heading"><h2>Packages</h2>{canEdit&&<button className="primary" onClick={()=>open('package')}>Add package</button>}</div><div className="table-wrap"><table><thead><tr><th>Name</th><th>Channels</th>{canEdit&&<th>Actions</th>}</tr></thead><tbody>{packages.map(row=><tr key={row.name}><td className="mono">{row.name}</td><td>{row.channels.length?row.channels.join(', '):'No channels'}</td>{canEdit&&<td className="actions"><button onClick={()=>open('package',row)}>Edit</button><button className="danger" onClick={()=>remove('package',row.name)}>Delete</button></td>}</tr>)}</tbody></table>{!packages.length&&<p className="padded muted">No packages yet.</p>}</div>{packageNext&&<div className="editor-footer"><button onClick={()=>more('packages',packageNext)}>Load more packages</button></div>}</section>:
   <section className="panel"><div className="panel-heading"><h2>Subscribers</h2>{canEdit&&<button className="primary" onClick={()=>open('subscriber')}>Add subscriber</button>}</div><div className="table-wrap"><table><thead><tr><th>Name</th><th>Packages</th><th>Session limit</th><th>Expires (UTC)</th><th>Token</th><th>Actions</th></tr></thead><tbody>{subscribers.map(row=>{const secret=canEdit&&row.token!=='[redacted]';const url=playlist(row);return <tr key={row.name}><td className="mono">{row.name}</td><td>{row.packages.join(', ')||'None'}</td><td>{row.max_sessions??'No limit'}</td><td>{row.flussonix_expires_at||'Never'}</td><td className="mono iptv-token">{secret?(shownToken===row.name?row.token:'••••'):'[redacted]'}</td><td className="actions">{secret&&<><button onClick={()=>setShownToken(shownToken===row.name?null:row.name)}>{shownToken===row.name?'Hide token':'Reveal token'}</button><button onClick={()=>copy(row.token,'Token')}>Copy token</button></>}{url&&<button onClick={()=>copy(url,'Playlist link')}>Copy playlist link</button>}{canEdit&&<><button onClick={()=>open('subscriber',row)}>Edit</button><button className="danger" onClick={()=>remove('subscriber',row.name)}>Delete</button></>}</td></tr>})}</tbody></table>{!subscribers.length&&<p className="padded muted">No subscribers yet.</p>}</div>{subscriberNext&&<div className="editor-footer"><button onClick={()=>more('subscribers',subscriberNext)}>Load more subscribers</button></div>}</section>}
  </>}
  {editor&&<div className="modal-backdrop"><section className="modal" role="dialog" aria-modal="true" aria-labelledby="iptv-editor-title"><div className="panel-heading"><h2 id="iptv-editor-title">{editor.original?'Edit':'Add'} {editor.kind}</h2><button aria-label="Close editor" onClick={()=>setEditor(null)}>×</button></div><div className="padded iptv-editor"><label>{editor.kind==='package'?'Package name':'Subscriber name'}<input value={editor.name} disabled={!!editor.original} maxLength={256} onChange={event=>setEditor({...editor,name:event.target.value})}/></label><fieldset><legend>{editor.kind==='package'?'Channels':'Packages'}</legend><div className="iptv-choices">{choices.map(name=><label key={name}><input type="checkbox" checked={editor.selected.includes(name)} onChange={event=>setEditor({...editor,selected:event.target.checked?[...editor.selected,name]:editor.selected.filter(value=>value!==name)})}/>{name}</label>)}</div>{!choices.length&&<p className="muted">{editor.kind==='package'?'Create a stream first to choose channels.':'Create a package first.'}</p>}</fieldset>{editor.kind==='subscriber'&&<><div className="form-grid"><label>{cluster?.mode==='authority'?'Maximum sessions across FlussoniX nodes':'Maximum sessions on this node'}<input type="number" min="1" step="1" value={editor.limit} onChange={event=>setEditor({...editor,limit:event.target.value})}/></label><label>Expires at (UTC)<input type="datetime-local" value={editor.expires} onChange={event=>setEditor({...editor,expires:event.target.value})}/></label></div><label>Subscriber token<input type="password" autoComplete="off" value={editor.token} placeholder={editor.original?'Leave blank to keep current token':'Leave blank to generate a token'} onChange={event=>setEditor({...editor,token:event.target.value,rotate:false})}/></label>{editor.original&&<button type="button" onClick={()=>setEditor({...editor,token:'',rotate:true})}>Generate new token on save</button>}<p className="field-help">{editor.rotate?'Saving will replace the current token and revoke its viewers.':'Tokens stay masked in the list until you reveal or copy them.'}</p></>}{formError&&<div className="error" role="alert">{formError}</div>}</div><div className="modal-actions"><button onClick={()=>setEditor(null)}>Cancel</button><button className="primary" disabled={busy} onClick={save}>{busy?'Saving…':`Save ${editor.kind}`}</button></div></section></div>}
 </section>;
}
