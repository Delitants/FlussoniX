import React from 'react';
import {Field,type Item} from './forms';

const owns=(v:Item,k:string)=>Object.prototype.hasOwnProperty.call(v,k);
const queryKeys=['streamid','passphrase','latency','connect_timeout','mode'];
function parsedURL(raw:string):URL {
 const i=raw.indexOf('?');if(i>=0)decodeURIComponent(raw.slice(i+1).replaceAll('+',' '));return new URL(i<0?raw:raw.slice(0,i)+'?'+raw.slice(i+1).replaceAll('#','%23'));
}
function queryEntries(u:URL):[string,string][] {const entries:[string,string][]=[];u.searchParams.forEach((v,k)=>entries.push([k,v]));return entries;}
function expandedPush(value:Item):Item {
 try {
  const u=parsedURL(value.url||'');const keys=queryEntries(u).map(([key])=>key);
  if(new Set(keys).size!==keys.length||keys.some(k=>!queryKeys.includes(k)||owns(value,k))||u.searchParams.has('mode')&&u.searchParams.get('mode')!=='caller')return {...value};
  const next={...value};for(const [key,v] of queryEntries(u))if(key!=='mode')next[key]=['latency','connect_timeout'].includes(key)?Number(v):v;
  u.search='';next.url=u.toString();return next;
 }catch{return {...value}}
}
function endpoint(raw:string):string {try {const u=parsedURL(raw);return 'srt://'+u.host;}catch{return 'Invalid destination'}}
export function pushSummary(pushes:Item[]):React.ReactNode {
 return pushes?.length?pushes.map((raw,i)=>{const p=expandedPush(raw);return <div key={i}>{i+1}. {endpoint(p.url)} · {p.disabled?'Disabled':'Enabled'} · {p.passphrase?'Encrypted':'Plaintext'}</div>}):'No destinations';
}
export function pushError(value:Item):string|undefined {
 if(!owns(value,'pushes'))return;
 if(!Array.isArray(value.pushes)||value.pushes.length>4)return 'Choose up to four SRT destinations.';
 for(const raw of value.pushes) {
  try {
   const u=parsedURL(raw.url||'');const keys=queryEntries(u).map(([key])=>key);
   if(u.protocol!=='srt:'||!u.hostname||!u.port||Number(u.port)===0||u.username||u.password||u.hash||!['','/'].includes(u.pathname)||/[\s\x00-\x1f\x7f]/.test(raw.url||''))return 'Destination URL must use srt://HOST:PORT in caller mode.';
   if(new Set(keys).size!==keys.length||keys.some(k=>!queryKeys.includes(k)||owns(raw,k))||u.searchParams.has('mode')&&u.searchParams.get('mode')!=='caller')return 'Use each supported SRT option once, in the URL or its field. Caller mode is required.';
  }catch{return 'Destination URL must use srt://HOST:PORT in caller mode.'}
  const p=expandedPush(raw);
  const secret=p.passphrase??'';
  if(typeof secret!=='string'||secret.length!==0&&(secret.length<10||secret.length>79)||/[^\x20-\x7e]/.test(secret))return 'SRT passphrase needs 10 to 79 ASCII characters, or leave it empty for plaintext.';
  const id=p.streamid??'';
  if(typeof id!=='string'||new TextEncoder().encode(id).length>512||/[\x00-\x1f\x7f-\x9f]/.test(id))return 'Stream ID must contain up to 512 bytes without control characters.';
  for(const [key,max] of [['latency',10000],['connect_timeout',30],['retry_timeout',300]] as const)if(owns(p,key)&&(!Number.isInteger(p[key])||p[key]<1||p[key]>max))return `SRT ${key==='latency'?'latency':key==='connect_timeout'?'connection timeout':'retry interval'} must be a whole number from 1 to ${max}.`;
 }
}

export function PushFields({value,inherited,onChange}:{value:Item,inherited?:Item[],onChange:(pushes:Item[]|undefined)=>void}) {
 const mode=owns(value,'pushes')?(value.pushes?.length?'override':'none'):'inherit';
 const rows:Item[]=(value.pushes||[]).map(expandedPush);
 const set=(i:number,key:string,v:any)=>onChange(rows.map((p,n)=>n===i?{...p,[key]:v}:p));
 return <fieldset><legend>SRT destinations</legend>
  <p className="muted">Caller mode sends the processed stream to each receiver. Enabled destinations keep a stream active without viewers. Editing destinations restarts the stream in this preview.</p>
  <Field label="Destination settings"><select value={mode} onChange={e=>onChange(e.target.value==='inherit'?undefined:e.target.value==='none'?[]:inherited?.length?inherited.map(expandedPush):[{url:''}])}><option value="inherit">{value.template?'Use template destinations':'No destinations (default)'}</option><option value="override">Override destinations</option><option value="none">No destinations</option></select></Field>
  {mode==='inherit'&&value.template&&<div className="muted">{pushSummary(inherited||[])}</div>}
  {mode==='override'&&<>{rows.map((p,i)=><div className="form-details" key={i}><h3>Destination {i+1}</h3><div className="form-grid">
   <Field label={`Destination URL ${i+1}`} help="Receiver address, including its SRT port."><input placeholder="srt://receiver.example:9000" value={p.url||''} onChange={e=>set(i,'url',e.target.value)}/></Field>
   <Field label={`Stream ID ${i+1}`} help="Optional identifier for the receiver, such as #!::r=channel,m=publish. Hidden because it may contain credentials."><input type="password" autoComplete="off" maxLength={512} value={p.streamid??''} onChange={e=>set(i,'streamid',e.target.value)}/></Field>
   <Field label={`Passphrase ${i+1}`} help="Matching 10–79 ASCII character secrets enable encryption. Empty means plaintext."><input type="password" autoComplete="off" maxLength={79} value={p.passphrase??''} onChange={e=>set(i,'passphrase',e.target.value)}/></Field>
   <Field label={`Latency (milliseconds) ${i+1}`} help="Delivery delay, 1–10000 milliseconds; default 120."><input type="number" min="1" max="10000" value={p.latency??120} onChange={e=>set(i,'latency',e.target.value===''?'':Number(e.target.value))}/></Field>
   <Field label={`Connection timeout (seconds) ${i+1}`}><input type="number" min="1" max="30" value={p.connect_timeout??3} onChange={e=>set(i,'connect_timeout',e.target.value===''?'':Number(e.target.value))}/></Field>
   <Field label={`Retry interval (seconds) ${i+1}`}><input type="number" min="1" max="300" value={p.retry_timeout??5} onChange={e=>set(i,'retry_timeout',e.target.value===''?'':Number(e.target.value))}/></Field>
   <Field label={`Destination enabled ${i+1}`}><input type="checkbox" checked={!p.disabled} onChange={e=>set(i,'disabled',!e.target.checked)}/></Field>
  </div><button type="button" onClick={()=>onChange(rows.filter((_,n)=>n!==i))}>Remove destination {i+1}</button></div>)}<button type="button" disabled={rows.length>=4} onClick={()=>onChange([...rows,{url:''}])}>Add SRT destination</button></>}
 </fieldset>;
}
