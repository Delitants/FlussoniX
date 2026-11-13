import {Field,type Item} from './forms';
export const fxlinkInput=(url:string)=>url.startsWith('fxlink://');
export function FXLinkInputFields({row,onChange}:{row:Item,onChange:(row:Item)=>void}) {
 if(!fxlinkInput(row.url||''))return null;
 const settings=row.flussonix_fxlink||{};
 const set=(key:string,value:any)=>onChange({...row,flussonix_fxlink:{...settings,[key]:value}});
 return <>
  <Field label="FXLink viewer token" help="Optional token required by the source stream. Sent inside the encrypted connection."><input type="password" autoComplete="off" value={row.flussonix_fxlink_token||''} onChange={e=>{const next={...row};if(e.target.value)next.flussonix_fxlink_token=e.target.value;else delete next.flussonix_fxlink_token;onChange(next)}}/></Field>
  <Field label="Transport buffer (ms)" help="500–1,500 ms for reordering and loss recovery. Default: 1,000 ms."><input type="number" min={500} max={1500} step={100} value={settings.latency_ms??1000} onChange={e=>set('latency_ms',Number(e.target.value))}/></Field>
  <Field label="FEC protection" help="Parity adds bandwidth to recover missing packets. Very short records may have no parity in 20-shard profiles; the Short records preset protects them at greater bandwidth cost."><select value={`${settings.data_shards??20}:${settings.parity_shards??4}`} onChange={e=>{const [data_shards,parity_shards]=e.target.value.split(':').map(Number);onChange({...row,flussonix_fxlink:{...settings,data_shards,parity_shards}})}}>{[['1:1','Short records · 1 data + 1 repair (100%)'],['20:2','Low · 20 data + 2 repair (10%)'],['20:4','Standard · 20 data + 4 repair (20%)'],['20:6','High · 20 data + 6 repair (30%)']].map(([value,label])=><option key={value} value={value}>{label}</option>)}</select></Field>
 </>;
}
export function fxlinkError(row:Item):string|undefined {
 if(!fxlinkInput(row.url||''))return row.flussonix_fxlink||row.flussonix_fxlink_token?'FXLink controls require an fxlink:// input.':undefined;
 try {const u=new URL(row.url);if(!u.hostname||!u.port||u.port==='0'||u.username||u.password||u.search||u.hash||!u.pathname.slice(1))return 'FXLink needs a host, explicit UDP port and stream name, without URL credentials, query or fragment.';}catch{return 'Enter a valid FXLink source URL.';}
 const s=row.flussonix_fxlink||{};
 if(s.latency_ms!==undefined&&(!Number.isInteger(s.latency_ms)||s.latency_ms<500||s.latency_ms>1500))return 'FXLink transport buffer must be 500–1,500 ms.';
 if(row.flussonix_fxlink_token!==undefined&&(typeof row.flussonix_fxlink_token!=='string'||new TextEncoder().encode(row.flussonix_fxlink_token).length>4096))return 'FXLink token must be at most 4,096 bytes.';
}
export function FXLinkSourceFields({value,onChange}:{value:Item,onChange:(value:Item)=>void}) {
 if(value.flussonix_transport!=='fxlink')return null;
 const row={url:(value.flussonix_fxlink_url||'fxlink://node.example:55000')+'/stream',flussonix_fxlink:value.flussonix_fxlink,flussonix_fxlink_token:value.flussonix_fxlink_token};
 return <>
  <Field label="FXLink UDP endpoint" help="Native source listener with an explicit UDP port. Management discovery continues using the separate Management URL."><input placeholder="fxlink://origin.example:55000" value={value.flussonix_fxlink_url||''} onChange={e=>onChange({...value,flussonix_fxlink_url:e.target.value})}/></Field>
  <FXLinkInputFields row={row} onChange={row=>{const next={...value};for(const field of ['flussonix_fxlink','flussonix_fxlink_token']){if(row[field]!==undefined)next[field]=row[field];else delete next[field];}onChange(next)}}/>
 </>;
}
