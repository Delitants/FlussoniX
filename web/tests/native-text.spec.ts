import {test,expect} from '@playwright/test';
import {createServer} from 'node:http';
import {existsSync,readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
for(const audio of [false,true])for(const prefix of ['', 'fmp4/'])test(`native ${audio?'audio-only ':''}text during ${prefix?'fMP4':'TS'} HLS playback`,async({page,request,baseURL})=>{
 const fixture=audio?process.env.FLUSSONIX_NATIVE_AUDIO_FIXTURE_FILE:process.env.FLUSSONIX_NATIVE_FIXTURE_FILE;
 expect(fixture&&existsSync(fixture),'Generate the owned native fixture with the Rust tests').toBeTruthy();
 const source=createServer((req,res)=>{if(req.url!=='/owned/m4s'){res.writeHead(404);res.end();return;}res.writeHead(200,{'Content-Type':'application/octet-stream'});res.write(readFileSync(fixture!));});
 await new Promise<void>(resolve=>source.listen(0,'127.0.0.1',resolve));
 const address=source.address() as {port:number};const name='ui-native-'+(audio?'a':'v')+Date.now()+(prefix?'m':'t');
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 try{
  expect((await request.put('/streamer/api/v3/streams/'+name,{headers,data:{$reset:true,static:false,inputs:[{url:`m4s://127.0.0.1:${address.port}/owned`}],...(audio?{}:{transcoder:{encoder:'libx264',vb:300}}),flussonix_subtitle_tracks:'drop',flussonix_hls_subtitles:'convert',flussonix_hls_captions:[{native_track:7,language:'en',name:'Native English'},{native_track:4294967295,language:'de',name:'Native German'}]}})).ok()).toBeTruthy();
  await page.goto('/admin/');await page.setContent('<video muted autoplay controls></video>');await page.addScriptTag({path:fileURLToPath(new URL('../node_modules/hls.js/dist/hls.min.js',import.meta.url))});
  await page.evaluate(({url,selected})=>{
   const w=window as any;const video=document.querySelector('video')!;w.nativeProbe={cues:[],fatals:[],frames:0};const hls=new w.Hls({startPosition:0,enableCEA708Captions:false});w.ownedHls=hls;
   hls.on(w.Hls.Events.ERROR,(_:any,d:any)=>{if(d.fatal)w.nativeProbe.fatals.push(d.details)});hls.on(w.Hls.Events.SUBTITLE_TRACKS_UPDATED,()=>queueMicrotask(()=>{hls.subtitleTrack=selected;hls.subtitleDisplay=true;}));hls.attachMedia(video);hls.loadSource(url);video.play().catch(()=>{});
   w.probeTimer=setInterval(()=>{w.nativeProbe.cues=Array.from(video.textTracks).flatMap(track=>Array.from(track.cues||[]).map(cue=>({text:(cue as VTTCue).text,id:cue.id,start:cue.startTime,end:cue.endTime,label:track.label})))},50);
   const count=()=>{w.nativeProbe.frames++;video.requestVideoFrameCallback(count)};video.requestVideoFrameCallback(count);
  },{url:baseURL+'/'+name+'/'+prefix+'index.m3u8',selected:prefix?1:0});
  if(audio)await expect.poll(()=>page.evaluate(()=>document.querySelector('video')!.currentTime),{timeout:18000}).toBeGreaterThan(0);else await expect.poll(()=>page.evaluate(()=>(window as any).nativeProbe.frames),{timeout:18000}).toBeGreaterThan(0);
  await expect.poll(()=>page.evaluate(()=>(window as any).nativeProbe.cues.length),{timeout:7000}).toBeGreaterThan(0);
  const probe=await page.evaluate(()=>(window as any).nativeProbe);expect(probe.fatals).toEqual([]);
  const cue=probe.cues.find((c:any)=>prefix?c.text==='EUROPE GRÜSSE':c.text.includes('AMERICA &lt;HELLO&gt;'));
  expect(cue,JSON.stringify(probe)).toBeTruthy();expect(cue.label).toBe(prefix?'Native German':'Native English');expect(cue.start).toBeCloseTo(audio?2:1.92,1);expect(cue.end-cue.start).toBeLessThanOrEqual(1.001);expect(cue.id).toMatch(new RegExp('^'+(prefix?'nt4294967295':'nt7')+'-[a-f0-9]{32}-'));
  await expect.poll(()=>page.evaluate(()=>document.querySelector('video')!.currentTime),{timeout:7000}).toBeGreaterThan(4);
  expect(await page.evaluate(()=>Array.from(document.querySelector('video')!.textTracks).flatMap(t=>Array.from(t.activeCues||[])).length)).toBe(0);
 }finally{
  await page.evaluate(()=>{clearInterval((window as any).probeTimer);(window as any).ownedHls?.destroy()}).catch(()=>{});await request.delete('/streamer/api/v3/streams/'+name,{headers}).catch(()=>{});source.closeAllConnections();await new Promise<void>(resolve=>source.close(()=>resolve()));
 }
});
