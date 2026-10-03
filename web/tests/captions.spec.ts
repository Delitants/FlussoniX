import {test,expect} from '@playwright/test';
import {spawn} from 'node:child_process';
import {existsSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
for(const [mode,prefix] of [['convert',''],['convert','fmp4/'],['passthrough',''],['drop',''],['drop','fmp4/']])test(`${mode} subtitles during ${prefix?'fMP4':'TS'} HLS playback`,async({page,request,baseURL})=>{
 const fixture=process.env.FLUSSONIX_CAPTION_FIXTURE_FILE;
 expect(fixture&&existsSync(fixture),'Generate the owned caption fixture with the Rust test before browser qualification').toBeTruthy();
 const name='ui-caption-'+Date.now()+(prefix?'m':'t');
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 expect((await request.put('/streamer/api/v3/streams/'+name,{headers,data:{$reset:true,static:false,inputs:[{url:'publish://'}],flussonix_hls_subtitles:mode,...(mode==='convert'?{flussonix_hls_captions:[{channel:1,language:'en',name:'English'}]}:{})}})).ok()).toBeTruthy();
 const source=spawn('ffmpeg',['-nostdin','-v','error','-re','-i',fixture!,'-c','copy','-f','mpegts','-method','POST',baseURL+'/'+name+'/mpegts'],{stdio:['ignore','ignore','pipe']});let diagnostics='';source.stderr?.on('data',b=>{diagnostics=(diagnostics+b.toString()).slice(-8192)});
 const closed=new Promise<void>(resolve=>source.once('exit',()=>resolve()));
 try{
  await page.goto('/admin/');await page.setContent('<video muted autoplay controls></video>');await page.addScriptTag({path:fileURLToPath(new URL('../node_modules/hls.js/dist/hls.min.js',import.meta.url))});
  await page.evaluate(({url,convert})=>{
   const w=window as any;const video=document.querySelector('video')!;w.captionProbe={cues:[],fatals:[],frames:0};
   const hls=new w.Hls({startPosition:0,maxBufferLength:8,enableCEA708Captions:!convert});w.ownedHls=hls;
   hls.on(w.Hls.Events.ERROR,(_:any,d:any)=>{if(d.fatal)w.captionProbe.fatals.push(d.details)});
   hls.on(w.Hls.Events.SUBTITLE_TRACKS_UPDATED,()=>{hls.subtitleTrack=0;hls.subtitleDisplay=true;});hls.attachMedia(video);hls.loadSource(url);video.play().catch(()=>{});
   const seen=new Set<string>();w.probeTimer=setInterval(()=>{for(const track of Array.from(video.textTracks)){for(const cue of Array.from(track.cues||[]) as VTTCue[]){const key=cue.id+cue.startTime+cue.text;if(!seen.has(key)){seen.add(key);w.captionProbe.cues.push({id:cue.id,text:cue.text,start:cue.startTime,end:cue.endTime,label:track.label});}}}},50);
   const count=()=>{w.captionProbe.frames++;video.requestVideoFrameCallback(count)};video.requestVideoFrameCallback(count);
  },{url:baseURL+'/'+name+'/'+prefix+'index.m3u8',convert:mode==='convert'});
  if(mode!=='drop')await expect.poll(()=>page.evaluate(()=>(window as any).captionProbe.cues.some((c:any)=>c.text==='USA 608')),{timeout:18000}).toBeTruthy();
  await expect.poll(()=>page.evaluate(()=>document.querySelector('video')!.currentTime),{timeout:7000}).toBeGreaterThan(mode==='drop'?4:3);
  const result=await page.evaluate(()=>(window as any).captionProbe);expect(result.fatals,diagnostics).toEqual([]);expect(result.frames).toBeGreaterThan(10);if(mode==='drop'){expect(result.cues).toEqual([]);return;}const cue=result.cues.find((c:any)=>c.text==='USA 608');if(mode==='convert'){expect(cue.label).toBe('English');expect(cue.start).toBeCloseTo(1.181333,1);expect(cue.end-cue.start).toBeLessThanOrEqual(1.001);expect(cue.id).toMatch(/^cc1-[a-f0-9]{32}-[0-9]+-[0-9]+$/);}
 }finally{
  await page.evaluate(()=>{clearInterval((window as any).probeTimer);(window as any).ownedHls?.destroy()}).catch(()=>{});
  if(source.exitCode===null){source.kill('SIGTERM');await closed;}
  await request.delete('/streamer/api/v3/streams/'+name,{headers});
 }
});
