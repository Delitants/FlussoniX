import {test,expect} from '@playwright/test';
import {spawn} from 'node:child_process';
import {existsSync,writeFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
for(const [mode,prefix,digital,teletext] of [['convert','',false,false],['convert','fmp4/',false,false],['passthrough','',false,false],['drop','',false,false],['drop','fmp4/',false,false],['convert','',true,false],['convert','fmp4/',true,false],['convert','',false,true],['convert','fmp4/',false,true]] as const)test(`${teletext?'teletext ':digital?'digital ':''}${mode} subtitles during ${prefix?'fMP4':'TS'} HLS playback`,async({page,request,baseURL},testInfo)=>{
 const fixture=teletext?process.env.FLUSSONIX_TELETEXT_FIXTURE_FILE:digital?process.env.FLUSSONIX_708_FIXTURE_FILE:process.env.FLUSSONIX_CAPTION_FIXTURE_FILE;
 const wanted=teletext?(prefix?'français':'GRÜSSE'):digital?(prefix?'ESPAÑOL':'USA708'):'USA 608';
 expect(fixture&&existsSync(fixture),'Generate the owned caption fixture with the Rust test before browser qualification').toBeTruthy();
 const name='ui-caption-'+(teletext?'teletext-':digital?'digital-':'')+Date.now()+(prefix?'m':'t');
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 expect((await request.put('/streamer/api/v3/streams/'+name,{headers,data:{$reset:true,static:false,inputs:[{url:'publish://'}],flussonix_hls_subtitles:mode,...(mode==='convert'?{flussonix_hls_captions:teletext?[{teletext_page:888,language:'de',name:'German'},{teletext_page:889,language:'fr',name:'French'}]:digital?[{service:1,language:'en',name:'English'},{service:2,language:'es',name:'Spanish'}]:[{channel:1,language:'en',name:'English'}]}:{})}})).ok()).toBeTruthy();
 const source=spawn('ffmpeg',['-nostdin','-v','error','-re','-i',fixture!,'-c','copy',...(teletext?['-map','0:v?','-map','0:a?','-map','0:s?','-muxdelay','0','-muxpreload','0','-pcr_period','20','-max_interleave_delta','100000','-flush_packets','1']:[]),'-f','mpegts','-method','POST',baseURL+'/'+name+'/mpegts'],{stdio:['ignore','ignore','pipe']});let diagnostics='';source.stderr?.on('data',b=>{diagnostics=(diagnostics+b.toString()).slice(-8192)});
 const closed=new Promise<void>(resolve=>source.once('exit',()=>resolve()));
 try{
  await page.goto('/admin/');await page.setContent('<video muted autoplay controls></video>');await page.addScriptTag({path:fileURLToPath(new URL('../node_modules/hls.js/dist/hls.min.js',import.meta.url))});
  await page.evaluate(({url,convert,selected})=>{
   const w=window as any;const video=document.querySelector('video')!;w.captionProbe={cues:[],fatals:[],frames:0};
   const hls=new w.Hls({startPosition:0,maxBufferLength:8,enableCEA708Captions:!convert});w.ownedHls=hls;
   hls.on(w.Hls.Events.ERROR,(_:any,d:any)=>{if(d.fatal)w.captionProbe.fatals.push(d.details)});
   hls.on(w.Hls.Events.SUBTITLE_TRACKS_UPDATED,()=>{queueMicrotask(()=>{hls.subtitleTrack=selected;hls.subtitleDisplay=true;});});hls.attachMedia(video);hls.loadSource(url);video.play().catch(()=>{});
   const seen=new Set<string>();w.probeTimer=setInterval(()=>{for(const track of Array.from(video.textTracks)){for(const cue of Array.from(track.cues||[]) as VTTCue[]){const key=cue.id+cue.startTime+cue.text;if(!seen.has(key)){seen.add(key);w.captionProbe.cues.push({id:cue.id,text:cue.text,start:cue.startTime,end:cue.endTime,label:track.label});}}}},50);
   const count=()=>{w.captionProbe.frames++;video.requestVideoFrameCallback(count)};video.requestVideoFrameCallback(count);
  },{url:baseURL+'/'+name+'/'+prefix+'index.m3u8',convert:mode==='convert',selected:(digital||teletext)&&prefix?1:0});
  if(mode!=='drop')await expect.poll(()=>page.evaluate(wanted=>(window as any).captionProbe.cues.some((c:any)=>c.text===wanted),wanted),{timeout:18000}).toBeTruthy();
  await expect.poll(()=>page.evaluate(()=>document.querySelector('video')!.currentTime),{timeout:7000}).toBeGreaterThan(mode==='drop'?4:3);
  const result=await page.evaluate(()=>(window as any).captionProbe);expect(result.fatals,diagnostics).toEqual([]);expect(result.frames).toBeGreaterThan(10);if(mode==='drop'){expect(result.cues).toEqual([]);return;}const cue=result.cues.find((c:any)=>c.text===wanted);if(mode==='convert'){expect(cue.label).toBe(teletext?(prefix?'French':'German'):digital&&prefix?'Spanish':'English');expect(cue.start).toBeCloseTo(1.181333,1);expect(cue.end-cue.start).toBeLessThanOrEqual(1.001);expect(cue.id).toMatch(new RegExp('^'+(teletext?(prefix?'ttx889':'ttx888'):digital?(prefix?'s2':'s1'):'cc1')+'-[a-f0-9]{32}-[0-9]+-[0-9]+$'));}
 }catch(error){
  const body=JSON.stringify({publisher:diagnostics,probe:await page.evaluate(()=>(window as any).captionProbe).catch(()=>null),stream:await(await request.get('/streamer/api/v3/streams/'+name,{headers})).json().catch(()=>null)});const path=testInfo.outputPath('caption-diagnostics.json');writeFileSync(path,body);await testInfo.attach('caption-diagnostics',{path,contentType:'application/json'});throw error;
 }finally{
  await page.evaluate(()=>{clearInterval((window as any).probeTimer);(window as any).ownedHls?.destroy()}).catch(()=>{});
  if(source.exitCode===null){source.kill('SIGTERM');await closed;}
  await request.delete('/streamer/api/v3/streams/'+name,{headers});
 }
});
