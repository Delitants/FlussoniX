import {test,expect} from '@playwright/test';
test.beforeEach(async({page})=>{await page.goto('/admin/');await page.getByLabel('Username').fill(process.env.FLUSSONIX_ADMIN_USER||'admin');await page.getByLabel('Password',{exact:true}).fill(process.env.FLUSSONIX_ADMIN_PASSWORD!);await page.getByRole('button',{name:'Sign in',exact:true}).click();await expect(page.getByRole('heading',{name:'Streams',exact:true})).toBeVisible();});
test('create owned stream and show persisted operational state',async({page})=>{await page.getByRole('button',{name:'Add stream'}).click();await page.getByLabel('Stream name',{exact:true}).fill('ui-test');await page.getByLabel('Input URL',{exact:true}).fill('testsrc://');await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('heading',{name:'ui-test',exact:true})).toBeVisible();await page.getByRole('button',{name:'← Streams'}).click();await expect(page.getByRole('button',{name:'ui-test',exact:true})).toBeVisible();await page.screenshot({path:'../.runtime/screenshots/streams.png',fullPage:true});});
test('configuration edits can be validated without applying',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 await page.getByRole('button',{name:'Config',exact:true}).click();await page.getByRole('button',{name:'Add stream',exact:true}).click();
 await page.getByLabel('Stream name',{exact:true}).fill('staged-stream');await page.getByLabel('Input URL',{exact:true}).fill('testsrc://');
 await page.getByRole('button',{name:'Stage changes',exact:true}).click();await page.getByRole('button',{name:'Validate',exact:true}).click();
 await expect(page.getByRole('status')).toContainText('Saved state has not changed');
 expect((await request.get('/streamer/api/v3/streams/staged-stream',{headers})).status()).toBe(404);
 await page.getByRole('button',{name:'Save & apply',exact:true}).click();await expect(page.getByRole('status')).toContainText('saved and applied');
 expect((await request.get('/streamer/api/v3/streams/staged-stream',{headers})).ok()).toBeTruthy();
 await expect(page.locator('textarea')).toHaveCount(0);
});
test('cluster and template screens use configured data',async({page})=>{await page.getByRole('button',{name:'Templates',exact:true}).click();await expect(page.getByRole('heading',{name:'Templates',exact:true})).toBeVisible();await page.getByRole('button',{name:'Cluster',exact:true}).click();await expect(page.getByText('Viewer entry point',{exact:true})).toBeVisible();await page.getByRole('button',{name:'Source servers',exact:true}).click();await expect(page.getByText('No sources configured')).toBeVisible();await page.screenshot({path:'../.runtime/screenshots/cluster.png',fullPage:true});});
test('editing a title preserves template inheritance',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 const template='ui-inheritance',stream='ui-inherited';
 expect((await request.put('/streamer/api/v3/templates/'+template,{headers,data:{static:false,inputs:[{url:'testsrc://'}]}})).ok()).toBeTruthy();
 expect((await request.put('/streamer/api/v3/streams/'+stream,{headers,data:{$reset:true,template}})).ok()).toBeTruthy();
 await expect(page.getByRole('button',{name:stream,exact:true})).toBeVisible();
 await page.getByRole('button',{name:stream,exact:true}).click(); await page.getByRole('button',{name:'Edit stream',exact:true}).click();
 await page.getByLabel('Title',{exact:true}).fill('A title edit');
 await expect(page.getByLabel('Input URL',{exact:true})).toHaveCount(0);
 await page.getByRole('button',{name:'Save',exact:true}).click(); await expect(page.getByRole('heading',{name:'A title edit',exact:true})).toBeVisible();
 expect((await request.put('/streamer/api/v3/templates/'+template,{headers,data:{inputs:[{url:'hls://example.net/updated.m3u8'}]}})).ok()).toBeTruthy();
 const effective=await (await request.get('/streamer/api/v3/streams/'+stream,{headers})).json(); expect(effective.inputs[0].url).toBe('hls://example.net/updated.m3u8'); expect(effective.config_on_disk.inputs).toBeUndefined();
});
test('auth tab reauthorizes and closes an actual viewer session',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 const name='ui-session-'+Date.now();
 expect((await request.put('/streamer/api/v3/streams/'+name,{headers,data:{$reset:true,static:false,inputs:[{url:'testsrc://'}]}})).ok()).toBeTruthy();
 const media=await request.get('/'+name+'/index.m3u8?token=browser-viewer');expect(media.status()).toBe(200);
 await expect(page.getByRole('button',{name,exact:true})).toBeVisible();await page.getByRole('button',{name,exact:true}).click();await page.getByRole('button',{name:'Auth',exact:true}).click();
 await expect(page.getByRole('heading',{name:'Viewer sessions',exact:true})).toBeVisible();
 await expect(page.getByRole('cell',{name:'hls',exact:true})).toBeVisible();
 await page.getByRole('button',{name:'Reauthorize sessions',exact:true}).click();await expect(page.getByRole('status')).toContainText('Reauthorized 1 session');
 await page.getByRole('button',{name:'Close session',exact:true}).click();await expect(page.getByRole('status')).toContainText('Session closed');
 await expect(page.getByText('No active viewer sessions', {exact:true})).toBeVisible();
 expect((await request.get('/'+name+'/index.m3u8?token=browser-viewer')).status()).toBe(403);
});

test('template and backend editors use labeled fields without JSON',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 await page.getByRole('button',{name:'Templates',exact:true}).click();await page.getByRole('button',{name:'Add template',exact:true}).click();
 await page.getByLabel('Template name',{exact:true}).fill('friendly-defaults');await page.getByLabel('Input URL',{exact:true}).fill('testsrc://');
 await page.getByLabel('Activation',{exact:true}).selectOption('ondemand');await page.getByLabel('Transcoding',{exact:true}).selectOption('libx264');
 await page.getByLabel('Video bitrate (kb/s)',{exact:true}).fill('1200');await expect(page.locator('textarea')).toHaveCount(0);
 await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('cell',{name:'friendly-defaults',exact:true})).toBeVisible();
 const t=await (await request.get('/streamer/api/v3/templates/friendly-defaults',{headers})).json();expect(t.transcoder.vb).toBe(1200);expect(t.static).toBe(false);
 await page.getByRole('button',{name:'Cluster',exact:true}).click();await page.getByRole('button',{name:'Auth backends',exact:true}).click();
 await page.getByRole('button',{name:'Add auth backend',exact:true}).click();await page.getByLabel('Backend name',{exact:true}).fill('friendly-auth');
 await page.getByLabel('Authorization URL',{exact:true}).fill('http://127.0.0.1:19999/check');await page.getByRole('button',{name:'Save',exact:true}).click();
 await expect(page.getByRole('cell',{name:'friendly-auth',exact:true})).toBeVisible();
 expect((await request.get('/streamer/api/v3/auth_backends/friendly-auth',{headers})).ok()).toBeTruthy();
});
test('cluster source fields preserve endpoints and mask the peer key',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 await page.getByRole('button',{name:'Cluster',exact:true}).click();await page.getByRole('button',{name:'Source servers',exact:true}).click();
 await page.getByRole('button',{name:'Add source',exact:true}).click();await page.getByLabel('Node name',{exact:true}).fill('friendly-source');
 await page.getByLabel('Management URL',{exact:true}).fill('http://127.0.0.1:19998');await page.getByLabel('Private media URL',{exact:true}).fill('http://127.0.0.1:19998');
 await page.getByLabel('Source transport',{exact:true}).selectOption('m4f');await page.getByLabel('Cluster key',{exact:true}).fill('owned-test-peer-key');await expect(page.getByLabel('Cluster key',{exact:true})).toHaveAttribute('type','password');
 await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('cell',{name:'friendly-source',exact:true})).toBeVisible();
 const source=await (await request.get('/streamer/api/v3/cluster/sources/friendly-source',{headers})).json();expect(source.private_payload_url).toBe('http://127.0.0.1:19998');expect(source.flussonix_transport).toBe('m4f');
 await expect(page.locator('textarea')).toHaveCount(0);
});

test('changing authorization mode preserves the existing session limits and identity',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 const name='ui-auth-policy';
 expect((await request.put('/streamer/api/v3/auth_backends/ui-policy-backend',{headers,data:{url:'http://127.0.0.1:19999/check'}})).ok()).toBeTruthy();
 expect((await request.put('/streamer/api/v3/streams/'+name,{headers,data:{$reset:true,static:false,inputs:[{url:'testsrc://'}],on_play:{url:'auth://ui-policy-backend',max_sessions:3,session_keys:['name','proto','token','token']}}})).ok()).toBeTruthy();
 await expect(page.getByRole('button',{name,exact:true})).toBeVisible();await page.getByRole('button',{name,exact:true}).click();await page.getByRole('button',{name:'Edit stream',exact:true}).click();
 await page.getByLabel('Authorization policy',{exact:true}).selectOption('url');await page.getByLabel('Callback URL',{exact:true}).fill('http://127.0.0.1:19999/new-check');
 await expect(page.getByLabel('Maximum viewer sessions',{exact:true})).toHaveValue('3');
 await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('status')).toContainText('Saved.');
 const result=await (await request.get('/streamer/api/v3/streams/'+name,{headers})).json();expect(result.on_play.max_sessions).toBe(3);expect(result.on_play.session_keys).toEqual(['name','proto','token','token']);
});

test('media timeout forms preserve template inheritance and explicit overrides',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 const template='ui-recovery-template',name='ui-recovery-timeout';
 expect((await request.put('/streamer/api/v3/templates/'+template,{headers,data:{static:false,inputs:[{url:'testsrc://'}],flussonix_input_timeout:30}})).ok()).toBeTruthy();
 expect((await request.put('/streamer/api/v3/streams/'+name,{headers,data:{$reset:true,template}})).ok()).toBeTruthy();
 await expect(page.getByRole('button',{name,exact:true})).toBeVisible();await page.getByRole('button',{name,exact:true}).click();await page.getByRole('button',{name:'Edit stream',exact:true}).click();
 const field=page.getByLabel('Media stall timeout (seconds)',{exact:true});await expect(field).toHaveValue('');await field.fill('42');
 await page.getByRole('button',{name:'Save',exact:true}).click();
 let state=await(await request.get('/streamer/api/v3/streams/'+name,{headers})).json();expect(state.flussonix_input_timeout).toBe(42);expect(state.config_on_disk.inputs).toBeUndefined();
 await page.getByRole('button',{name:'Edit stream',exact:true}).click();await field.fill('');await page.getByRole('button',{name:'Save',exact:true}).click();
 expect((await request.put('/streamer/api/v3/templates/'+template,{headers,data:{flussonix_input_timeout:50}})).ok()).toBeTruthy();
 state=await(await request.get('/streamer/api/v3/streams/'+name,{headers})).json();expect(state.flussonix_input_timeout).toBe(50);expect(state.config_on_disk.flussonix_input_timeout).toBeUndefined();
});

test('HLS viewer resumes across an owned packaging-worker failure',async({page,request})=>{
 test.setTimeout(60000);
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 const name='ui-recovery-browser';
 expect((await request.put('/streamer/api/v3/streams/'+name,{headers,data:{$reset:true,static:false,inputs:[{url:'testsrc://'}]}})).ok()).toBeTruthy();
 await page.setContent('<video muted autoplay playsinline></video>');await page.addScriptTag({path:'node_modules/hls.js/dist/hls.min.js'});
 await page.evaluate(({name})=>{
  const w=window as any;w.recoveryPlayback={fragments:0,lastSequence:0,endSequence:0,fatal:[]};
  const hls=new w.Hls({maxBufferLength:6});const video=document.querySelector('video')!;
  hls.on(w.Hls.Events.FRAG_BUFFERED,(_:unknown,d:any)=>{w.recoveryPlayback.fragments++;w.recoveryPlayback.lastSequence=d.frag.sn});
  hls.on(w.Hls.Events.LEVEL_LOADED,(_:unknown,d:any)=>{w.recoveryPlayback.endSequence=d.details.endSN});
  hls.on(w.Hls.Events.ERROR,(_:unknown,d:any)=>{if(d.fatal)w.recoveryPlayback.fatal.push(d.details)});
  hls.attachMedia(video);hls.loadSource('/'+name+'/index.m3u8');video.play().catch(()=>{});
 },{name});
 await page.waitForFunction(()=>document.querySelector('video')!.currentTime>1);
 const before=await page.evaluate(()=>({...((window as any).recoveryPlayback),time:document.querySelector('video')!.currentTime}));
 const state=await(await request.get('/streamer/api/v3/streams/'+name,{headers})).json();expect(state.stats.status).toBe('running');
 const {execFileSync}=await import('node:child_process');execFileSync('kill',['-KILL',String(state.stats.pid)]);
 await expect.poll(async()=>{const s=await(await request.get('/streamer/api/v3/streams/'+name,{headers})).json();return s.stats.restart_count},{timeout:30000}).toBeGreaterThan(0);
 await page.waitForFunction((previous)=>{const w=window as any;const video=document.querySelector('video')!;return w.recoveryPlayback.fragments>previous.fragments+2&&w.recoveryPlayback.lastSequence>previous.endSequence&&video.currentTime>previous.time+6&&!video.paused},before,{timeout:30000});
 const result=await page.evaluate(()=>(window as any).recoveryPlayback);expect(result.fatal).toEqual([]);
 await request.delete('/streamer/api/v3/streams/'+name,{headers});
});
