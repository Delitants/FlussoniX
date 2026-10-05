import {test,expect} from '@playwright/test';
test.beforeEach(async({page})=>{await page.goto('/admin/');await page.getByLabel('Username').fill(process.env.FLUSSONIX_ADMIN_USER||'admin');await page.getByLabel('Password',{exact:true}).fill(process.env.FLUSSONIX_ADMIN_PASSWORD!);await page.getByRole('button',{name:'Sign in',exact:true}).click();await expect(page.getByRole('heading',{name:'Streams',exact:true})).toBeVisible();});
test('Config shows actual HTTP and HTTPS listeners with startup guidance',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 const node=await(await request.get('/flussonix/api/v1/node',{headers})).json();
 await page.getByRole('button',{name:'Config',exact:true}).click();
 const delivery=page.getByRole('region',{name:'HTTP delivery'});
 await expect(delivery).toBeVisible();
 await expect(delivery).toContainText(node.http_delivery?.http||'Not enabled');
 await expect(delivery).toContainText(node.http_delivery?.https||'Not enabled');
 await expect(delivery).toContainText('startup options');
 await expect(delivery.locator('input,textarea')).toHaveCount(0);
});
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

test('cluster MPEG-TS source transport is editable without JSON',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 const name='ui-ts-source';
 expect((await request.put(`/streamer/api/v3/cluster/sources/${name}`,{headers,data:{api_url:'https://source.example/control',private_payload_url:'https://source.example/lan',cluster_key:'owned-ts-source-key'}})).ok()).toBeTruthy();
 await page.getByRole('button',{name:'Cluster',exact:true}).click();await page.getByRole('button',{name:'Source servers',exact:true}).click();
 await page.getByRole('row').filter({has:page.getByRole('cell',{name,exact:true})}).getByRole('button',{name:'Edit',exact:true}).click();
 await page.getByLabel('Source transport',{exact:true}).selectOption('mpegts');
 await expect(page.locator('textarea')).toHaveCount(0);
 await page.getByRole('button',{name:'Save',exact:true}).click();
 const saved=await(await request.get(`/streamer/api/v3/cluster/sources/${name}`,{headers})).json();
 expect(saved.flussonix_transport).toBe('mpegts');expect(saved.private_payload_url).toBe('https://source.example/lan');expect(saved.cluster_key).toBe('owned-ts-source-key');
 await page.getByRole('row').filter({has:page.getByRole('cell',{name,exact:true})}).getByRole('button',{name:'Edit',exact:true}).click();
 await expect(page.getByLabel('Source transport',{exact:true})).toHaveValue('mpegts');
 await page.getByRole('button',{name:'Cancel',exact:true}).click();
 await request.delete(`/streamer/api/v3/cluster/sources/${name}`,{headers});
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
  const w=window as any;w.recoveryPlayback={fragments:0,lastSequence:0,endSequence:0,lastGeneration:null,initialGeneration:null,oldBufferEnd:0,fatal:[]};
  const hls=new w.Hls({maxBufferLength:6});const video=document.querySelector('video')!;
  hls.on(w.Hls.Events.FRAG_BUFFERED,(_:unknown,d:any)=>{w.recoveryPlayback.fragments++;w.recoveryPlayback.lastSequence=d.frag.sn;const generation=d.frag.url.match(/\/(g[0-9a-f]{32})_/)?.[1];w.recoveryPlayback.initialGeneration??=generation;w.recoveryPlayback.lastGeneration=generation;if(generation===w.recoveryPlayback.initialGeneration)w.recoveryPlayback.oldBufferEnd=Math.max(w.recoveryPlayback.oldBufferEnd,d.frag.start+d.frag.duration)});
  hls.on(w.Hls.Events.LEVEL_LOADED,(_:unknown,d:any)=>{w.recoveryPlayback.endSequence=d.details.endSN});
  hls.on(w.Hls.Events.ERROR,(_:unknown,d:any)=>{if(d.fatal)w.recoveryPlayback.fatal.push(d.details)});
  hls.attachMedia(video);hls.loadSource('/'+name+'/index.m3u8');video.play().catch(()=>{});
 },{name});
 await page.waitForFunction(()=>document.querySelector('video')!.currentTime>1);
 const before=await page.evaluate(()=>({...((window as any).recoveryPlayback),time:document.querySelector('video')!.currentTime,bufferEnd:document.querySelector('video')!.buffered.end(document.querySelector('video')!.buffered.length-1)}));
 const state=await(await request.get('/streamer/api/v3/streams/'+name,{headers})).json();expect(state.stats.status).toBe('running');
 const {execFileSync}=await import('node:child_process');execFileSync('kill',['-KILL',String(state.stats.pid)]);
 await expect.poll(async()=>{const s=await(await request.get('/streamer/api/v3/streams/'+name,{headers})).json();return s.stats.restart_count},{timeout:30000}).toBeGreaterThan(0);
 await page.waitForFunction((previous)=>{const w=window as any;const video=document.querySelector('video')!;return w.recoveryPlayback.fragments>previous.fragments+2&&w.recoveryPlayback.lastSequence>previous.endSequence&&w.recoveryPlayback.lastGeneration!==previous.lastGeneration&&video.currentTime>Math.max(previous.bufferEnd,w.recoveryPlayback.oldBufferEnd)+2&&!video.paused},before,{timeout:30000});
 const resumedAt=await page.evaluate(()=>document.querySelector('video')!.currentTime);
 await page.waitForTimeout(5000);
 const advanced=await page.evaluate(()=>document.querySelector('video')!.currentTime)-resumedAt;
 expect(advanced).toBeGreaterThan(3);expect(advanced).toBeLessThan(8);
 const result=await page.evaluate(()=>(window as any).recoveryPlayback);expect(result.fatal).toEqual([]);
 await request.delete('/streamer/api/v3/streams/'+name,{headers});
});

test('content identity inherits through friendly forms and sources persist failover groups',async({page,request})=>{
 test.setTimeout(20000);
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 const template='ui-content-template',name='ui-content-stream';
 await request.delete('/streamer/api/v3/cluster/sources/ui-replica-primary',{headers});
 expect((await request.put('/streamer/api/v3/templates/'+template,{headers,data:{static:false,inputs:[{url:'testsrc://'}],flussonix_content_id:'news-v1'}})).ok()).toBeTruthy();
 expect((await request.put('/streamer/api/v3/streams/'+name,{headers,data:{$reset:true,template}})).ok()).toBeTruthy();
 await expect(page.getByRole('button',{name,exact:true})).toBeVisible();await page.getByRole('button',{name,exact:true}).click();await page.getByRole('button',{name:'Edit stream',exact:true}).click();
 const identity=page.getByLabel('Content identity',{exact:true});await expect(identity).toHaveValue('');await identity.fill('invalid identity');await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog').getByRole('alert')).toContainText('Content identity must contain');await identity.fill('stream-override');await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toHaveCount(0);
 let stream=await(await request.get('/streamer/api/v3/streams/'+name,{headers})).json();expect(stream.flussonix_content_id).toBe('stream-override');expect(stream.config_on_disk.inputs).toBeUndefined();
 await page.getByRole('button',{name:'Edit stream',exact:true}).click();await identity.fill('');await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toHaveCount(0);
 stream=await(await request.get('/streamer/api/v3/streams/'+name,{headers})).json();expect(stream.flussonix_content_id).toBe('news-v1');expect(stream.config_on_disk.flussonix_content_id).toBeUndefined();
 await page.getByRole('button',{name:'Cluster',exact:true}).click();await page.getByRole('button',{name:'Source servers',exact:true}).click();await page.getByRole('button',{name:'Add source',exact:true}).click();
 await page.getByLabel('Node name',{exact:true}).fill('ui-replica-primary');await page.getByLabel('Management URL',{exact:true}).fill('http://127.0.0.1:19994');await page.getByLabel('Private media URL',{exact:true}).fill('http://127.0.0.1:19995');await page.getByLabel('Failover group',{exact:true}).fill('ui-replicas');
 await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('cell',{name:'ui-replica-primary',exact:true})).toBeVisible();expect((await(await request.get('/streamer/api/v3/cluster/sources/ui-replica-primary',{headers})).json()).flussonix_source_group).toBe('ui-replicas');
 await expect(page.getByRole('heading',{name:'Active source pulls',exact:true})).toBeVisible();await expect(page.locator('textarea')).toHaveCount(0);
});

test('RTSP input transport uses a friendly selector and clears UDP when changing protocol',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 const name='ui-rtsp-transport';
 expect((await request.put('/streamer/api/v3/streams/'+name,{headers,data:{$reset:true,static:false,inputs:[{url:'rtsp://127.0.0.1:19990/camera'}]}})).ok()).toBeTruthy();
 await expect(page.getByRole('button',{name,exact:true})).toBeVisible();await page.getByRole('button',{name,exact:true}).click();await page.getByRole('button',{name:'Edit stream',exact:true}).click();
 const transport=page.getByLabel('RTSP transport',{exact:true});await expect(transport).toHaveValue('tcp');await transport.selectOption('udp');await page.getByRole('button',{name:'Save',exact:true}).click();
 let state=await(await request.get('/streamer/api/v3/streams/'+name,{headers})).json();expect(state.config_on_disk.inputs[0].rtp).toBe('udp');
 await page.getByRole('button',{name:'Edit stream',exact:true}).click();await expect(transport).toHaveValue('udp');await transport.selectOption('tcp');await page.getByRole('button',{name:'Save',exact:true}).click();
 state=await(await request.get('/streamer/api/v3/streams/'+name,{headers})).json();expect(state.config_on_disk.inputs[0].rtp).toBeUndefined();
 await page.getByRole('button',{name:'Edit stream',exact:true}).click();await transport.selectOption('udp');await page.getByLabel('Input URL',{exact:true}).fill('hls://example.net/camera/index.m3u8');await expect(transport).toHaveCount(0);await page.getByRole('button',{name:'Save',exact:true}).click();
 state=await(await request.get('/streamer/api/v3/streams/'+name,{headers})).json();expect(state.config_on_disk.inputs[0].rtp).toBeUndefined();await expect(page.locator('textarea')).toHaveCount(0);
 await request.delete('/streamer/api/v3/streams/'+name,{headers});
});


test('RTSPS trust uses a friendly CA path field and clears it when changing protocol',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};const name='ui-rtsps-trust';
 expect((await request.put('/streamer/api/v3/streams/'+name,{headers,data:{$reset:true,static:false,inputs:[{url:'rtsp://127.0.0.1:19990/camera'}]}})).ok()).toBeTruthy();
 await expect(page.getByRole('button',{name,exact:true})).toBeVisible();await page.getByRole('button',{name,exact:true}).click();await page.getByRole('button',{name:'Edit stream',exact:true}).click();await page.getByLabel('Input URL',{exact:true}).fill('rtsps://localhost:19990/camera');
 const ca=page.getByLabel('Trusted CA file',{exact:true});await expect(ca).toBeVisible();await expect(page.getByLabel('RTSP transport',{exact:true})).toHaveCount(0);await ca.fill('relative.pem');await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByText('Trusted CA file must be an absolute server path.',{exact:true})).toBeVisible();
 await ca.fill(process.env.FLUSSONIX_TEST_CA_FILE!);await page.getByRole('button',{name:'Save',exact:true}).click();let state=await(await request.get('/streamer/api/v3/streams/'+name,{headers})).json();expect(state.config_on_disk.inputs[0].flussonix_tls_ca).toBe(process.env.FLUSSONIX_TEST_CA_FILE);
 await page.getByRole('button',{name:'Edit stream',exact:true}).click();await expect(ca).toHaveValue(process.env.FLUSSONIX_TEST_CA_FILE!);await ca.fill('');await page.getByRole('button',{name:'Save',exact:true}).click();state=await(await request.get('/streamer/api/v3/streams/'+name,{headers})).json();expect(state.config_on_disk.inputs[0].flussonix_tls_ca).toBeUndefined();
 await page.getByRole('button',{name:'Edit stream',exact:true}).click();await ca.fill(process.env.FLUSSONIX_TEST_CA_FILE!);await page.getByLabel('Input URL',{exact:true}).fill('hls://example.net/camera/index.m3u8');await expect(ca).toHaveCount(0);await page.getByRole('button',{name:'Save',exact:true}).click();state=await(await request.get('/streamer/api/v3/streams/'+name,{headers})).json();expect(state.config_on_disk.inputs[0].flussonix_tls_ca).toBeUndefined();await expect(page.locator('textarea')).toHaveCount(0);await request.delete('/streamer/api/v3/streams/'+name,{headers});
});

test('publication forms expose masked publisher credentials and preserve template inheritance',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 const template='ui-publish-template',name='ui-publish-stream';
 await page.route('**/streamer/api/v3/templates/'+template,async route=>{if(route.request().method()==='PUT')await new Promise(resolve=>setTimeout(resolve,250));await route.continue()});
 const save=async(kind:string,id:string)=>{const saved=page.waitForResponse(r=>r.request().method()==='PUT'&&new URL(r.url()).pathname==='/streamer/api/v3/'+kind+'/'+id);await page.getByRole('button',{name:'Save',exact:true}).click();expect((await saved).ok()).toBeTruthy();await expect(page.getByRole('dialog')).toHaveCount(0)};
 await page.getByRole('button',{name:'Templates',exact:true}).click();await page.getByRole('button',{name:'Add template',exact:true}).click();await page.getByLabel('Template name',{exact:true}).fill(template);
 await page.getByLabel('Input mode',{exact:true}).selectOption('publish');await expect(page.getByLabel('Input URL',{exact:true})).toHaveCount(0);await expect(page.getByLabel('Publication URL',{exact:true})).toHaveCount(0);await expect(page.getByText('Create a stream using this template, then publish to that stream’s publication URL.',{exact:true})).toBeVisible();
 const password=page.getByLabel('Publisher password',{exact:true});await expect(password).toHaveAttribute('type','password');await password.fill('owned-browser-publisher');await page.getByLabel('Publisher authorization',{exact:true}).selectOption('url');await page.getByLabel('Publisher callback URL',{exact:true}).fill('http://127.0.0.1:19999/publish');await save('templates',template);
 const t=await(await request.get('/streamer/api/v3/templates/'+template,{headers})).json();expect(t.inputs).toEqual([{url:'publish://'}]);expect(t.password).toBe('owned-browser-publisher');expect(t.on_publish).toBe('http://127.0.0.1:19999/publish');
 expect((await request.put('/streamer/api/v3/streams/'+name,{headers,data:{$reset:true,template}})).ok()).toBeTruthy();await page.getByRole('button',{name:'Streams',exact:true}).click();await expect(page.getByRole('button',{name,exact:true})).toBeVisible();await page.getByRole('button',{name,exact:true}).click();await page.getByRole('button',{name:'Edit stream',exact:true}).click();
 await expect(page.getByLabel('Publication URL',{exact:true})).toHaveValue(new URL('/'+name+'/mpegts',process.env.FLUSSONIX_TEST_URL||'http://127.0.0.1:18210').href);await expect(password).toHaveValue('');await page.getByLabel('Title',{exact:true}).fill('Published channel');await save('streams',name);
 let s=await(await request.get('/streamer/api/v3/streams/'+name,{headers})).json();expect(s.password).toBe('owned-browser-publisher');expect(s.config_on_disk.password).toBeUndefined();expect(s.config_on_disk.inputs).toBeUndefined();
 await page.getByRole('button',{name:'Edit stream',exact:true}).click();await password.fill('override-publisher');await save('streams',name);s=await(await request.get('/streamer/api/v3/streams/'+name,{headers})).json();expect(s.password).toBe('override-publisher');await expect(page.getByText('override-publisher',{exact:true})).toHaveCount(0);
 await page.getByRole('button',{name:'Edit stream',exact:true}).click();await password.fill('');await save('streams',name);s=await(await request.get('/streamer/api/v3/streams/'+name,{headers})).json();expect(s.password).toBe('owned-browser-publisher');await expect(page.locator('textarea')).toHaveCount(0);
 await request.delete('/streamer/api/v3/streams/'+name,{headers});await request.delete('/streamer/api/v3/templates/'+template,{headers});
});
test('subtitle track controls inherit, override and restore template policy without JSON',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 const template='ui-subtitle-template',stream='ui-subtitle-stream';
 const save=async()=>{await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toHaveCount(0);};
 await page.getByRole('button',{name:'Templates',exact:true}).click();await page.getByRole('button',{name:'Add template',exact:true}).click();
 await page.getByLabel('Template name',{exact:true}).fill(template);await page.getByLabel('Input URL',{exact:true}).fill('testsrc://');
 await page.getByLabel('Original subtitle tracks',{exact:true}).selectOption('preserve');
 await save();
 const savedTemplate=await(await request.get('/streamer/api/v3/templates/'+template,{headers})).json();expect(savedTemplate.flussonix_subtitle_tracks).toBe('preserve');
 expect((await request.put('/streamer/api/v3/streams/'+stream,{headers,data:{$reset:true,template,static:false}})).ok()).toBeTruthy();
 await page.getByRole('button',{name:'Streams',exact:true}).click();await page.getByRole('button',{name:stream,exact:true}).click();
 await page.getByRole('button',{name:'Edit stream',exact:true}).click();
 await expect(page.getByLabel('Original subtitle tracks',{exact:true})).toHaveValue('inherit');
 await expect(page.getByRole('dialog')).toContainText('Enhanced/non-Latin teletext remains pending');
 await expect(page.getByRole('dialog').locator('textarea')).toHaveCount(0);
 await page.getByLabel('Title',{exact:true}).fill('Subtitle policy inherited');await save();
 let cfg=await(await request.get('/streamer/api/v3/streams/'+stream,{headers})).json();expect(cfg.flussonix_subtitle_tracks).toBe('preserve');expect(cfg.config_on_disk.flussonix_subtitle_tracks).toBeUndefined();
 await page.getByRole('button',{name:'Edit stream',exact:true}).click();await page.getByLabel('Original subtitle tracks',{exact:true}).selectOption('drop');await save();
 cfg=await(await request.get('/streamer/api/v3/streams/'+stream,{headers})).json();expect(cfg.flussonix_subtitle_tracks).toBe('drop');expect(cfg.config_on_disk.flussonix_subtitle_tracks).toBe('drop');
 await page.getByRole('button',{name:'Edit stream',exact:true}).click();await page.getByLabel('Original subtitle tracks',{exact:true}).selectOption('inherit');await save();
 cfg=await(await request.get('/streamer/api/v3/streams/'+stream,{headers})).json();expect(cfg.flussonix_subtitle_tracks).toBe('preserve');expect(cfg.config_on_disk.flussonix_subtitle_tracks).toBeUndefined();
 await page.getByRole('button',{name:'Transcoder',exact:true}).click();await expect(page.getByText('Keep in compatible outputs',{exact:true})).toBeVisible();
});

test('HLS subtitle controls pass through, convert, filter and inherit without JSON',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 const template='ui-hls-caption-template',stream='ui-hls-caption-stream';const save=async()=>{await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toHaveCount(0);};
 try{
 await page.getByRole('button',{name:'Templates',exact:true}).click();await page.getByRole('button',{name:'Add template',exact:true}).click();await page.getByLabel('Template name',{exact:true}).fill(template);await page.getByLabel('Input mode',{exact:true}).selectOption('publish');await page.getByLabel('Activation',{exact:true}).selectOption('ondemand');
 await expect(page.getByLabel('HLS subtitles',{exact:true})).toBeVisible();await page.getByLabel('HLS subtitles',{exact:true}).selectOption('convert');await page.getByLabel('Caption language 1',{exact:true}).fill('en');await page.getByLabel('Caption name 1',{exact:true}).fill('English');await page.getByRole('button',{name:'Add caption channel',exact:true}).click();await page.getByLabel('Caption channel 2',{exact:true}).selectOption('3');await page.getByLabel('Caption language 2',{exact:true}).fill('es');await page.getByLabel('Caption name 2',{exact:true}).fill('Español');await save();
 let t=await(await request.get('/streamer/api/v3/templates/'+template,{headers})).json();expect(t.flussonix_hls_captions.map((s:any)=>s.channel)).toEqual([1,3]);expect(t.flussonix_hls_subtitles).toBe('convert');
 expect((await request.put('/streamer/api/v3/streams/'+stream,{headers,data:{$reset:true,template}})).ok()).toBeTruthy();await page.getByRole('button',{name:'Streams',exact:true}).click();await expect(page.getByRole('button',{name:stream,exact:true})).toBeVisible();await page.getByRole('button',{name:stream,exact:true}).click();await page.getByRole('button',{name:'Edit stream',exact:true}).click();await expect(page.getByLabel('HLS subtitles',{exact:true})).toHaveValue('inherit');await expect(page.getByRole('dialog')).toContainText('Enhanced/non-Latin teletext remains pending');await expect(page.getByRole('dialog').locator('textarea')).toHaveCount(0);await page.getByLabel('Title',{exact:true}).fill('Inherited HLS captions');await save();
 const get=async()=>await(await request.get('/streamer/api/v3/streams/'+stream,{headers})).json();let cfg=await get();expect(cfg.config_on_disk.flussonix_hls_captions).toBeUndefined();expect(cfg.flussonix_hls_captions).toEqual(t.flussonix_hls_captions);
 for(const mode of ['passthrough','drop']){await page.getByRole('button',{name:'Edit stream',exact:true}).click();await page.getByLabel('HLS subtitles',{exact:true}).selectOption(mode);await save();cfg=await get();expect(cfg.flussonix_hls_subtitles).toBe(mode);expect(cfg.config_on_disk.flussonix_hls_captions).toBeUndefined();}
 await page.getByRole('button',{name:'Edit stream',exact:true}).click();await page.getByLabel('HLS subtitles',{exact:true}).selectOption('convert');await page.getByLabel('Caption name 1',{exact:true}).fill('Overridden captions');await save();cfg=await get();expect(cfg.config_on_disk.flussonix_hls_captions[0].name).toBe('Overridden captions');
 await page.getByRole('button',{name:'Edit stream',exact:true}).click();await page.getByLabel('HLS subtitles',{exact:true}).selectOption('inherit');await save();cfg=await get();expect(cfg.config_on_disk.flussonix_hls_subtitles).toBeUndefined();expect(cfg.config_on_disk.flussonix_hls_captions).toBeUndefined();expect(cfg.flussonix_hls_subtitles).toBe('convert');await page.getByRole('button',{name:'Transcoder',exact:true}).click();await expect(page.getByText('CC1 · English · en',{exact:true})).toBeVisible();
 }finally{await request.delete('/streamer/api/v3/streams/'+stream,{headers}).catch(()=>{});await request.delete('/streamer/api/v3/templates/'+template,{headers}).catch(()=>{});}
});
test('digital caption format controls preserve typed selectors and inheritance',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 const template='ui-708-template',stream='ui-708-stream';
 const save=async()=>{await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toHaveCount(0);};
 try{
  await page.getByRole('button',{name:'Templates',exact:true}).click();await page.getByRole('button',{name:'Add template',exact:true}).click();await page.getByLabel('Template name',{exact:true}).fill(template);await page.getByLabel('Input mode',{exact:true}).selectOption('publish');await page.getByLabel('Activation',{exact:true}).selectOption('ondemand');await page.getByLabel('HLS subtitles',{exact:true}).selectOption('convert');
  await expect(page.getByLabel('Caption format 1',{exact:true})).toBeVisible();await page.getByLabel('Caption format 1',{exact:true}).selectOption('708');await page.getByLabel('Caption service 1',{exact:true}).fill('63');await page.getByLabel('Caption name 1',{exact:true}).fill('Digital English');await page.getByRole('button',{name:'Add caption channel',exact:true}).click();await page.getByLabel('Caption name 2',{exact:true}).fill('Analog English');await save();
  const getTemplate=async()=>await(await request.get('/streamer/api/v3/templates/'+template,{headers})).json();let cfg=await getTemplate();expect(cfg.flussonix_hls_captions).toEqual([{service:63,language:'en',name:'Digital English'},{channel:1,language:'en',name:'Analog English'}]);
  expect((await request.put('/streamer/api/v3/streams/'+stream,{headers,data:{$reset:true,template}})).ok()).toBeTruthy();await page.getByRole('button',{name:'Streams',exact:true}).click();await page.getByRole('button',{name:stream,exact:true}).click();await page.getByRole('button',{name:'Edit stream',exact:true}).click();await page.getByLabel('HLS subtitles',{exact:true}).selectOption('convert');await expect(page.getByLabel('Caption format 1',{exact:true})).toHaveValue('708');await expect(page.getByLabel('Caption service 1',{exact:true})).toHaveValue('63');await expect(page.getByRole('dialog').locator('textarea')).toHaveCount(0);
  await page.getByLabel('Caption format 2',{exact:true}).selectOption('708');await page.getByLabel('Caption service 2',{exact:true}).fill('63');await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toContainText('distinct caption');await page.getByLabel('Caption format 2',{exact:true}).selectOption('608');await expect(page.getByLabel('Caption service 2',{exact:true})).toHaveCount(0);await save();
  const getStream=async()=>await(await request.get('/streamer/api/v3/streams/'+stream,{headers})).json();cfg=await getStream();expect(cfg.config_on_disk.flussonix_hls_captions[1]).toEqual({channel:1,language:'en',name:'Analog English'});
  await page.getByRole('button',{name:'Edit stream',exact:true}).click();await page.getByLabel('HLS subtitles',{exact:true}).selectOption('inherit');await save();cfg=await getStream();expect(cfg.config_on_disk.flussonix_hls_captions).toBeUndefined();expect(cfg.flussonix_hls_captions[0].service).toBe(63);
 }finally{await request.delete('/streamer/api/v3/streams/'+stream,{headers});await request.delete('/streamer/api/v3/templates/'+template,{headers});}
});
test('teletext page controls validate mixed selectors and preserve inheritance without JSON',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 const template='ui-teletext-template',stream='ui-teletext-stream';
 const save=async()=>{await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toHaveCount(0);};
 try{
  await page.getByRole('button',{name:'Templates',exact:true}).click();await page.getByRole('button',{name:'Add template',exact:true}).click();await page.getByLabel('Template name',{exact:true}).fill(template);await page.getByLabel('Input mode',{exact:true}).selectOption('publish');await page.getByLabel('Activation',{exact:true}).selectOption('ondemand');await page.getByLabel('HLS subtitles',{exact:true}).selectOption('convert');
  await expect(page.getByLabel('Caption format 1',{exact:true}).locator('option[value=teletext]')).toHaveCount(1);await page.getByLabel('Caption format 1',{exact:true}).selectOption('teletext');await expect(page.getByLabel('Teletext page 1',{exact:true})).toHaveValue('888');await page.getByLabel('Caption language 1',{exact:true}).fill('de');await page.getByLabel('Caption name 1',{exact:true}).fill('German');
  for(const invalid of ['99','900']){await page.getByLabel('Teletext page 1',{exact:true}).fill(invalid);await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toContainText('100 to 899');}
  await page.getByLabel('Teletext page 1',{exact:true}).fill('899');await page.getByRole('button',{name:'Add caption channel',exact:true}).click();await page.getByLabel('Caption format 2',{exact:true}).selectOption('708');await page.getByLabel('Caption service 2',{exact:true}).fill('1');await page.getByLabel('Caption name 2',{exact:true}).fill('Digital English');await page.getByRole('button',{name:'Add caption channel',exact:true}).click();await page.getByLabel('Caption name 3',{exact:true}).fill('Analog English');await save();
  const t=await(await request.get('/streamer/api/v3/templates/'+template,{headers})).json();expect(t.flussonix_hls_captions).toEqual([{teletext_page:899,language:'de',name:'German'},{service:1,language:'en',name:'Digital English'},{channel:1,language:'en',name:'Analog English'}]);
  expect((await request.put('/streamer/api/v3/streams/'+stream,{headers,data:{$reset:true,template}})).ok()).toBeTruthy();await page.getByRole('button',{name:'Streams',exact:true}).click();await page.getByRole('button',{name:stream,exact:true}).click();await page.getByRole('button',{name:'Edit stream',exact:true}).click();await expect(page.getByLabel('HLS subtitles',{exact:true})).toHaveValue('inherit');await page.getByLabel('HLS subtitles',{exact:true}).selectOption('convert');await expect(page.getByLabel('Teletext page 1',{exact:true})).toHaveValue('899');await expect(page.getByRole('dialog').locator('textarea')).toHaveCount(0);
  await page.getByLabel('Caption format 2',{exact:true}).selectOption('teletext');await page.getByLabel('Teletext page 2',{exact:true}).fill('899');await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toContainText('distinct caption');
  await page.getByLabel('Caption format 2',{exact:true}).selectOption('608');await expect(page.getByLabel('Teletext page 2',{exact:true})).toHaveCount(0);await expect(page.getByLabel('Caption service 2',{exact:true})).toHaveCount(0);await page.getByLabel('Caption channel 2',{exact:true}).selectOption('2');await page.getByLabel('Teletext page 1',{exact:true}).fill('100');await save();
  const get=async()=>await(await request.get('/streamer/api/v3/streams/'+stream,{headers})).json();let s=await get();expect(s.config_on_disk.flussonix_hls_captions[0]).toEqual({teletext_page:100,language:'de',name:'German'});expect(s.config_on_disk.flussonix_hls_captions[1]).toEqual({channel:2,language:'en',name:'Digital English'});
  await page.getByRole('button',{name:'Edit stream',exact:true}).click();await page.getByLabel('HLS subtitles',{exact:true}).selectOption('inherit');await save();s=await get();expect(s.config_on_disk.flussonix_hls_captions).toBeUndefined();expect(s.flussonix_hls_captions).toEqual(t.flussonix_hls_captions);await page.getByRole('button',{name:'Transcoder',exact:true}).click();await expect(page.getByText('Teletext page 899 · German · de',{exact:true})).toBeVisible();
 }finally{await request.delete('/streamer/api/v3/streams/'+stream,{headers}).catch(()=>{});await request.delete('/streamer/api/v3/templates/'+template,{headers}).catch(()=>{});}
});
test('DVB page and recognition controls validate mixed formats and inherit without JSON',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 const template='ui-dvb-template',stream='ui-dvb-stream';const save=async()=>{await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toHaveCount(0);};
 try{
  await page.getByRole('button',{name:'Templates',exact:true}).click();await page.getByRole('button',{name:'Add template',exact:true}).click();await page.getByLabel('Template name',{exact:true}).fill(template);await page.getByLabel('Input mode',{exact:true}).selectOption('publish');await page.getByLabel('Activation',{exact:true}).selectOption('ondemand');await page.getByLabel('HLS subtitles',{exact:true}).selectOption('convert');
  await expect(page.getByLabel('Caption format 1',{exact:true}).locator('option[value=dvb]')).toHaveCount(1);await page.getByLabel('Caption format 1',{exact:true}).selectOption('dvb');await expect(page.getByLabel('DVB page 1',{exact:true})).toHaveValue('1');await expect(page.getByLabel('Recognition language 1',{exact:true})).toHaveValue('eng');
  for(const invalid of ['-1','65536']){await page.getByLabel('DVB page 1',{exact:true}).fill(invalid);await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toContainText('0 to 65535');}
  await page.getByLabel('DVB page 1',{exact:true}).fill('65535');await page.getByLabel('Recognition language 1',{exact:true}).fill('../eng');await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toContainText('recognition language');
  await page.getByLabel('Recognition language 1',{exact:true}).fill('eng+deu');await page.getByLabel('Caption language 1',{exact:true}).fill('de');await page.getByLabel('Caption name 1',{exact:true}).fill('Bitmap German');
  await page.getByRole('button',{name:'Add caption channel',exact:true}).click();await page.getByLabel('Caption format 2',{exact:true}).selectOption('708');await page.getByLabel('Caption name 2',{exact:true}).fill('Digital');
  await page.getByRole('button',{name:'Add caption channel',exact:true}).click();await page.getByLabel('Caption format 3',{exact:true}).selectOption('teletext');await page.getByLabel('Caption name 3',{exact:true}).fill('Teletext');
  await page.getByRole('button',{name:'Add caption channel',exact:true}).click();await page.getByLabel('Caption name 4',{exact:true}).fill('Analog');await expect(page.getByRole('button',{name:'Add caption channel',exact:true})).toBeDisabled();await expect(page.getByRole('dialog').locator('textarea')).toHaveCount(0);await save();
  const t=await(await request.get('/streamer/api/v3/templates/'+template,{headers})).json();expect(t.flussonix_hls_captions[0]).toEqual({dvb_page:65535,ocr_language:'eng+deu',language:'de',name:'Bitmap German'});expect(t.flussonix_hls_captions[1].ocr_language).toBeUndefined();
  expect((await request.put('/streamer/api/v3/streams/'+stream,{headers,data:{$reset:true,template}})).ok()).toBeTruthy();await page.getByRole('button',{name:'Streams',exact:true}).click();await page.getByRole('button',{name:stream,exact:true}).click();await page.getByRole('button',{name:'Edit stream',exact:true}).click();await page.getByLabel('HLS subtitles',{exact:true}).selectOption('convert');await expect(page.getByLabel('DVB page 1',{exact:true})).toHaveValue('65535');await expect(page.getByLabel('Recognition language 1',{exact:true})).toHaveValue('eng+deu');
  await page.getByLabel('Caption format 2',{exact:true}).selectOption('dvb');await page.getByLabel('DVB page 2',{exact:true}).fill('65535');await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toContainText('distinct caption');
  await page.getByLabel('Caption format 2',{exact:true}).selectOption('608');await expect(page.getByLabel('Recognition language 2',{exact:true})).toHaveCount(0);await expect(page.getByLabel('DVB page 2',{exact:true})).toHaveCount(0);await page.getByLabel('Caption channel 2',{exact:true}).selectOption('2');await page.getByLabel('DVB page 1',{exact:true}).fill('0');await save();
  const get=async()=>await(await request.get('/streamer/api/v3/streams/'+stream,{headers})).json();let s=await get();expect(s.config_on_disk.flussonix_hls_captions[0].dvb_page).toBe(0);expect(s.config_on_disk.flussonix_hls_captions[1].ocr_language).toBeUndefined();
  await page.getByRole('button',{name:'Edit stream',exact:true}).click();await page.getByLabel('HLS subtitles',{exact:true}).selectOption('inherit');await save();s=await get();expect(s.config_on_disk.flussonix_hls_captions).toBeUndefined();expect(s.flussonix_hls_captions).toEqual(t.flussonix_hls_captions);await page.getByRole('button',{name:'Transcoder',exact:true}).click();await expect(page.getByText('DVB page 65535 · Bitmap German · de · eng+deu',{exact:true})).toBeVisible();
 }finally{await request.delete('/streamer/api/v3/streams/'+stream,{headers}).catch(()=>{});await request.delete('/streamer/api/v3/templates/'+template,{headers}).catch(()=>{});}
});
test('native text controls retain full track IDs and template inheritance without JSON',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 const template='ui-native-template',stream='ui-native-stream';const save=async()=>{await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toHaveCount(0);};
 try{
  await page.getByRole('button',{name:'Templates',exact:true}).click();await page.getByRole('button',{name:'Add template',exact:true}).click();await page.getByLabel('Template name',{exact:true}).fill(template);await page.getByLabel('Input URL',{exact:true}).fill('m4s://127.0.0.1:9/owned');await page.getByLabel('Activation',{exact:true}).selectOption('ondemand');await page.getByLabel('HLS subtitles',{exact:true}).selectOption('convert');
  await expect(page.getByLabel('Caption format 1',{exact:true}).locator('option[value=native]')).toHaveCount(1);await page.getByLabel('Caption format 1',{exact:true}).selectOption('native');
  for(const invalid of ['0','4294967296']){await page.getByLabel('Native track ID 1',{exact:true}).fill(invalid);await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toContainText('1 to 4294967295');}
  await page.getByLabel('Native track ID 1',{exact:true}).fill('4294967295');await page.getByLabel('Caption language 1',{exact:true}).fill('de');await page.getByLabel('Caption name 1',{exact:true}).fill('Native German');
  await page.getByRole('button',{name:'Add caption channel',exact:true}).click();await page.getByLabel('Caption name 2',{exact:true}).fill('Native English');await save();
  const t=await(await request.get('/streamer/api/v3/templates/'+template,{headers})).json();expect(t.flussonix_hls_captions).toEqual([{native_track:4294967295,language:'de',name:'Native German'},{native_track:1,language:'en',name:'Native English'}]);
  expect((await request.put('/streamer/api/v3/streams/'+stream,{headers,data:{$reset:true,template}})).ok()).toBeTruthy();await page.getByRole('button',{name:'Streams',exact:true}).click();await page.getByRole('button',{name:stream,exact:true}).click();await page.getByRole('button',{name:'Edit stream',exact:true}).click();await page.getByLabel('HLS subtitles',{exact:true}).selectOption('convert');await expect(page.getByLabel('Native track ID 1',{exact:true})).toHaveValue('4294967295');await expect(page.getByRole('dialog').locator('textarea')).toHaveCount(0);
  await page.getByLabel('Caption format 2',{exact:true}).selectOption('608');await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toContainText('Native text cannot be mixed');await page.getByLabel('Caption format 2',{exact:true}).selectOption('native');await page.getByLabel('Native track ID 2',{exact:true}).fill('4294967295');await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toContainText('distinct caption');await page.getByLabel('Native track ID 2',{exact:true}).fill('7');await save();
  await page.getByRole('button',{name:'Edit stream',exact:true}).click();await page.getByLabel('HLS subtitles',{exact:true}).selectOption('inherit');await save();const s=await(await request.get('/streamer/api/v3/streams/'+stream,{headers})).json();expect(s.config_on_disk.flussonix_hls_captions).toBeUndefined();expect(s.flussonix_hls_captions).toEqual(t.flussonix_hls_captions);await page.getByRole('button',{name:'Transcoder',exact:true}).click();await expect(page.getByText('Native track 4294967295 · Native German · de',{exact:true})).toBeVisible();
 }finally{await request.delete('/streamer/api/v3/streams/'+stream,{headers}).catch(()=>{});await request.delete('/streamer/api/v3/templates/'+template,{headers}).catch(()=>{});}
});
test('reopening a saved source uses acknowledged settings while refresh is delayed',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};const name='ui-save-race';
 const releases:Array<()=>void>=[];let delay=false;
 try{
  expect((await request.put(`/streamer/api/v3/cluster/sources/${name}`,{headers,data:{api_url:'https://source.example/control',cluster_key:'owned-save-race-key'}})).ok()).toBeTruthy();
  await page.getByRole('button',{name:'Cluster',exact:true}).click();await page.getByRole('button',{name:'Source servers',exact:true}).click();
  const edit=()=>page.getByRole('row').filter({has:page.getByRole('cell',{name,exact:true})}).getByRole('button',{name:'Edit',exact:true}).click();
  await edit();await page.getByLabel('Source transport',{exact:true}).selectOption('mpegts');
  await page.route('**/streamer/api/v3/config',async route=>{const response=await route.fetch();if(delay)await new Promise<void>(resolve=>releases.push(resolve));await route.fulfill({response});});
  delay=true;await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toHaveCount(0);
  const saved=await(await request.get(`/streamer/api/v3/cluster/sources/${name}`,{headers})).json();expect(saved.flussonix_transport).toBe('mpegts');
  await edit();await expect(page.getByLabel('Source transport',{exact:true})).toHaveValue('mpegts');
 }finally{delay=false;for(const release of releases)release();await page.unrouteAll({behavior:'wait'});await request.delete(`/streamer/api/v3/cluster/sources/${name}`,{headers}).catch(()=>{});}
});
test('slow successful polling still loads the requested collection',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};const name='ui-slow-poll';
 try{
  expect((await request.put(`/streamer/api/v3/templates/${name}`,{headers,data:{static:false,inputs:[{url:'testsrc://'}]}})).ok()).toBeTruthy();
  await page.route('**/streamer/api/v3/templates',async route=>{const response=await route.fetch();await new Promise(resolve=>setTimeout(resolve,4000));await route.fulfill({response});});
  await page.getByRole('button',{name:'Templates',exact:true}).click();
  await expect(page.getByRole('cell',{name,exact:true})).toBeVisible({timeout:8500});
 }finally{await page.unrouteAll({behavior:'wait'});await request.delete(`/streamer/api/v3/templates/${name}`,{headers}).catch(()=>{});}
});
test('clicking the active tab during a slow load does not stop polling',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};const name='ui-active-poll';let started!:()=>void;const pending=new Promise<void>(resolve=>started=resolve);
 try{
  expect((await request.put(`/streamer/api/v3/templates/${name}`,{headers,data:{static:false,inputs:[{url:'testsrc://'}]}})).ok()).toBeTruthy();
  await page.route('**/streamer/api/v3/templates',async route=>{const response=await route.fetch();started();await new Promise(resolve=>setTimeout(resolve,4000));await route.fulfill({response});});
  await page.getByRole('button',{name:'Templates',exact:true}).click();await pending;
  await page.getByRole('button',{name:'Templates',exact:true}).click();
  await expect(page.getByRole('cell',{name,exact:true})).toBeVisible({timeout:8500});
 }finally{await page.unrouteAll({behavior:'wait'});await request.delete(`/streamer/api/v3/templates/${name}`,{headers}).catch(()=>{});}
});


test('native TLS inputs retain a friendly trusted CA field across secure protocols',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 const name='ui-native-tls-trust';
 expect((await request.put('/streamer/api/v3/streams/'+name,{headers,data:{$reset:true,static:false,inputs:[{url:'m4fs://localhost:19990/owned'}]}})).ok()).toBeTruthy();
 await expect(page.getByRole('button',{name,exact:true})).toBeVisible();await page.getByRole('button',{name,exact:true}).click();await page.getByRole('button',{name:'Edit stream',exact:true}).click();
 const ca=page.getByLabel('Trusted CA file',{exact:true});const url=page.getByLabel('Input URL',{exact:true});
 await expect(ca).toBeVisible();await ca.fill('relative.pem');await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByText('Trusted CA file must be an absolute server path.',{exact:true})).toBeVisible();
 await ca.fill(process.env.FLUSSONIX_TEST_CA_FILE!);
 for(const protocol of ['m4ss','rtsps','m4fs']) {await url.fill(protocol+'://localhost:19990/owned');await expect(ca).toHaveValue(process.env.FLUSSONIX_TEST_CA_FILE!)}
 await page.getByRole('button',{name:'Save',exact:true}).click();let state=await(await request.get('/streamer/api/v3/streams/'+name,{headers})).json();expect(state.config_on_disk.inputs[0].flussonix_tls_ca).toBe(process.env.FLUSSONIX_TEST_CA_FILE);
 await page.getByRole('button',{name:'Edit stream',exact:true}).click();await expect(ca).toHaveValue(process.env.FLUSSONIX_TEST_CA_FILE!);await url.fill('m4s://localhost:19990/owned');await expect(ca).toHaveCount(0);await page.getByRole('button',{name:'Save',exact:true}).click();state=await(await request.get('/streamer/api/v3/streams/'+name,{headers})).json();expect(state.config_on_disk.inputs[0].flussonix_tls_ca).toBeUndefined();await expect(page.locator('textarea')).toHaveCount(0);await request.delete('/streamer/api/v3/streams/'+name,{headers});
});


test('cluster trust settings persist and clear with endpoint protocol changes',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};const name='ui-cluster-ca';
 expect((await request.put(`/streamer/api/v3/cluster/sources/${name}`,{headers,data:{api_url:'https://localhost:19997',private_payload_url:'https://localhost:19998'}})).ok()).toBeTruthy();
 await page.getByRole('button',{name:'Cluster',exact:true}).click();await page.getByRole('button',{name:'Source servers',exact:true}).click();
 const edit=async()=>{await page.getByRole('row').filter({has:page.getByRole('cell',{name,exact:true})}).getByRole('button',{name:'Edit',exact:true}).click()};await edit();
 const management=page.getByLabel('Management trusted CA file',{exact:true});const media=page.getByLabel('Private media trusted CA file',{exact:true});await expect(management).toBeVisible();await expect(media).toBeVisible();
 await management.fill('relative.pem');await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog').getByRole('alert')).toContainText('Trusted CA file must be an absolute server path');
 await management.fill(process.env.FLUSSONIX_TEST_CA_FILE!);await media.fill(process.env.FLUSSONIX_TEST_CA_FILE!);await page.getByRole('button',{name:'Save',exact:true}).click();
 let saved=await(await request.get(`/streamer/api/v3/cluster/sources/${name}`,{headers})).json();expect(saved.flussonix_tls_ca).toBe(process.env.FLUSSONIX_TEST_CA_FILE);expect(saved.flussonix_media_tls_ca).toBe(process.env.FLUSSONIX_TEST_CA_FILE);
 await edit();await expect(management).toHaveValue(process.env.FLUSSONIX_TEST_CA_FILE!);await expect(media).toHaveValue(process.env.FLUSSONIX_TEST_CA_FILE!);
 await page.getByLabel('Private media URL',{exact:true}).fill('');await expect(media).toHaveValue(process.env.FLUSSONIX_TEST_CA_FILE!);const cleared=page.waitForResponse(r=>r.request().method()==='PUT'&&new URL(r.url()).pathname===`/streamer/api/v3/cluster/sources/${name}`);await page.getByRole('button',{name:'Save',exact:true}).click();expect((await cleared).ok()).toBeTruthy();await expect(page.getByRole('dialog')).toHaveCount(0);
 saved=await(await request.get(`/streamer/api/v3/cluster/sources/${name}`,{headers})).json();expect(saved.private_payload_url).toBeUndefined();expect(saved.flussonix_media_tls_ca).toBe(process.env.FLUSSONIX_TEST_CA_FILE);await edit();
 await page.getByLabel('Private media URL',{exact:true}).fill('http://localhost:19998');await expect(media).toHaveCount(0);await expect(management).toHaveValue(process.env.FLUSSONIX_TEST_CA_FILE!);await page.getByLabel('Management URL',{exact:true}).fill('http://localhost:19997');await expect(management).toHaveCount(0);await page.getByRole('button',{name:'Save',exact:true}).click();
 saved=await(await request.get(`/streamer/api/v3/cluster/sources/${name}`,{headers})).json();expect(saved.flussonix_tls_ca).toBeUndefined();expect(saved.flussonix_media_tls_ca).toBeUndefined();await expect(page.locator('textarea')).toHaveCount(0);await request.delete(`/streamer/api/v3/cluster/sources/${name}`,{headers});
});

for(const kind of ['streams','templates'] as const) test(`HTTPS pull trust uses friendly fields in ${kind}`,async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};const name='ui-http-ca-'+kind;const caFile=process.env.FLUSSONIX_TEST_CA_FILE!;
 expect((await request.put(`/streamer/api/v3/${kind}/${name}`,{headers,data:{$reset:true,static:false,inputs:[{url:'hlss://localhost:19990/entry'}]}})).ok()).toBeTruthy();
 const edit=async()=>{if(kind==='streams'){await page.getByRole('button',{name,exact:true}).click();await page.getByRole('button',{name:'Edit stream',exact:true}).click()}else{await page.getByRole('button',{name:'Templates',exact:true}).click();await page.getByRole('row').filter({has:page.getByRole('cell',{name,exact:true})}).getByRole('button',{name:'Edit',exact:true}).click()}};
 await edit();const ca=page.getByLabel('Trusted CA file',{exact:true});const input=page.getByLabel('Input URL',{exact:true});await expect(ca).toBeVisible();await ca.fill('relative.pem');await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog').getByRole('alert')).toContainText('Trusted CA file must be an absolute server path');await ca.fill(caFile);
 for(const scheme of ['tshttps','m4fs','m4ss','rtsps','https','hlss']){await input.fill(`${scheme}://localhost:19990/entry`);await expect(ca).toHaveValue(caFile)}await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toHaveCount(0);
 let state=await(await request.get(`/streamer/api/v3/${kind}/${name}`,{headers})).json();expect((state.config_on_disk||state).inputs[0].flussonix_tls_ca).toBe(caFile);
 if(kind==='templates'){const inherited=name+'-inherited';try{expect((await request.put(`/streamer/api/v3/streams/${inherited}`,{headers,data:{$reset:true,template:name}})).ok()).toBeTruthy();const stream=await(await request.get(`/streamer/api/v3/streams/${inherited}`,{headers})).json();expect(stream.inputs[0].flussonix_tls_ca).toBe(caFile);expect(stream.config_on_disk.inputs).toBeUndefined()}finally{await request.delete(`/streamer/api/v3/streams/${inherited}`,{headers})}}
 if(kind==='streams'){await page.getByRole('button',{name:'Edit stream',exact:true}).click()}else{await page.getByRole('row').filter({has:page.getByRole('cell',{name,exact:true})}).getByRole('button',{name:'Edit',exact:true}).click()}
 await expect(ca).toHaveValue(caFile);await ca.fill('');await page.getByRole('button',{name:'Save',exact:true}).click();state=await(await request.get(`/streamer/api/v3/${kind}/${name}`,{headers})).json();expect((state.config_on_disk||state).inputs[0].flussonix_tls_ca).toBeUndefined();
 if(kind==='streams'){await page.getByRole('button',{name:'Edit stream',exact:true}).click()}else{await page.getByRole('row').filter({has:page.getByRole('cell',{name,exact:true})}).getByRole('button',{name:'Edit',exact:true}).click()}
 await ca.fill(caFile);await input.fill('tshttp://localhost:19990/transport');await expect(ca).toHaveCount(0);await page.getByRole('button',{name:'Save',exact:true}).click();state=await(await request.get(`/streamer/api/v3/${kind}/${name}`,{headers})).json();expect((state.config_on_disk||state).inputs[0].flussonix_tls_ca).toBeUndefined();await expect(page.locator('textarea')).toHaveCount(0);await request.delete(`/streamer/api/v3/${kind}/${name}`,{headers});
});

test('friendly CPU HEVC and audio controls round-trip template profiles independently',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 const name='ui-codec-template-'+Date.now();
 await page.getByRole('button',{name:'Templates',exact:true}).click();await page.getByRole('button',{name:'Add template',exact:true}).click();
 await page.getByLabel('Template name',{exact:true}).fill(name);await page.getByLabel('Input URL',{exact:true}).fill('testsrc://');await page.getByLabel('Activation',{exact:true}).selectOption('ondemand');
 await page.getByLabel('Transcoding',{exact:true}).selectOption('libx265');await page.getByLabel('Video bitrate (kb/s)',{exact:true}).fill('1100');
 await page.getByLabel('Audio encoding',{exact:true}).selectOption('mp2a');await page.getByLabel('Audio bitrate (kb/s)',{exact:true}).selectOption('192');
 await expect(page.locator('textarea')).toHaveCount(0);await page.getByRole('button',{name:'Save',exact:true}).click();
 await expect(page.getByRole('cell',{name,exact:true})).toBeVisible();
 const read=async()=>(await(await request.get('/streamer/api/v3/templates/'+name,{headers})).json()).transcoder;
 expect(await read()).toEqual({encoder:'libx265',vb:1100,acodec:'mp2a',ab:192});
 await page.getByRole('row').filter({has:page.getByRole('cell',{name,exact:true})}).getByRole('button',{name:'Edit',exact:true}).click();
 await page.getByLabel('Transcoding',{exact:true}).selectOption('libx264');await expect(page.getByLabel('Audio encoding',{exact:true})).toHaveValue('mp2a');
 await page.getByLabel('Audio encoding',{exact:true}).selectOption('copy');await expect(page.getByLabel('Video bitrate (kb/s)',{exact:true})).toHaveValue('1100');await expect(page.getByLabel('Audio bitrate (kb/s)',{exact:true})).toHaveCount(0);
 await page.getByRole('button',{name:'Save',exact:true}).click();expect(await read()).toEqual({encoder:'libx264',vb:1100,acodec:'copy'});
});

test('audio-only stream override keeps independent template video inheritance',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 const template='ui-codec-inherit',name='ui-audio-only';
 expect((await request.put('/streamer/api/v3/templates/'+template,{headers,data:{static:false,transcoder:{encoder:'libx265',vb:900,acodec:'mp2a',ab:192}}})).ok()).toBeTruthy();
 expect((await request.put('/streamer/api/v3/streams/'+name,{headers,data:{template}})).ok()).toBeTruthy();
 await expect(page.getByRole('button',{name,exact:true})).toBeVisible();await page.getByRole('button',{name,exact:true}).click();await page.getByRole('button',{name:'Edit stream',exact:true}).click();
 await expect(page.getByLabel('Transcoding',{exact:true})).toHaveValue('inherit');await page.getByLabel('Audio encoding',{exact:true}).selectOption('mp3');
 await expect(page.getByLabel('Transcoding',{exact:true})).toHaveValue('inherit');await expect(page.getByLabel('Audio bitrate (kb/s)',{exact:true})).toHaveValue('128');
 await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('heading',{name,exact:true})).toBeVisible();
 const read=async()=>await(await request.get('/streamer/api/v3/streams/'+name,{headers})).json();
 expect((await read()).config_on_disk.transcoder).toEqual({acodec:'mp3',ab:128});
 expect((await request.put('/streamer/api/v3/templates/'+template,{headers,data:{transcoder:{encoder:'libx264',vb:1200,acodec:'aac',ab:96}}})).ok()).toBeTruthy();
 expect((await read()).transcoder).toEqual({encoder:'libx264',vb:1200,acodec:'mp3',ab:128});
 await page.getByRole('button',{name:'Edit stream',exact:true}).click();await page.getByLabel('Transcoding',{exact:true}).selectOption('copy');
 await expect(page.getByLabel('Audio encoding',{exact:true})).toHaveValue('mp3');await page.getByLabel('Transcoding',{exact:true}).selectOption('inherit');
 await expect(page.getByLabel('Audio encoding',{exact:true})).toHaveValue('mp3');await page.getByLabel('Audio encoding',{exact:true}).selectOption('inherit');
 await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('heading',{name,exact:true})).toBeVisible();
 expect((await read()).config_on_disk.transcoder).toBeUndefined();expect((await read()).transcoder.acodec).toBe('aac');
});

test('changing audio retains legacy empty-profile video encoding',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 const name='ui-legacy-codecs';
 expect((await request.put('/streamer/api/v3/templates/'+name,{headers,data:{static:false,transcoder:{}}})).ok()).toBeTruthy();
 await page.getByRole('button',{name:'Templates',exact:true}).click();await page.getByRole('row').filter({has:page.getByRole('cell',{name,exact:true})}).getByRole('button',{name:'Edit',exact:true}).click();
 await expect(page.getByLabel('Transcoding',{exact:true})).toHaveValue('libx264');await page.getByLabel('Audio encoding',{exact:true}).selectOption('mp3');
 await expect(page.getByLabel('Transcoding',{exact:true})).toHaveValue('libx264');await page.getByRole('button',{name:'Save',exact:true}).click();
 await expect(page.getByRole('cell',{name,exact:true})).toBeVisible();const t=await(await request.get('/streamer/api/v3/templates/'+name,{headers})).json();expect(t.transcoder).toEqual({encoder:'libx264',acodec:'mp3',ab:128});
});

test('partial and pinned audio overrides retain the effective template bitrate',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 const template='ui-codec-rates-'+Date.now(),name='ui-partial-audio-'+Date.now();
 expect((await request.put('/streamer/api/v3/templates/'+template,{headers,data:{static:false,transcoder:{encoder:'libx265',vb:1800,acodec:'mp3',ab:320}}})).ok()).toBeTruthy();
 expect((await request.put('/streamer/api/v3/streams/'+name,{headers,data:{template,transcoder:{encoder:'libx265',acodec:'mp3'}}})).ok()).toBeTruthy();
 await expect(page.getByRole('button',{name,exact:true})).toBeVisible();await page.getByRole('button',{name,exact:true}).click();await page.getByRole('button',{name:'Edit stream',exact:true}).click();
 await expect(page.getByLabel('Audio bitrate (kb/s)',{exact:true})).toHaveValue('320');await expect(page.getByLabel('Video bitrate (kb/s)',{exact:true})).toHaveValue('1800');await page.getByLabel('Transcoding',{exact:true}).selectOption('inherit');
 await page.getByLabel('Audio encoding',{exact:true}).selectOption('inherit');await page.getByLabel('Audio encoding',{exact:true}).selectOption('mp3');
 await expect(page.getByLabel('Audio bitrate (kb/s)',{exact:true})).toHaveValue('320');await expect(page.getByLabel('Transcoding',{exact:true})).toHaveValue('inherit');
 await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('heading',{name,exact:true})).toBeVisible();
 const stream=await(await request.get('/streamer/api/v3/streams/'+name,{headers})).json();expect(stream.config_on_disk.transcoder).toEqual({acodec:'mp3',ab:320});expect(stream.transcoder.encoder).toBe('libx265');
});

 test('editing inherited legacy audio retains the existing video encoder',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 const template='ui-legacy-inherited',name='ui-legacy-stream';
 expect((await request.put('/streamer/api/v3/templates/'+template,{headers,data:{static:false,transcoder:{}}})).ok()).toBeTruthy();
 expect((await request.put('/streamer/api/v3/streams/'+name,{headers,data:{template}})).ok()).toBeTruthy();
 await expect(page.getByRole('button',{name,exact:true})).toBeVisible();await page.getByRole('button',{name,exact:true}).click();await page.getByRole('button',{name:'Edit stream',exact:true}).click();await page.getByLabel('Audio encoding',{exact:true}).selectOption('mp3');
 await expect(page.getByLabel('Transcoding',{exact:true})).toHaveValue('inherit');await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('heading',{name,exact:true})).toBeVisible();
 const stream=await(await request.get('/streamer/api/v3/streams/'+name,{headers})).json();expect(stream.transcoder.encoder).toBe('libx264');expect(stream.transcoder.acodec).toBe('mp3');
 });

test('bitrate-only overrides keep inherited codecs and matching bitrate controls',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 const template='ui-rate-only-template-'+Date.now(),name='ui-rate-only-'+Date.now();
 expect((await request.put('/streamer/api/v3/templates/'+template,{headers,data:{static:false,transcoder:{encoder:'libx265',vb:1800,acodec:'mp3',ab:320}}})).ok()).toBeTruthy();
 expect((await request.put('/streamer/api/v3/streams/'+name,{headers,data:{template,transcoder:{vb:1000,ab:128}}})).ok()).toBeTruthy();
 await expect(page.getByRole('button',{name,exact:true})).toBeVisible();await page.getByRole('button',{name,exact:true}).click();await page.getByRole('button',{name:'Edit stream',exact:true}).click();
 await expect(page.getByLabel('Transcoding',{exact:true})).toHaveValue('inherit');await expect(page.getByLabel('Audio encoding',{exact:true})).toHaveValue('inherit');
 await expect(page.getByLabel('Video bitrate (kb/s)',{exact:true})).toHaveValue('1000');await expect(page.getByLabel('Audio bitrate (kb/s)',{exact:true})).toHaveJSProperty('tagName','SELECT');await expect(page.getByLabel('Audio bitrate (kb/s)',{exact:true})).toHaveValue('128');
 await page.getByLabel('Video bitrate (kb/s)',{exact:true}).fill('2000');await page.getByLabel('Audio bitrate (kb/s)',{exact:true}).selectOption('320');
 await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('heading',{name,exact:true})).toBeVisible();
 const stream=await(await request.get('/streamer/api/v3/streams/'+name,{headers})).json();expect(stream.config_on_disk.transcoder).toEqual({vb:2000,ab:320});expect(stream.transcoder).toEqual({encoder:'libx265',vb:2000,acodec:'mp3',ab:320});
 await expect(page.getByText('Use template video · 2000 kb/s · Use template / default audio · 320 kb/s',{exact:true})).toBeVisible();
});

test('audio bitrate edits preserve standalone and inherited legacy video defaults',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 const suffix=Date.now(),direct='ui-legacy-rate-'+suffix,parent='ui-legacy-rate-parent-'+suffix,name='ui-legacy-rate-stream-'+suffix;
 for(const template of [direct,parent])expect((await request.put('/streamer/api/v3/templates/'+template,{headers,data:{static:false,transcoder:{}}})).ok()).toBeTruthy();
 expect((await request.put('/streamer/api/v3/streams/'+name,{headers,data:{template:parent}})).ok()).toBeTruthy();
 await page.getByRole('button',{name:'Templates',exact:true}).click();await page.getByRole('row').filter({has:page.getByRole('cell',{name:direct,exact:true})}).getByRole('button',{name:'Edit',exact:true}).click();
 await expect(page.getByLabel('Audio bitrate (kb/s)',{exact:true})).toHaveValue('96');await page.getByLabel('Audio bitrate (kb/s)',{exact:true}).fill('128');await expect(page.getByLabel('Transcoding',{exact:true})).toHaveValue('libx264');
 await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('cell',{name:direct,exact:true})).toBeVisible();
 const template=await(await request.get('/streamer/api/v3/templates/'+direct,{headers})).json();expect(template.transcoder).toEqual({encoder:'libx264',ab:128});
 await page.getByRole('button',{name:'Streams',exact:true}).click();await expect(page.getByRole('button',{name,exact:true})).toBeVisible();await page.getByRole('button',{name,exact:true}).click();await page.getByRole('button',{name:'Edit stream',exact:true}).click();
 await page.getByLabel('Audio bitrate (kb/s)',{exact:true}).fill('128');await expect(page.getByLabel('Transcoding',{exact:true})).toHaveValue('inherit');await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('heading',{name,exact:true})).toBeVisible();
 const stream=await(await request.get('/streamer/api/v3/streams/'+name,{headers})).json();expect(stream.config_on_disk.transcoder).toEqual({ab:128});expect(stream.transcoder.encoder).toBe('libx264');
});
