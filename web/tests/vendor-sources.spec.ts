import {test,expect,type Page} from '@playwright/test';

const headers=()=>({Authorization:'Basic '+Buffer.from('admin:'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')});
const source='m4ss://owned-ui-origin.invalid:8443';
const uri='/streamer/api/v3/cluster/sources/'+encodeURIComponent(source);
async function sources(page:Page){await page.getByRole('button',{name:'Cluster',exact:true}).click();await page.getByRole('button',{name:'Source servers',exact:true}).click();}
const row=(page:Page)=>page.getByRole('row').filter({has:page.getByRole('cell',{name:source,exact:true})});
async function edit(page:Page){await row(page).getByRole('button',{name:'Edit',exact:true}).click();}
test.beforeEach(async({page,request})=>{
 await request.delete(uri,{headers:headers()});
 await page.goto('/admin/');await page.getByLabel('Username').fill('admin');await page.getByLabel('Password',{exact:true}).fill(process.env.FLUSSONIX_ADMIN_PASSWORD!);await page.getByRole('button',{name:'Sign in',exact:true}).click();await sources(page);
});
test.afterEach(async({request})=>{await request.delete(uri,{headers:headers()});await request.delete('/streamer/api/v3/templates/owned-ui-processing',{headers:headers()});await request.delete('/streamer/api/v3/auth_backends/owned-ui-billing',{headers:headers()});});

test('vendor source form defaults to unresolved viewer policy and masks private credentials',async({page,request})=>{
 await page.getByRole('button',{name:'Add source',exact:true}).click();
 await page.getByLabel('Source type',{exact:true}).selectOption('flussonic');
 await page.getByLabel('Source address',{exact:true}).fill(source);
 await expect(page.getByLabel('Viewer policy',{exact:true})).toHaveValue('inherit');
 await expect(page.getByLabel('Stream activation',{exact:true})).toHaveCount(0);
 await expect(page.getByLabel('Public delivery URL',{exact:true})).toHaveCount(0);
 await expect(page.getByLabel('Failover group',{exact:true})).toHaveCount(0);
 await expect(page.getByLabel('Source transport',{exact:true})).toHaveCount(0);
 await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog').getByRole('alert')).toContainText('Cluster key is required');
 await page.getByLabel('Cluster key',{exact:true}).fill('owned-vendor-private-key');await expect(page.getByLabel('Cluster key',{exact:true})).toHaveAttribute('type','password');
 await page.getByRole('button',{name:'Save',exact:true}).click();await expect(row(page)).toBeVisible();
 const saved=await(await request.get(uri,{headers:headers()})).json();expect(saved.flussonix_source_kind).toBe('flussonic');expect(saved.on_play).toBeUndefined();expect(saved.cluster_key).toBe('owned-vendor-private-key');
 await edit(page);await expect(page.getByLabel('Source address',{exact:true})).toHaveAttribute('readonly','');await expect(page.locator('textarea')).toHaveCount(0);
 await page.screenshot({path:'../.runtime/screenshots/vendor-source-desktop.png',fullPage:true});
 await page.setViewportSize({width:393,height:852});expect(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth)).toBe(false);await page.screenshot({path:'../.runtime/screenshots/vendor-source-mobile.png',fullPage:true});
 await page.getByLabel('Viewer token SHA256',{exact:true}).fill('d'.repeat(64));await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toHaveCount(0);const guarded=await(await request.get(uri,{headers:headers()})).json();expect(guarded.on_play).toBeUndefined();expect(guarded.flussonix_token_sha256).toBe('d'.repeat(64));
});

test('vendor source callback and group overrides persist and Config staging does not apply early',async({page,request})=>{
 expect((await request.put('/streamer/api/v3/auth_backends/owned-ui-billing',{headers:headers(),data:{url:'https://billing.example/check'}})).ok()).toBeTruthy();
 await page.getByRole('button',{name:'Add source',exact:true}).click();await page.getByLabel('Source type',{exact:true}).selectOption('flussonic');await page.getByLabel('Source address',{exact:true}).fill(source);await page.getByLabel('Cluster key',{exact:true}).fill('owned-vendor-private-key');
 await page.getByLabel('Viewer policy',{exact:true}).selectOption('backend');await page.getByLabel('Viewer backend',{exact:true}).selectOption('owned-ui-billing');await page.getByLabel('Viewer maximum sessions',{exact:true}).fill('3');await page.getByLabel('Viewer token SHA256',{exact:true}).fill('a'.repeat(64));
 await page.getByLabel('New group name',{exact:true}).fill('free');await page.getByRole('button',{name:'Add group override',exact:true}).click();
 await page.getByLabel('Group free policy',{exact:true}).selectOption('allow');await page.getByLabel('Group free token SHA256',{exact:true}).fill('b'.repeat(64));
 await page.getByLabel('New group name',{exact:true}).fill('free');await page.getByRole('button',{name:'Add group override',exact:true}).click();await expect(page.getByText('That group already has an override.',{exact:true})).toBeVisible();
 await page.getByRole('button',{name:'Save',exact:true}).click();await expect(row(page)).toBeVisible();
 let saved=await(await request.get(uri,{headers:headers()})).json();expect(saved.on_play).toEqual({url:'auth://owned-ui-billing',max_sessions:3});expect(saved.group_config.free).toEqual({on_play:'true',flussonix_token_sha256:'b'.repeat(64)});
 await edit(page);await expect(page.getByLabel('Viewer backend',{exact:true})).toHaveValue('owned-ui-billing');await expect(page.getByLabel('Group free policy',{exact:true})).toHaveValue('allow');await page.getByRole('button',{name:'Cancel',exact:true}).click();
 await page.getByRole('button',{name:'Config',exact:true}).click();await page.getByRole('button',{name:'Source servers',exact:true}).first().click();await edit(page);await page.getByLabel('Group free policy',{exact:true}).selectOption('inherit');await page.getByRole('button',{name:'Stage changes',exact:true}).click();
 await page.getByRole('button',{name:'Validate',exact:true}).click();await expect(page.locator('.notice[role=status]')).toContainText('Saved state has not changed');expect((await(await request.get(uri,{headers:headers()})).json()).group_config.free.on_play).toBe('true');
 await page.getByRole('button',{name:'Save & apply',exact:true}).click();await expect(page.locator('.notice[role=status]')).toContainText('saved and applied');saved=await(await request.get(uri,{headers:headers()})).json();expect(saved.group_config.free.on_play).toBeUndefined();expect(saved.group_config.free.flussonix_token_sha256).toBe('b'.repeat(64));await expect(page.locator('textarea')).toHaveCount(0);
});

test('switching a protected vendor source to native requires explicitly clearing viewer policies',async({page,request})=>{
 expect((await request.put(uri,{headers:headers(),data:{flussonix_source_kind:'flussonic',cluster_key:'owned-vendor-private-key',on_play:'true',flussonix_token_sha256:'a'.repeat(64),group_config:{free:{on_play:'true'}}}})).ok()).toBeTruthy();
 await expect(row(page)).toBeVisible();await edit(page);await page.getByLabel('Source type',{exact:true}).selectOption('native');await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog').getByRole('alert')).toContainText('Clear vendor viewer policies');
 expect((await(await request.get(uri,{headers:headers()})).json()).flussonix_token_sha256).toBe('a'.repeat(64));
 await page.getByRole('button',{name:'Clear vendor viewer policies',exact:true}).click();await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toHaveCount(0);const saved=await(await request.get(uri,{headers:headers()})).json();expect(saved.flussonix_source_kind).toBe('native');expect(saved.on_play).toBeUndefined();expect(saved.group_config).toBeUndefined();expect(saved.flussonix_token_sha256).toBeUndefined();expect(saved.cluster_key).toBe('owned-vendor-private-key');
});

test('native settings are retained until explicitly removed for vendor conversion',async({page,request})=>{
 expect((await request.put(uri,{headers:headers(),data:{cluster_key:'owned-vendor-private-key',public_payload_url:'https://delivery.example',flussonix_source_group:'owned-replicas'}})).ok()).toBeTruthy();
 await expect(row(page)).toBeVisible();await edit(page);await page.getByLabel('Source type',{exact:true}).selectOption('flussonic');await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog').getByRole('alert')).toContainText('unsupported native settings');
 expect((await(await request.get(uri,{headers:headers()})).json()).public_payload_url).toBe('https://delivery.example');
 await page.getByRole('button',{name:'Remove unsupported native settings',exact:true}).click();await page.getByLabel('Viewer policy',{exact:true}).selectOption('allow');await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toHaveCount(0);const saved=await(await request.get(uri,{headers:headers()})).json();expect(saved.on_play).toBe('true');expect(saved.public_payload_url).toBeUndefined();expect(saved.flussonix_source_group).toBeUndefined();
});

test('invalid group guards and callback policies cannot be saved',async({page,request})=>{
 await page.getByRole('button',{name:'Add source',exact:true}).click();await page.getByLabel('Source type',{exact:true}).selectOption('flussonic');await page.getByLabel('Source address',{exact:true}).fill(source);await page.getByLabel('Cluster key',{exact:true}).fill('owned-vendor-private-key');await page.getByLabel('Viewer policy',{exact:true}).selectOption('allow');
 await page.getByLabel('New group name',{exact:true}).fill('paid');await page.getByRole('button',{name:'Add group override',exact:true}).click();await page.getByLabel('Group paid token SHA256',{exact:true}).fill('invalid');await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog').getByRole('alert')).toContainText('64 hexadecimal');expect((await request.get(uri,{headers:headers()})).status()).toBe(404);
 await page.getByLabel('Group paid token SHA256',{exact:true}).fill('c'.repeat(64));await page.getByLabel('Group paid policy',{exact:true}).selectOption('url');await page.getByLabel('Group paid callback URL',{exact:true}).fill('ftp://callback.example');await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog').getByRole('alert')).toContainText('HTTP or HTTPS');expect((await request.get(uri,{headers:headers()})).status()).toBe(404);
});

test('valid ordered repeated session keys survive unrelated vendor edits',async({page,request})=>{
 const policy={url:'https://billing.example/check',max_sessions:3,session_keys:['name','proto','token','ip','token']};
 expect((await request.put(uri,{headers:headers(),data:{flussonix_source_kind:'flussonic',cluster_key:'owned-vendor-private-key',on_play:policy}})).ok()).toBeTruthy();
 await expect(row(page)).toBeVisible();await edit(page);await page.getByLabel('Cluster key',{exact:true}).fill('owned-updated-private-key');await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toHaveCount(0);
 expect((await(await request.get(uri,{headers:headers()})).json()).on_play).toEqual(policy);
 await edit(page);await page.getByText('Viewer session identity',{exact:true}).click();await page.getByRole('button',{name:'Move viewer session key 5 up',exact:true}).click();await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toHaveCount(0);expect((await(await request.get(uri,{headers:headers()})).json()).on_play.session_keys).toEqual(['name','proto','token','token','ip']);
});

test('vendor TLS trust stays separate for management and private media',async({page,request})=>{
 const ca=process.env.FLUSSONIX_TEST_CA_FILE!;expect(ca).toBeTruthy();
 expect((await request.put(uri,{headers:headers(),data:{flussonix_source_kind:'flussonic',cluster_key:'owned-vendor-private-key',private_payload_url:'https://owned-ui-media.invalid',on_play:'true'}})).ok()).toBeTruthy();
 await expect(row(page)).toBeVisible();await edit(page);await page.getByLabel('Management trusted CA file',{exact:true}).fill(ca);await page.getByLabel('Private media trusted CA file',{exact:true}).fill(ca);await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toHaveCount(0);
 await edit(page);await page.getByLabel('Management URL',{exact:true}).fill('http://owned-ui-control.invalid');await expect(page.getByLabel('Management trusted CA file',{exact:true})).toHaveCount(0);await expect(page.getByLabel('Private media trusted CA file',{exact:true})).toHaveValue(ca);await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toHaveCount(0);
 const saved=await(await request.get(uri,{headers:headers()})).json();expect(saved.flussonix_tls_ca).toBeUndefined();expect(saved.flussonix_media_tls_ca).toBe(ca);expect(saved.private_payload_url).toBe('https://owned-ui-media.invalid');expect(saved.on_play).toBe('true');
});

test('long vendor group names fit mobile editor without horizontal overflow',async({page,request})=>{
 const group='g'.repeat(180);
 expect((await request.put(uri,{headers:headers(),data:{flussonix_source_kind:'flussonic',cluster_key:'owned-vendor-private-key',on_play:'true',group_config:{[group]:{flussonix_token_sha256:'a'.repeat(64)}}}})).ok()).toBeTruthy();
 await expect(row(page)).toBeVisible();await edit(page);await page.setViewportSize({width:393,height:852});await page.getByText('Group '+group,{exact:true}).scrollIntoViewIfNeeded();
 expect(await page.getByRole('dialog').evaluate(e=>e.scrollWidth>e.clientWidth)).toBe(false);
 await page.screenshot({path:'../.runtime/screenshots/vendor-groups-mobile.png',fullPage:true});
});
test('vendor processing template uses readable Media settings and supports staged removal',async({page,request})=>{
 await page.getByRole('button',{name:/^Media/}).click();await page.getByRole('button',{name:'Templates',exact:true}).click();await page.getByRole('button',{name:'Add template',exact:true}).click();await page.getByLabel('Template name',{exact:true}).fill('owned-ui-processing');await page.getByLabel('Input URL',{exact:true}).fill('http://owned-template-input.invalid');await page.getByLabel('Transcoding',{exact:true}).selectOption('copy');await page.getByLabel('Audio encoding',{exact:true}).selectOption('aac');await page.getByLabel('Audio bitrate (kb/s)',{exact:true}).fill('96');await page.getByLabel('HLS subtitles',{exact:true}).selectOption('drop');await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toHaveCount(0);
 await sources(page);await page.getByRole('button',{name:'Add source',exact:true}).click();await page.getByLabel('Source type',{exact:true}).selectOption('flussonic');await page.getByLabel('Source address',{exact:true}).fill(source);await page.getByLabel('Cluster key',{exact:true}).fill('owned-vendor-private-key');await page.getByLabel('Viewer policy',{exact:true}).selectOption('allow');await page.getByLabel('Processing template',{exact:true}).selectOption('owned-ui-processing');
 const processing=page.getByRole('region',{name:'Vendor output processing'});await expect(processing).toContainText('AAC');await expect(processing).toContainText('Filter out HLS subtitles');await expect(processing).not.toContainText('owned-template-input.invalid');await expect(processing.locator('textarea')).toHaveCount(0);await processing.scrollIntoViewIfNeeded();await page.screenshot({path:'../.runtime/screenshots/vendor-processing-desktop.png',fullPage:true});await page.setViewportSize({width:393,height:852});await page.getByLabel('Processing template',{exact:true}).scrollIntoViewIfNeeded();expect(await page.getByRole('dialog').evaluate(e=>e.scrollWidth>e.clientWidth)).toBe(false);await page.screenshot({path:'../.runtime/screenshots/vendor-processing-mobile.png',fullPage:true});await page.setViewportSize({width:1280,height:720});await page.getByRole('button',{name:'Save',exact:true}).click();await expect(row(page)).toBeVisible();expect((await(await request.get(uri,{headers:headers()})).json()).flussonix_output_template).toBe('owned-ui-processing');
 await edit(page);await expect(page.getByLabel('Processing template',{exact:true})).toHaveValue('owned-ui-processing');await page.getByRole('button',{name:'Cancel',exact:true}).click();
 await page.getByRole('button',{name:'Config',exact:true}).click();await page.getByRole('button',{name:'Source servers',exact:true}).first().click();await edit(page);await page.getByLabel('Processing template',{exact:true}).selectOption('');await page.getByRole('button',{name:'Stage changes',exact:true}).click();await page.getByRole('button',{name:'Validate',exact:true}).click();await expect(page.locator('.notice[role=status]')).toContainText('Saved state has not changed');expect((await(await request.get(uri,{headers:headers()})).json()).flussonix_output_template).toBe('owned-ui-processing');await page.getByRole('button',{name:'Save & apply',exact:true}).click();await expect(page.locator('.notice[role=status]')).toContainText('saved and applied');expect((await(await request.get(uri,{headers:headers()})).json()).flussonix_output_template).toBeUndefined();
});
test('native conversion preserves processing selection until explicitly cleared',async({page,request})=>{
 expect((await request.put('/streamer/api/v3/templates/owned-ui-processing',{headers:headers(),data:{transcoder:{acodec:'aac',ab:96}}})).ok()).toBeTruthy();
 expect((await request.put(uri,{headers:headers(),data:{flussonix_source_kind:'flussonic',cluster_key:'owned-vendor-private-key',on_play:'true',flussonix_output_template:'owned-ui-processing'}})).ok()).toBeTruthy();await expect(row(page)).toBeVisible();await edit(page);await page.getByLabel('Source type',{exact:true}).selectOption('native');await page.getByRole('button',{name:'Clear vendor viewer policies',exact:true}).click();await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog').getByRole('alert')).toContainText('Clear vendor processing template');expect((await(await request.get(uri,{headers:headers()})).json()).flussonix_output_template).toBe('owned-ui-processing');
 await page.getByRole('button',{name:'Clear vendor processing template',exact:true}).click();await page.getByRole('button',{name:'Save',exact:true}).click();await expect(page.getByRole('dialog')).toHaveCount(0);const saved=await(await request.get(uri,{headers:headers()})).json();expect(saved.flussonix_output_template).toBeUndefined();expect(saved.flussonix_source_kind).toBe('native');expect(saved.cluster_key).toBe('owned-vendor-private-key');
});
test('vendor processing selection shows the existing GPU dependency readiness',async({page,request})=>{
 expect((await request.put('/streamer/api/v3/templates/owned-ui-processing',{headers:headers(),data:{transcoder:{encoder:'h264_nvenc',vb:300}}})).ok()).toBeTruthy();
 expect((await request.put(uri,{headers:headers(),data:{flussonix_source_kind:'flussonic',cluster_key:'owned-vendor-private-key',on_play:'true',flussonix_output_template:'owned-ui-processing'}})).ok()).toBeTruthy();
 await expect(row(page)).toBeVisible();await edit(page);const readiness=page.getByRole('region',{name:'GPU transcoding readiness'});await expect(readiness).toBeVisible();
 const caps=await(await request.get('/flussonix/api/v1/capabilities',{headers:headers()})).json();const profile=caps.transcoding.gpu_profiles.find((p:{encoder:string})=>p.encoder==='h264_nvenc');expect(profile).toBeTruthy();
 await expect(readiness.getByTestId('gpu-h264_nvenc')).toContainText(profile.status==='available'?'Ready for GPU encoding':'GPU encoding unavailable');await expect(readiness).toContainText('Initialization does not measure streaming capacity.');await expect(page.locator('textarea')).toHaveCount(0);
});
