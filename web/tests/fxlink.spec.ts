import {test,expect} from '@playwright/test';
test.beforeEach(async({page})=>{
 await page.goto('/admin/');await page.getByLabel('Username').fill(process.env.FLUSSONIX_ADMIN_USER||'admin');
 await page.getByLabel('Password',{exact:true}).fill(process.env.FLUSSONIX_ADMIN_PASSWORD!);
 await page.getByRole('button',{name:'Sign in',exact:true}).click();
 await expect(page.getByRole('heading',{name:'Streams',exact:true})).toBeVisible();
});
test('FXLink controls are normal fields and preserve separate peer and viewer credentials',async({page,request})=>{
 const name='ui-fxlink-'+Date.now();
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 try {
  await page.getByRole('button',{name:'Add stream',exact:true}).click();
  await page.getByLabel('Stream name',{exact:true}).fill(name);
  await page.getByLabel('Activation',{exact:true}).selectOption('ondemand');
  await page.getByLabel('Input URL',{exact:true}).fill('fxlink://localhost:55000/group/news');
  await page.getByLabel('FXLink peer key',{exact:true}).fill('owned-ui-native-peer');
  await page.getByLabel('FXLink viewer token',{exact:true}).fill('owned-ui-viewer-token');
  await page.getByLabel('Transport buffer (ms)',{exact:true}).fill('1200');
  await page.getByLabel('FEC protection',{exact:true}).selectOption('20:6');
  await expect(page.locator('textarea')).toHaveCount(0);
  await page.getByLabel('FEC protection',{exact:true}).scrollIntoViewIfNeeded();
  await page.screenshot({path:test.info().outputPath('fxlink-form.png'),fullPage:true});
  await page.getByRole('button',{name:'Save',exact:true}).click();
  await expect(page.getByRole('heading',{name,exact:true})).toBeVisible();
  const response=await request.get('/streamer/api/v3/streams/'+name,{headers});expect(response.ok()).toBeTruthy();
  const data=(await response.json()).config_on_disk;
  expect(data.flussonix_peer_key).toBe('owned-ui-native-peer');
  expect(data.inputs[0]).toEqual({url:'fxlink://localhost:55000/group/news',flussonix_fxlink_token:'owned-ui-viewer-token',flussonix_fxlink:{latency_ms:1200,data_shards:20,parity_shards:6}});
  await expect(page.getByText('owned-ui-viewer-token',{exact:false})).toHaveCount(0);
  await page.getByRole('button',{name:'Edit stream',exact:true}).click();
  await page.getByLabel('Input URL',{exact:true}).fill('m4s://localhost/news?token=legacy');
  await expect(page.getByLabel('FEC protection',{exact:true})).toHaveCount(0);
  await expect(page.getByLabel('FXLink viewer token',{exact:true})).toHaveCount(0);
  await page.getByRole('button',{name:'Save',exact:true}).click();
  const legacy=(await(await request.get('/streamer/api/v3/streams/'+name,{headers})).json()).config_on_disk;
  expect(legacy.inputs).toEqual([{url:'m4s://localhost/news?token=legacy'}]);
 } finally {await request.delete('/streamer/api/v3/streams/'+name,{headers});}
});
test('Config displays FXLink listener guidance without a JSON field',async({page,request})=>{
 const headers={Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')};
 const node=await(await request.get('/flussonix/api/v1/node',{headers})).json();
 await page.getByRole('button',{name:'Config',exact:true}).click();
 const info=page.getByRole('region',{name:'FXLink transport',exact:true});
 await expect(info).toContainText(node.fxlink?.listen||'Not enabled');
 await expect(info).toContainText('TLS + Reed–Solomon FEC');
 await expect(info).toContainText('startup options');
 await expect(info.locator('textarea,input')).toHaveCount(0);
});
test('FXLink private CA remains separate when the management URL changes',async({page})=>{
 await page.getByRole('button',{name:'Cluster',exact:true}).click();
 await page.getByRole('button',{name:'Source servers',exact:true}).click();
 await page.getByRole('button',{name:'Add source',exact:true}).click();
 await page.getByLabel('Node name',{exact:true}).fill('owned-ui-fxlink-source');
 await page.getByLabel('Management URL',{exact:true}).fill('http://localhost:19998');
 await page.getByLabel('Source transport',{exact:true}).selectOption('fxlink');
 await page.getByLabel('FXLink UDP endpoint',{exact:true}).fill('fxlink://localhost:55000');
 const ca=page.getByLabel('Private media trusted CA file',{exact:true});
 await ca.fill('/owned/ui-only-private-ca.pem');
 await page.getByLabel('Management URL',{exact:true}).fill('http://localhost:19997');
 await expect(ca).toHaveValue('/owned/ui-only-private-ca.pem');
 await expect(page.getByLabel('FXLink UDP endpoint',{exact:true})).toHaveValue('fxlink://localhost:55000');
 await page.getByLabel('Source transport',{exact:true}).selectOption('m4s');
 await expect(page.getByLabel('FXLink UDP endpoint',{exact:true})).toHaveCount(0);
 await expect(ca).toHaveCount(0);
 await page.getByRole('button',{name:'Cancel',exact:true}).click();
});
