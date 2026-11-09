import {expect,test} from '@playwright/test';

const stream='ui-landscape-news';
const peer='ui-landscape-cdn';
const headers=()=>({Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')});

test.beforeEach(async({page,request})=>{
 const created=await request.put('/streamer/api/v3/streams/'+stream,{headers:headers(),data:{$reset:true,static:false,inputs:[{url:'publish://'}]}});
 expect(created.ok()).toBeTruthy();
 const registered=await request.put('/streamer/api/v3/cluster/peers/'+peer,{headers:headers(),data:{hostname:peer,api_url:'http://127.0.0.1:1',public_payload_url:'http://127.0.0.1:1'}});
 expect(registered.ok()).toBeTruthy();
 await page.setViewportSize({width:568,height:375});
 await page.goto('/admin/');
 await page.getByLabel('Username').fill(process.env.FLUSSONIX_ADMIN_USER||'admin');
 await page.getByLabel('Password',{exact:true}).fill(process.env.FLUSSONIX_ADMIN_PASSWORD!);
 await page.getByRole('button',{name:'Sign in',exact:true}).click();
 await expect(page.getByRole('button',{name:stream})).toBeVisible();
});

test.afterEach(async({request})=>{
 await request.delete('/streamer/api/v3/cluster/peers/'+peer,{headers:headers()});
 await request.delete('/streamer/api/v3/streams/'+stream,{headers:headers()});
});

test('landscape Streams and Cluster expose hidden columns and metrics',async({page})=>{
 for(const width of [568,667]){
  await page.setViewportSize({width,height:375});
  await expect(page.getByRole('button',{name:stream})).toBeVisible();
  const strip=page.locator('.status-strip');
  expect(await strip.evaluate(element=>element.scrollWidth>element.clientWidth)).toBe(true);
  expect(await strip.evaluate(element=>getComputedStyle(element,'::after').content)).toContain('→');
  const streams=page.locator('.mobile-cue-streams');
  expect(await streams.evaluate(element=>element.scrollWidth>element.clientWidth)).toBe(true);
  expect(await streams.evaluate(element=>getComputedStyle(element,'::before').content)).toContain('Swipe');
  await page.getByRole('button',{name:'Cluster',exact:true}).first().click();
  await page.getByRole('textbox',{name:'Search'}).fill(peer);
  await expect(page.getByRole('cell',{name:peer})).toBeVisible();
  const cluster=page.locator('.mobile-cue-cluster-table');
  expect(await cluster.evaluate(element=>element.scrollWidth>element.clientWidth)).toBe(true);
  expect(await cluster.evaluate(element=>getComputedStyle(element,'::before').content)).toContain('Swipe');
  expect(await page.evaluate(()=>document.documentElement.scrollWidth-innerWidth)).toBe(0);
  await page.getByRole('button',{name:'Media',exact:false}).first().click();
  await page.getByRole('textbox',{name:'Search'}).fill(stream);
  await expect(page.getByRole('button',{name:stream})).toBeVisible();
 }
});

test('landscape cues disappear when their content fits',async({page})=>{
 await page.setViewportSize({width:684,height:375});
 await page.getByRole('button',{name:'Cluster',exact:true}).first().click();
 await page.getByRole('textbox',{name:'Search'}).fill(peer);
 await expect(page.getByRole('cell',{name:peer})).toBeVisible();
 const cluster=page.locator('.mobile-cue-cluster-table');
 expect(await cluster.evaluate(element=>element.scrollWidth<=element.clientWidth)).toBe(true);
 expect(await cluster.evaluate(element=>getComputedStyle(element,'::before').content)).toBe('none');
 await page.setViewportSize({width:776,height:375});
 const strip=page.locator('.status-strip');
 const withoutCue=await page.addStyleTag({content:'.status-strip::after{content:none!important}'});
 expect(await strip.evaluate(element=>element.scrollWidth>element.clientWidth)).toBe(true);
 await withoutCue.evaluate(element=>element.remove());
 expect(await strip.evaluate(element=>getComputedStyle(element,'::after').content)).toContain('→');
 await page.setViewportSize({width:777,height:375});
 expect(await strip.evaluate(element=>element.scrollWidth<=element.clientWidth)).toBe(true);
 expect(await strip.evaluate(element=>getComputedStyle(element,'::after').content)).toBe('none');
 await page.setViewportSize({width:812,height:375});
 expect(await strip.evaluate(element=>element.scrollWidth<=element.clientWidth)).toBe(true);
 expect(await strip.evaluate(element=>getComputedStyle(element,'::after').content)).toBe('none');
 await page.getByRole('button',{name:'Media',exact:false}).first().click();
 await page.getByRole('textbox',{name:'Search'}).fill(stream);
 await expect(page.getByRole('button',{name:stream})).toBeVisible();
 const streams=page.locator('.mobile-cue-streams');
 expect(await streams.evaluate(element=>element.scrollWidth<=element.clientWidth)).toBe(true);
 expect(await streams.evaluate(element=>getComputedStyle(element,'::before').content)).toBe('none');
});
