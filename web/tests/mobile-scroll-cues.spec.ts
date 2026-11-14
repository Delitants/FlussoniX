import {expect,test} from '@playwright/test';

const stream='ui320-news';
const headers=()=>({Authorization:'Basic '+Buffer.from((process.env.FLUSSONIX_ADMIN_USER||'admin')+':'+process.env.FLUSSONIX_ADMIN_PASSWORD).toString('base64')});

test.beforeEach(async({page,request})=>{
 const created=await request.put('/streamer/api/v3/streams/'+stream,{headers:headers(),data:{$reset:true,static:false,inputs:[{url:'publish://'}]}});
 expect(created.ok()).toBeTruthy();
 await page.setViewportSize({width:320,height:760});
 await page.goto('/admin/');
 await page.getByLabel('Username').fill(process.env.FLUSSONIX_ADMIN_USER||'admin');
 await page.getByLabel('Password',{exact:true}).fill(process.env.FLUSSONIX_ADMIN_PASSWORD!);
 await page.getByRole('button',{name:'Sign in',exact:true}).click();
 await expect(page.getByRole('button',{name:stream})).toBeVisible();
});

test.afterEach(async({request})=>{await request.delete('/streamer/api/v3/streams/'+stream,{headers:headers()});});

test('320px Streams table offers a cue and keyboard access to hidden columns',async({page})=>{
 const table=page.locator('.panel.table-wrap').first();
 expect(await table.evaluate(element=>element.scrollWidth>element.clientWidth)).toBe(true);
 expect(await table.evaluate(element=>getComputedStyle(element,'::before').content)).toContain('Swipe');
 await expect(table).toHaveAttribute('tabindex','0');
 await expect(table).toHaveAttribute('aria-label','Streams table; use left and right arrows for more columns');
 await table.focus();
 await page.keyboard.press('ArrowRight');
 await expect.poll(()=>table.evaluate(element=>element.scrollLeft)).toBeGreaterThan(0);
 const twoTabBar=page.locator('.tabs').first();
 expect(await twoTabBar.evaluate(element=>element.scrollWidth<=element.clientWidth)).toBe(true);
 expect(await twoTabBar.evaluate(element=>getComputedStyle(element,'::after').content)).toBe('none');
});

test('320px status strip and Cluster tabs indicate more content to the right',async({page})=>{
 const strip=page.locator('.status-strip');
 expect(await strip.evaluate(element=>element.scrollWidth>element.clientWidth)).toBe(true);
 expect(await strip.evaluate(element=>getComputedStyle(element,'::after').content)).toContain('→');
 await expect(strip).toHaveAttribute('tabindex','0');
 await expect(strip).toHaveAttribute('aria-label','Node metrics; use left and right arrows for more metrics');
 await page.getByRole('button',{name:'Cluster',exact:true}).first().click();
 const tabs=page.locator('.tabs').first();
 expect(await tabs.evaluate(element=>element.scrollWidth>element.clientWidth)).toBe(true);
 await expect.poll(()=>tabs.evaluate(element=>getComputedStyle(element,'::after').content)).toContain('→');
 await expect(page.getByRole('region',{name:'Cluster table; use left and right arrows for more columns'})).toBeVisible();
});

test('320px stream editor wraps long help text without sideways scrolling',async({page})=>{
 await page.getByRole('button',{name:'Add stream',exact:true}).click();
 const dialog=page.getByRole('dialog');
 await expect(dialog).toBeVisible();
 expect(await dialog.evaluate(element=>element.scrollWidth-element.clientWidth)).toBeLessThanOrEqual(1);
});

test('Configuration tabs wrap without a false horizontal-scroll cue',async({page})=>{
 await page.getByRole('button',{name:'Config',exact:true}).first().click();
 const tabs=page.locator('.mobile-config-tabs');
 await expect(tabs).toBeVisible();
 expect(await tabs.evaluate(element=>element.scrollWidth<=element.clientWidth)).toBe(true);
 expect(await tabs.evaluate(element=>getComputedStyle(element,'::after').content)).toBe('none');
});

test('Cluster tabs do not show an arrow when they fit at wider mobile width',async({page})=>{
 await page.setViewportSize({width:700,height:760});
 await page.getByRole('button',{name:'Cluster',exact:true}).first().click();
 const tabs=page.locator('.tabs').first();
 await expect.poll(()=>tabs.evaluate(element=>element.scrollWidth<=element.clientWidth)).toBe(true);
 await expect.poll(()=>tabs.evaluate(element=>getComputedStyle(element,'::after').content)).toBe('none');
});

test('Cluster tab cue follows content width when labels become wider',async({page})=>{
 await page.setViewportSize({width:436,height:760});
 await page.getByRole('button',{name:'Cluster',exact:true}).first().click();
 const tabs=page.locator('.mobile-cue-cluster');
 const wider=await page.addStyleTag({content:'.mobile-cue-cluster button:last-child{min-width:240px!important}'});
 await expect.poll(()=>tabs.evaluate(element=>element.scrollWidth>element.clientWidth)).toBe(true);
 await expect.poll(()=>tabs.evaluate(element=>getComputedStyle(element,'::after').content)).toContain('→');
 await wider.evaluate(element=>element.remove());
 const compact=await page.addStyleTag({content:'.mobile-cue-cluster button{font-size:9px!important}'});
 await expect.poll(()=>tabs.evaluate(element=>element.scrollWidth<=element.clientWidth)).toBe(true);
 await expect.poll(()=>tabs.evaluate(element=>getComputedStyle(element,'::after').content)).toBe('none');
 await compact.evaluate(element=>element.remove());
});

test('stream detail cue follows content width at a wider viewport',async({page})=>{
 await page.getByRole('button',{name:stream}).click();
 await page.setViewportSize({width:481,height:760});
 const tabs=page.locator('.mobile-cue-detail');
 const wider=await page.addStyleTag({content:'.mobile-cue-detail button{min-width:150px!important}'});
 await expect.poll(()=>tabs.evaluate(element=>element.scrollWidth>element.clientWidth)).toBe(true);
 await expect.poll(()=>tabs.evaluate(element=>getComputedStyle(element,'::after').content)).toContain('→');
 await wider.evaluate(element=>element.remove());
});

test('status strip cue follows content width rather than a fixed breakpoint',async({page})=>{
 const strip=page.locator('.status-strip');
 await page.setViewportSize({width:776,height:760});
 const compact=await page.addStyleTag({content:'.status-strip>span:last-child{display:none!important}'});
 await expect.poll(()=>strip.evaluate(element=>element.scrollWidth<=element.clientWidth)).toBe(true);
 await expect.poll(()=>strip.evaluate(element=>getComputedStyle(element,'::after').content)).toBe('none');
 await compact.evaluate(element=>element.remove());
 await page.setViewportSize({width:777,height:760});
 const wider=await page.addStyleTag({content:'.status-strip>span:last-child{min-width:260px!important}'});
 await expect.poll(()=>strip.evaluate(element=>element.scrollWidth>element.clientWidth)).toBe(true);
 await expect.poll(()=>strip.evaluate(element=>getComputedStyle(element,'::after').content)).toContain('→');
 await wider.evaluate(element=>element.remove());
});

test('stream detail tabs show a cue only while content is hidden',async({page})=>{
 await page.getByRole('button',{name:'ui320-news'}).click();
 const tabs=page.locator('.mobile-cue-detail');
 expect(await tabs.evaluate(element=>element.scrollWidth>element.clientWidth)).toBe(true);
 await expect.poll(()=>tabs.evaluate(element=>getComputedStyle(element,'::after').content)).toContain('→');
 await page.setViewportSize({width:481,height:760});
 const withoutCue=await page.addStyleTag({content:'.tabs.mobile-cue-detail::after{content:none!important}'});
 const overflowing=await tabs.evaluate(element=>element.scrollWidth>element.clientWidth+1);
 await withoutCue.evaluate(element=>element.remove());
 await expect.poll(()=>tabs.evaluate(element=>getComputedStyle(element,'::after').content)).toBe(overflowing?'"→"':'none');
 await page.setViewportSize({width:700,height:760});
 await expect.poll(()=>tabs.evaluate(element=>element.scrollWidth<=element.clientWidth)).toBe(true);
 await expect.poll(()=>tabs.evaluate(element=>getComputedStyle(element,'::after').content)).toBe('none');
});

test('desktop tables do not show a mobile swipe hint',async({page})=>{
 await page.setViewportSize({width:1280,height:800});
 const table=page.locator('.panel.table-wrap').first();
 expect(await table.evaluate(element=>getComputedStyle(element,'::before').content)).toBe('none');
});

test('a table shows a swipe hint only while its own columns overflow',async({page})=>{
 await page.getByRole('button',{name:'Templates',exact:true}).first().click();
 const table=page.getByRole('region',{name:'Templates table; use left and right arrows for more columns'});
 await page.setViewportSize({width:560,height:760});
 await expect.poll(()=>table.evaluate(element=>element.querySelector('table')!.scrollWidth>element.clientWidth)).toBe(false);
 await expect.poll(()=>table.evaluate(element=>getComputedStyle(element,'::before').content)).toBe('none');
 await page.setViewportSize({width:320,height:760});
 await expect.poll(()=>table.evaluate(element=>element.querySelector('table')!.scrollWidth>element.clientWidth)).toBe(true);
 await expect.poll(()=>table.evaluate(element=>getComputedStyle(element,'::before').content)).toContain('Swipe');
});
