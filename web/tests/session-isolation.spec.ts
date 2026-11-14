import {expect,test} from '@playwright/test';

test('signing in as View cannot reopen an earlier Edit user’s staged secrets',async({page})=>{
 const secret='owned-editor-cluster-key';
 const source='https://owned-origin.example';
 const viewerAuth='Basic '+Buffer.from('viewer:view-pass').toString('base64');
 let viewerConfigReads=0;
 await page.route('**/flussonix/api/v1/**',async route=>{
  const pathname=new URL(route.request().url()).pathname;
  if(pathname.endsWith('/node'))return route.fulfill({json:{name:'owned-node',role:'standalone',management_role:route.request().headers()['authorization']===viewerAuth?'view':'edit'}});
  if(pathname.endsWith('/capabilities'))return route.fulfill({json:{}});
  return route.fulfill({status:404,json:{errors:[{message:'unknown fixture route'}]}});
 });
 await page.route('**/streamer/api/v3/**',async route=>{
  const pathname=new URL(route.request().url()).pathname;
  if(pathname.endsWith('/config')){
   const isViewer=route.request().headers()['authorization']===viewerAuth;
   if(isViewer)viewerConfigReads++;
   return route.fulfill({json:{server_id:'owned-node',streams:[],templates:[],peers:[],auth_backends:[],sources:[{url:source,flussonix_source_kind:'flussonic',cluster_key:isViewer?'[redacted]':secret}]}});
  }
  if(pathname.endsWith('/templates'))return route.fulfill({json:{templates:[]}});
  if(pathname.endsWith('/auth_backends'))return route.fulfill({json:{auth_backends:[]}});
  if(pathname.endsWith('/streams'))return route.fulfill({json:{streams:[]}});
  return route.fulfill({status:404,json:{errors:[{message:'unknown fixture route'}]}});
 });
 const signIn=async(user:string,password:string)=>{
  await page.getByLabel('Username').fill(user);
  await page.getByLabel('Password',{exact:true}).fill(password);
  await page.getByRole('button',{name:'Sign in',exact:true}).click();
 };
 await page.goto('/admin/');
 await signIn('admin','edit-pass');
 await page.getByRole('button',{name:'Config',exact:true}).click();
 await page.getByRole('button',{name:'Source servers',exact:true}).first().click();
 await expect(page.getByRole('row').filter({hasText:source})).toBeVisible();
 await page.getByRole('button',{name:'Streams',exact:true}).first().click();
 await page.getByRole('button',{name:'Add stream',exact:true}).click();
 await page.getByLabel('Stream name',{exact:true}).fill('staged-only');
 await page.getByLabel('Input URL',{exact:true}).fill('testsrc://');
 await page.getByRole('button',{name:'Stage changes',exact:true}).click();
 await page.getByRole('button',{name:'Sign out',exact:true}).click();
 await signIn('viewer','view-pass');
 await page.getByRole('button',{name:'Config',exact:true}).click();
 await expect.poll(()=>viewerConfigReads).toBeGreaterThan(0);
 await page.getByRole('button',{name:'Source servers',exact:true}).first().click();
 const sourceRow=page.getByRole('row').filter({hasText:source});
 await expect(sourceRow).toBeVisible();
 await expect(sourceRow.getByRole('button',{name:'Edit',exact:true})).toHaveCount(0);
 await expect(sourceRow.getByRole('button',{name:'Remove',exact:true})).toHaveCount(0);
 await expect(page.locator('body')).not.toContainText(secret);
});

test('a late unauthorized response from a prior login cannot sign out the next user',async({page})=>{
 const editorAuth='Basic '+Buffer.from('admin:edit-pass').toString('base64');
 let releaseOldRequest:()=>void=()=>{};
 let markOldRequestSeen:()=>void=()=>{};
 const oldRequestSeen=new Promise<void>(resolve=>{markOldRequestSeen=resolve});
 const oldRequestReleased=new Promise<void>(resolve=>{releaseOldRequest=resolve});
 await page.route('**/flussonix/api/v1/**',route=>route.fulfill({json:{name:'owned-node',role:'standalone'}}));
 await page.route('**/streamer/api/v3/**',async route=>{
  const pathname=new URL(route.request().url()).pathname;
  if(pathname.endsWith('/templates')&&route.request().headers()['authorization']===editorAuth){
   markOldRequestSeen();
   await oldRequestReleased;
   return route.fulfill({status:401,json:{errors:[{message:'expired'}]}});
  }
  const key=pathname.endsWith('/auth_backends')?'auth_backends':pathname.endsWith('/templates')?'templates':'streams';
  return route.fulfill({json:{[key]:[]}});
 });
 const signIn=async(user:string,password:string)=>{
  await page.getByLabel('Username').fill(user);
  await page.getByLabel('Password',{exact:true}).fill(password);
  await page.getByRole('button',{name:'Sign in',exact:true}).click();
 };
 await page.goto('/admin/');
 await signIn('admin','edit-pass');
 await oldRequestSeen;
 await page.getByRole('button',{name:'Sign out',exact:true}).click();
 await signIn('viewer','view-pass');
 await expect(page.getByRole('button',{name:'Sign out',exact:true})).toBeVisible();
 const oldResponse=page.waitForResponse(response=>response.url().endsWith('/streamer/api/v3/templates')&&response.status()===401);
 releaseOldRequest();
 await oldResponse;
 await page.waitForTimeout(100);
 await expect(page.getByRole('button',{name:'Sign out',exact:true})).toBeVisible();
});
