import {expect,test} from '@playwright/test';

test.beforeEach(async({page})=>{
 await page.goto('/admin/');
 await page.getByLabel('Username').fill(process.env.FLUSSONIX_ADMIN_USER||'admin');
 await page.getByLabel('Password',{exact:true}).fill(process.env.FLUSSONIX_ADMIN_PASSWORD!);
 await page.getByRole('button',{name:'Sign in',exact:true}).click();
 await expect(page.getByRole('heading',{name:'Streams',exact:true})).toBeVisible();
});

test('stream editor contains keyboard focus and returns it to Add stream',async({page})=>{
 const opener=page.getByRole('button',{name:'Add stream',exact:true});
 await opener.click();
 const dialog=page.getByRole('dialog');
 await expect(dialog).toBeVisible();
 await expect.poll(()=>page.evaluate(()=>document.querySelector('[role="dialog"]')?.contains(document.activeElement))).toBe(true);
 expect(await page.evaluate(()=>document.getElementById('root')?.inert)).toBe(true);
 await page.keyboard.press('Tab');
 await expect.poll(()=>page.evaluate(()=>document.querySelector('[role="dialog"]')?.contains(document.activeElement))).toBe(true);
 await page.keyboard.press('Shift+Tab');
 await expect.poll(()=>page.evaluate(()=>document.querySelector('[role="dialog"]')?.contains(document.activeElement))).toBe(true);
 await page.evaluate(()=>{
  const outside=document.createElement('button');
  outside.textContent='Outside focus probe';
  document.body.append(outside);
  outside.focus();
 });
 await expect(dialog).toBeFocused();
 await page.keyboard.press('Escape');
 await expect(dialog).toHaveCount(0);
 await expect(opener).toBeFocused();
 expect(await page.evaluate(()=>document.getElementById('root')?.inert)).toBe(false);
});

test('mobile editor keeps focus during field updates and wraps both Tab directions',async({page})=>{
 await page.setViewportSize({width:390,height:844});
 await page.getByRole('button',{name:'Add stream',exact:true}).click();
 const dialog=page.getByRole('dialog');
 const name=dialog.getByLabel('Stream name',{exact:true});
 await name.fill('mobile-focus');
 await expect(name).toBeFocused();
 const close=dialog.getByRole('button',{name:'Close editor'});
 const save=dialog.getByRole('button',{name:'Save',exact:true});
 await save.focus();
 await page.keyboard.press('Tab');
 await expect(close).toBeFocused();
 await page.keyboard.press('Shift+Tab');
 await expect(save).toBeFocused();
 await save.evaluate(button=>(button as HTMLButtonElement).disabled=true);
 await close.focus();
 await page.keyboard.press('Shift+Tab');
 await expect(dialog.getByRole('button',{name:'Cancel'})).toBeFocused();
 await page.keyboard.press('Tab');
 await expect(close).toBeFocused();
 await dialog.getByRole('button',{name:'Cancel'}).click();
 await expect(dialog).toHaveCount(0);
});

test('Templates, Config and Cluster editors use the same focus boundary',async({page})=>{
 for(const [view,openerName] of [
  ['Templates','Add template'],
  ['Config','Add stream'],
  ['Cluster','Add peer'],
 ] as const){
  await page.getByRole('button',{name:view,exact:true}).first().click();
  const opener=page.getByRole('button',{name:openerName,exact:true});
  await opener.click();
  const dialog=page.getByRole('dialog');
  await expect(dialog).toBeFocused();
  await page.keyboard.press('Escape');
  await expect(dialog).toHaveCount(0);
  await expect(opener).toBeFocused();
 }
});

test('player restores focus to Open player after Escape',async({page})=>{
 await page.route('**/streamer/api/v3/streams?*',route=>route.fulfill({json:{streams:[{name:'focus-player',title:'Focus player',static:false,inputs:[],stats:{status:'waiting'}}]}}));
 await page.getByRole('button',{name:'Templates',exact:true}).click();
 await page.getByRole('button',{name:'Streams',exact:true}).click();
 await page.getByRole('button',{name:'focus-player'}).click();
 const opener=page.getByRole('button',{name:'Open player'});
 await opener.click();
 const dialog=page.getByRole('dialog',{name:'focus-player'});
 await expect(dialog).toBeFocused();
 await page.keyboard.press('Escape');
 await expect(dialog).toHaveCount(0);
 await expect(opener).toBeFocused();
});

test('IPTV package editor contains focus and restores its Add package button',async({page})=>{
 await page.getByRole('button',{name:'Config',exact:true}).click();
 await page.getByRole('button',{name:'IPTV',exact:true}).first().click();
 const opener=page.getByRole('button',{name:'Add package',exact:true});
 const enable=page.getByRole('button',{name:'Enable IPTV',exact:true});
 await expect(enable.or(opener).first()).toBeVisible();
 const wasDisabled=await enable.isVisible();
 try{
  if(wasDisabled)await enable.click();
  await opener.click();
  const dialog=page.getByRole('dialog',{name:'Add package'});
  await expect(dialog).toBeFocused();
  await dialog.getByLabel('Package name',{exact:true}).fill('focused-package');
  await expect(dialog.getByLabel('Package name',{exact:true})).toBeFocused();
  await page.keyboard.press('Escape');
  await expect(dialog).toHaveCount(0);
  await expect(opener).toBeFocused();
  await page.getByRole('button',{name:'Subscribers',exact:true}).click();
  const subscriberOpener=page.getByRole('button',{name:'Add subscriber',exact:true});
  await subscriberOpener.click();
  const subscriberDialog=page.getByRole('dialog',{name:'Add subscriber'});
  await expect(subscriberDialog).toBeFocused();
  await subscriberDialog.getByLabel('Subscriber name',{exact:true}).fill('focused-subscriber');
  await expect(subscriberDialog.getByLabel('Subscriber name',{exact:true})).toBeFocused();
  await page.keyboard.press('Escape');
  await expect(subscriberDialog).toHaveCount(0);
  await expect(subscriberOpener).toBeFocused();
 }finally{
  if(wasDisabled){
   if(await page.getByRole('dialog').count())await page.keyboard.press('Escape');
   const disable=page.getByRole('button',{name:'Disable IPTV',exact:true});
   if(await disable.isVisible()){
    await disable.click();
    await expect(enable).toBeVisible();
   }
  }
 }
});

test('sign-out with an open editor does not restore focus to a removed opener',async({page})=>{
 const opener=page.getByRole('button',{name:'Add stream',exact:true});
 await opener.click();
 await expect(page.getByRole('dialog')).toBeFocused();
 await page.evaluate(()=>{
  const signOut=Array.from(document.querySelectorAll('button')).find(button=>button.textContent?.trim()==='Sign out');
  if(!signOut)throw new Error('Sign out action missing');
  signOut.dispatchEvent(new MouseEvent('click',{bubbles:true}));
 });
 await expect(page.getByRole('dialog')).toHaveCount(0);
 await expect(opener).toHaveCount(0);
 await expect(page.getByLabel('Username')).toBeVisible();
 expect(await page.evaluate(()=>document.getElementById('root')?.inert)).toBe(false);
});

test('only the top dialog owns focus and Escape when dialogs overlap',async({page})=>{
 test.setTimeout(10000);
 await page.route('**/streamer/api/v3/streams?*',route=>route.fulfill({json:{streams:[{name:'overlap-player',title:'Overlap player',static:false,inputs:[],stats:{status:'waiting'}}]}}));
 await page.getByRole('button',{name:'Templates',exact:true}).click();
 await page.getByRole('button',{name:'Streams',exact:true}).click();
 await page.getByRole('button',{name:'overlap-player'}).click();
 const opener=page.getByRole('button',{name:'Open player'});
 await opener.click();
 await expect(page.getByRole('dialog')).toHaveCount(1);
 await page.evaluate(()=>{
  const edit=Array.from(document.querySelectorAll('button')).find(button=>button.textContent?.trim()==='Edit stream');
  if(!edit)throw new Error('Edit stream opener missing');
  edit.dispatchEvent(new MouseEvent('click',{bubbles:true}));
 });
 const dialogs=page.getByRole('dialog');
 await expect(dialogs).toHaveCount(2);
 await expect(dialogs.last()).toBeFocused();
 await page.keyboard.press('Escape');
 await expect(dialogs).toHaveCount(1);
 await expect(dialogs.first()).toBeFocused();
 await page.keyboard.press('Escape');
 await expect(dialogs).toHaveCount(0);
 await expect(opener).toBeFocused();
});
