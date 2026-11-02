import {expect, test} from '@playwright/test';

test('Input tab selects the second configured input without saving secrets', async ({page, request, browser}) => {
 const user = process.env.FLUSSONIX_ADMIN_USER || 'admin';
 const password = process.env.FLUSSONIX_ADMIN_PASSWORD!;
 const headers = {Authorization: 'Basic ' + Buffer.from(`${user}:${password}`).toString('base64')};
 const name = 'ui-input-select-' + Date.now().toString(36);
 const streamURL = `/streamer/api/v3/streams/${name}`;
 const selectionURL = `${streamURL}/inputs/1/select`;
 const created = await request.put(streamURL, {headers, data: {$reset:true, static:false, inputs:[
  {url:'tshttp://feeduser:feedpass@127.0.0.1:1/unused'},
  {url:'testsrc://'},
 ]}});
 expect(created.ok()).toBeTruthy();
 try {
  await page.goto('/admin/');
  await page.getByLabel('Username').fill(user);
  await page.getByLabel('Password', {exact:true}).fill(password);
  await page.getByRole('button', {name:'Sign in', exact:true}).click();
  await page.getByRole('button', {name, exact:true}).click();
  await page.getByRole('button', {name:'Input', exact:true}).click();
  const selector = page.getByRole('region', {name:'Stream input selection'});
  await expect(selector).toContainText('Input 2');
  await expect(selector).not.toContainText('feedpass');
  const response = page.waitForResponse(response => response.url().endsWith(selectionURL) && response.request().method() === 'POST');
  await selector.getByRole('button', {name:'Select input 2'}).click();
  const selectedResponse = await response;
  expect(selectedResponse.status()).toBe(204);
  expect(selectedResponse.request().postData()).toBeNull();
  await expect(selector).toContainText('Pending input 2');
  const current = await (await request.get(streamURL, {headers})).json();
  expect(current.stats.preferred_input_index).toBe(1);
  expect(current.stats.input_index).toBeUndefined();

  const viewContext = await browser.newContext({baseURL:process.env.FLUSSONIX_TEST_URL});
  try {
   const viewPage = await viewContext.newPage();
   await viewPage.goto('/admin/');
   await viewPage.getByLabel('Username').fill(process.env.FLUSSONIX_VIEW_USER || 'viewer');
   await viewPage.getByLabel('Password', {exact:true}).fill(process.env.FLUSSONIX_VIEW_PASSWORD!);
   await viewPage.getByRole('button', {name:'Sign in', exact:true}).click();
   await viewPage.getByRole('button', {name, exact:true}).click();
   await viewPage.getByRole('button', {name:'Input', exact:true}).click();
   const viewSelector = viewPage.getByRole('region', {name:'Stream input selection'});
   await expect(viewSelector).toContainText('Input 2');
   await expect(viewSelector.getByRole('button', {name:/Select input/})).toHaveCount(0);
  } finally {
   await viewContext.close();
  }
 } finally {
  await request.delete(streamURL, {headers});
 }
});
