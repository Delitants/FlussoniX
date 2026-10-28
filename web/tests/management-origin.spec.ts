import {expect, test} from '@playwright/test';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {guardedManagementFetch, managementOriginAllowed} from '../src/management_origin';

const dist = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../dist');

async function serveBundle(page: import('@playwright/test').Page, origin: string) {
  await page.route(`${origin}/**`, async route => {
    const pathname = new URL(route.request().url()).pathname;
    const relative = pathname === '/admin/' ? 'index.html' : pathname.replace(/^\/admin\//, '');
    const file = path.join(dist, relative);
    if (!fs.existsSync(file)) {
      await route.fulfill({status: 404});
      return;
    }
    await route.fulfill({path: file});
  });
}

test('management origin accepts HTTPS and exact loopback HTTP only', () => {
  for (const [protocol, host] of [
    ['https:', 'admin.example'],
    ['http:', 'localhost'],
    ['http:', '127.0.0.1'],
    ['http:', '127.255.255.254'],
    ['http:', '[::1]'],
  ]) expect(managementOriginAllowed(protocol, host)).toBe(true);
  for (const [protocol, host] of [
    ['http:', 'admin.example'],
    ['http:', 'localhost.evil.example'],
    ['http:', '127.0.0.1.evil.example'],
    ['http:', '127.256.0.1'],
    ['http:', '128.0.0.1'],
    ['file:', 'localhost'],
  ]) expect(managementOriginAllowed(protocol, host)).toBe(false);
});

test('credential request seam rejects remote HTTP before calling fetch', async () => {
  let calls = 0;
  const fetcher = async () => {
    calls++;
    return new Response('{}', {status: 200});
  };
  await expect(guardedManagementFetch('http:', 'remote.example', '/flussonix/api/v1/node', {
    headers: {Authorization: 'Basic b3duZWQ6c2VjcmV0'},
  }, fetcher)).rejects.toThrow('Remote management requires HTTPS');
  expect(calls).toBe(0);
  await expect(guardedManagementFetch('http:', '127.0.0.1', '/flussonix/api/v1/node', {}, fetcher)).resolves.toBeTruthy();
  expect(calls).toBe(1);
});

test('remote HTTP admin page does not offer a credential form', async ({page}) => {
  await serveBundle(page, 'http://remote.example');
  let credentialRequests = 0;
  page.on('request', request => {if (request.headers().authorization) credentialRequests++;});
  await page.goto('http://remote.example/admin/');
  await expect(page.getByRole('heading', {name: 'Remote management requires HTTPS'})).toBeVisible();
  await expect(page.getByLabel('Password', {exact: true})).toHaveCount(0);
  await expect(page.getByRole('button', {name: 'Sign in'})).toHaveCount(0);
  expect(credentialRequests).toBe(0);
});

test('loopback HTTP admin page still offers login', async ({page}) => {
  await serveBundle(page, 'http://127.0.0.1:19999');
  await page.goto('http://127.0.0.1:19999/admin/');
  await expect(page.getByRole('heading', {name: 'Media control'})).toBeVisible();
  await expect(page.getByLabel('Password', {exact: true})).toBeVisible();
});
