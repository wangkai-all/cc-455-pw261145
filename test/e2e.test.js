/**
 * Browser end-to-end test.
 *
 * Runs the real UI in Chromium against the real server and drives the four
 * demo scenarios: fault-heavy download, cancel + reload + resume, live file
 * replacement, and a corrupted stored chunk repaired on resume.
 *
 * Requires a Chromium that Playwright can launch. In minimal containers point
 * LD_LIBRARY_PATH at a private sysroot's lib directories, and FONTCONFIG_PATH /
 * XDG_DATA_HOME at its font config and font data (see README).
 */

import test, { before, after } from 'node:test';
import assert from 'node:assert/strict';
import { chromium } from 'playwright';
import { server, state } from '../server/index.js';

let browser;
let base;
const pages = new Set();

before(async () => {
  await new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      base = `http://127.0.0.1:${server.address().port}`;
      resolve();
    });
  });
  browser = await chromium.launch();
});

after(async () => {
  for (const page of pages) await page.close().catch(() => {});
  await browser.close().catch(() => {});
  await new Promise((resolve) => server.close(resolve));
});

async function api(path, body) {
  const res = await fetch(`${base}${path}`, {
    method: body === undefined ? 'GET' : 'POST',
    headers: body === undefined ? {} : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return res.json().catch(() => null);
}

async function setFaults(patch) {
  return api('/api/admin/faults', { resetCounters: true, ...patch });
}

async function cleanFaults() {
  return setFaults({
    offline: false, dropRate: 0, corruptRate: 0, rangeMismatchRate: 0, staleRate: 0,
    latencyMs: 0, jitterMs: 0,
  });
}

async function openApp({ context: reuseContext } = {}) {
  // Reusing a context keeps IndexedDB, which is how a reload is simulated.
  const context = reuseContext || (await browser.newContext({ viewport: { width: 1440, height: 1000 } }));
  const page = await context.newPage();
  pages.add(page);
  page.on('pageerror', (err) => {
    throw new Error(`page error: ${err.message}`);
  });
  // `networkidle` never settles: the dashboard holds an SSE connection open.
  await page.goto(`${base}/?file=telemetry`, { waitUntil: 'load' });
  await page.locator('#dl-title', { hasText: 'telemetry-archive.bin' }).waitFor();
  return page;
}

async function selectTelemetry(page) {
  if ((await page.locator('#dl-title').innerText()) !== 'telemetry-archive.bin') {
    await page.locator('.file-tab', { hasText: 'telemetry-archive.bin' }).click();
    await page.locator('#dl-title', { hasText: 'telemetry-archive.bin' }).waitFor();
  }
}

async function waitForStatus(page, status, timeout = 120_000) {
  await page.waitForFunction(
    (s) => document.querySelector('#status-chip')?.dataset.state === s,
    status,
    { timeout },
  );
}

test('e2e: dashboard loads files, chunks and renders the grid', async () => {
  await cleanFaults();
  await api('/api/admin/files/telemetry/reset', {});
  const page = await openApp();
  assert.equal(await page.locator('.file-tab').count(), 3);
  await selectTelemetry(page);
  await page.waitForFunction(() => document.querySelectorAll('.chunk').length === 48);
  assert.equal(await page.locator('#rev-badge').innerText(), 'r1');
  assert.equal(await page.locator('#btn-start').isEnabled(), true);
  assert.equal(await page.locator('.chunk[data-status="done"]').count(), 0);
});

test('e2e: fault-heavy download completes through retries', async () => {
  await api('/api/admin/files/telemetry/reset', {});
  await setFaults({
    dropRate: 0.2, corruptRate: 0.18, rangeMismatchRate: 0.12,
    latencyMs: 15, jitterMs: 10,
  });
  const page = await openApp();
  await selectTelemetry(page);
  await page.locator('#btn-start').click();

  await page.waitForFunction(
    () => Number(document.querySelector('#retry-count').innerText.replace(/\D/g, '')) > 0,
    null,
    { timeout: 30_000 },
  );
  const retryText = await page.locator('#retry-count').innerText();
  assert.match(retryText, /[1-9]/);

  await waitForStatus(page, 'completed');
  await cleanFaults();

  const manifest = await api('/api/files/telemetry/manifest');
  assert.equal(await page.locator('#hero-percent').innerText(), '100');
  assert.match(await page.locator('#merged-hash').innerText(), new RegExp(manifest.sha256));
  assert.equal(await page.locator('.chunk[data-status="done"]').count(), 48);
  assert.equal(await page.locator('#completed-panel').isVisible(), true);

  // Failure reasons are surfaced with labels, not raw codes.
  const reasons = await page.locator('#retry-bars .rr-label').allInnerTexts();
  assert.ok(reasons.length > 0);
  for (const label of reasons) assert.ok(label.trim().length > 0 && !/^[a-z-]+$/.test(label));
});

test('e2e: pause, reload, resume reuses verified chunks', async () => {
  await cleanFaults();
  await api('/api/admin/files/telemetry/reset', {});
  await setFaults({ latencyMs: 120, jitterMs: 60 });
  const page = await openApp();
  await selectTelemetry(page);

  await page.locator('#btn-start').click();
  await page.waitForFunction(
    () => document.querySelectorAll('.chunk[data-status="done"]').length >= 4,
    null,
    { timeout: 30_000 },
  );
  await page.locator('#btn-pause').click();
  await waitForStatus(page, 'paused');
  const doneBefore = await page.locator('.chunk[data-status="done"]').count();
  assert.ok(doneBefore > 0 && doneBefore < 48);

  // Close the tab entirely and open a new one: state must come from IndexedDB.
  const context = page.context();
  await page.close();
  await cleanFaults();
  const page2 = await openApp({ context });
  await selectTelemetry(page2);
  assert.equal(await page2.locator('#resume-banner').isVisible(), true);
  const bannerText = await page2.locator('#resume-detail').innerText();
  const saved = Number(/已完成 (\d+)/.exec(bannerText)?.[1] || 0);
  assert.ok(saved >= doneBefore, `banner says ${bannerText}`);

  await page2.locator('#btn-resume').click();
  await waitForStatus(page2, 'completed');
  const manifest = await api('/api/files/telemetry/manifest');
  assert.match(await page2.locator('#merged-hash').innerText(), new RegExp(manifest.sha256));

  // The recovery notice proves stored chunks were re-hashed rather than trusted.
  const timeline = await page2.locator('#timeline').innerText();
  assert.match(timeline, /恢复校验完成/);
  const reused = Number(await page2.locator('#kpi-reused').innerText());
  assert.ok(reused > 0, 'reused chunk counter is populated');
});

test('e2e: live replacement shows version diff and prunes stale chunks', async () => {
  await cleanFaults();
  await api('/api/admin/files/telemetry/reset', {});
  await setFaults({ latencyMs: 90, jitterMs: 40 });
  const page = await openApp();
  await selectTelemetry(page);
  await page.locator('#btn-start').click();
  await page.waitForFunction(
    () => document.querySelectorAll('.chunk[data-status="done"]').length >= 8,
    null,
    { timeout: 30_000 },
  );

  await page.locator('#btn-replace').click();
  await page.waitForFunction(
    () => document.querySelector('#rev-badge')?.innerText === 'r2',
    null,
    { timeout: 30_000 },
  );
  await cleanFaults();
  await waitForStatus(page, 'completed');

  assert.equal(await page.locator('#version-content').isVisible(), true);
  assert.equal(await page.locator('#version-badge').innerText(), 'r1 → r2');
  const summary = await page.locator('#version-summary').innerText();
  assert.match(summary, /校验和一致 · 复用/);
  assert.match(summary, /内容变化 · 作废/);
  assert.ok((await page.locator('#version-strip span[data-status="same"]').count()) > 0);
  assert.ok((await page.locator('#version-strip span[data-status="changed"]').count()) > 0);

  const manifest = await api('/api/files/telemetry/manifest');
  assert.equal(manifest.revision, 2);
  assert.match(await page.locator('#merged-hash').innerText(), new RegExp(manifest.sha256));
  const history = await page.locator('#version-history-list').innerText();
  assert.match(history, /r1 → r2/);
});

test('e2e: offline exhaustion surfaces a failure, and resume recovers', async () => {
  await cleanFaults();
  await api('/api/admin/files/telemetry/reset', {});
  const page = await openApp();
  await selectTelemetry(page);
  await setFaults({ offline: true, latencyMs: 0, jitterMs: 0 });

  await page.locator('#btn-start').click();
  await waitForStatus(page, 'failed', 90_000);
  const chip = await page.locator('#failure-chip').innerText();
  assert.match(chip, /重试耗尽/);
  assert.equal(await page.locator('#failure-panel').isVisible(), true);

  await cleanFaults();
  await page.locator('#btn-start').click();
  await waitForStatus(page, 'completed');
  const manifest = await api('/api/files/telemetry/manifest');
  assert.match(await page.locator('#merged-hash').innerText(), new RegExp(manifest.sha256));
});

test('e2e: table view exposes per-chunk hashes and retry column', async () => {
  await cleanFaults();
  await api('/api/admin/files/telemetry/reset', {});
  const page = await openApp();
  await selectTelemetry(page);
  await page.locator('#btn-start').click();
  await waitForStatus(page, 'completed');

  await page.locator('.seg-btn[data-view="table"]').click();
  await page.locator('#chunk-table-wrap').waitFor({ state: 'visible' });
  assert.equal(await page.locator('#chunk-table tbody tr').count(), 48);
  const firstHash = await page.locator('#chunk-table tbody tr').first().locator('code').innerText();
  assert.match(firstHash, /^[0-9a-f]{12}…$/);
  await page.locator('.seg-btn[data-view="grid"]').click();
  await page.locator('#chunk-grid').waitFor({ state: 'visible' });
  assert.equal(await page.locator('.chunk').count(), 48);
});
