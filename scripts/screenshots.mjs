/**
 * Visual smoke: captures light, dark and narrow-viewport screenshots of the
 * dashboard in a few representative states. Run against a live server:
 *   node scripts/screenshots.mjs [baseUrl] [outDir]
 */
import { chromium } from 'playwright';

const base = process.argv[2] || 'http://127.0.0.1:4173';
const out = process.argv[3] || '/tmp/shots';

const api = async (path, body) => {
  const res = await fetch(`${base}${path}`, {
    method: body === undefined ? 'GET' : 'POST',
    headers: body === undefined ? {} : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return res.json().catch(() => null);
};

const browser = await chromium.launch();
const context = await browser.newContext({ viewport: { width: 1440, height: 1080 }, deviceScaleFactor: 2 });
const page = await context.newPage();

await api('/api/admin/files/telemetry/reset', {});
await api('/api/admin/faults', {
  resetCounters: true, offline: false, dropRate: 0.18, corruptRate: 0.14,
  rangeMismatchRate: 0.1, staleRate: 0, latencyMs: 30, jitterMs: 20,
});
await page.goto(`${base}/?file=telemetry`, { waitUntil: 'load' });
await page.locator('#btn-start').click();
await page.waitForFunction(
  () => document.querySelectorAll('.chunk[data-status="done"]').length >= 10,
  null,
  { timeout: 60_000 },
);
await page.screenshot({ path: `${out}/01-downloading-light.png`, fullPage: true });

// Hover a retrying/failed chunk to capture the tooltip.
const failed = page.locator('.chunk[data-status="retrying"], .chunk[data-status="failed"]').first();
if (await failed.count()) {
  await failed.hover();
  await page.waitForTimeout(200);
  await page.screenshot({ path: `${out}/02-tooltip-light.png` });
  await page.mouse.move(5, 5);
}

await page.locator('#btn-replace').click();
await page.waitForFunction(() => document.querySelector('#rev-badge')?.innerText === 'r2', null, { timeout: 30_000 });
await page.waitForTimeout(600);
await api('/api/admin/faults', { resetCounters: true, offline: false, dropRate: 0, corruptRate: 0, rangeMismatchRate: 0, latencyMs: 0, jitterMs: 0 });
await page.waitForFunction(
  () => document.querySelector('#status-chip')?.dataset.state === 'completed',
  null,
  { timeout: 120_000 },
);
await page.waitForTimeout(400);
await page.screenshot({ path: `${out}/03-completed-light.png`, fullPage: true });

await page.locator('#theme-toggle').click();
await page.waitForTimeout(300);
await page.screenshot({ path: `${out}/04-completed-dark.png`, fullPage: true });

await page.locator('.file-tab').nth(0).click();
await page.waitForTimeout(400);
await page.locator('#concurrency').fill('6');
await page.locator('#btn-start').click();
await page.waitForTimeout(900);
await page.screenshot({ path: `${out}/05-atlas-dark.png`, fullPage: true });

const narrow = await context.newPage();
await narrow.setViewportSize({ width: 420, height: 900 });
await narrow.goto(`${base}/?file=telemetry`, { waitUntil: 'load' });
await narrow.waitForTimeout(800);
await narrow.screenshot({ path: `${out}/06-narrow-light.png`, fullPage: true });

await browser.close();
console.log(`screenshots written to ${out}`);
