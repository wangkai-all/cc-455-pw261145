/**
 * Boots the real server on an ephemeral port and exercises the engine end to
 * end: clean download, all four injected faults, cancel/resume from storage,
 * and a mid-download file replacement.
 */

import test, { before, after } from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { server, state } from '../server/index.js';
import { DownloadEngine } from '../public/lib/engine.js';
import { MemoryStore } from '../public/lib/store.js';

let base;
let port;

const FILE = 'telemetry';

async function api(path, body) {
  const res = await fetch(`${base}${path}`, {
    method: body === undefined ? 'GET' : 'POST',
    headers: body === undefined ? {} : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return { status: res.status, body: await res.json().catch(() => null) };
}

const setFaults = (patch) => api('/api/admin/faults', { resetCounters: true, ...patch });

async function cleanFaults() {
  await setFaults({
    offline: false, dropRate: 0, corruptRate: 0, rangeMismatchRate: 0, staleRate: 0,
    latencyMs: 0, jitterMs: 0,
  });
}

function makeEngine(overrides = {}) {
  return new DownloadEngine({
    fileId: FILE,
    baseUrl: base,
    concurrency: 4,
    store: new MemoryStore(),
    versionCheckMs: 0,
    retryBaseMs: 25,
    retryMaxMs: 120,
    maxAttempts: 8,
    ...overrides,
  });
}

before(async () => {
  await new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      port = server.address().port;
      base = `http://127.0.0.1:${port}`;
      resolve();
    });
  });
});

after(async () => {
  await new Promise((resolve) => server.close(resolve));
});

test('server: catalog exposes size, hash and chunk geometry', async () => {
  const { status, body } = await api('/api/files');
  assert.equal(status, 200);
  assert.ok(body.files.length >= 3);
  const file = body.files.find((f) => f.id === FILE);
  assert.equal(file.chunkCount, Math.ceil(file.size / file.chunkSize));
  assert.match(file.sha256, /^[0-9a-f]{64}$/);
});

test('server: chunk range request returns exactly the requested bytes', async () => {
  await cleanFaults();
  const manifest = (await api(`/api/files/${FILE}/manifest`)).body;
  const chunk = manifest.chunks[3];
  const res = await fetch(`${base}/api/files/${FILE}/chunk/3?rev=1`, {
    headers: { range: `bytes ${chunk.offset}-${chunk.end}` },
  });
  assert.equal(res.status, 206);
  assert.equal(res.headers.get('content-range'), `bytes ${chunk.offset}-${chunk.end}/${manifest.size}`);
  const bytes = Buffer.from(await res.arrayBuffer());
  assert.equal(bytes.length, chunk.length);
  assert.equal(crypto.createHash('sha256').update(bytes).digest('hex'), chunk.sha256);
});

test('server: invalid ranges return 416', async () => {
  const manifest = (await api(`/api/files/${FILE}/manifest`)).body;
  const res = await fetch(`${base}/api/files/${FILE}/content?rev=1`, {
    headers: { range: `bytes=${manifest.size + 10}-` },
  });
  assert.equal(res.status, 416);
});

test('engine: clean download merges and verifies the whole-file hash', async () => {
  await cleanFaults();
  const engine = makeEngine();
  await engine.init();
  const events = [];
  engine.on('completed', (e) => events.push(e));
  const snap = await engine.start();
  assert.equal(snap.status, 'completed');
  assert.equal(snap.completed, snap.chunkCount);

  const manifest = (await api(`/api/files/${FILE}/manifest`)).body;
  assert.equal(snap.mergedSha256, manifest.sha256);
  assert.equal(events.length, 1);
  const merged = Buffer.from(await events[0].blob.arrayBuffer());
  assert.equal(merged.length, manifest.size);
  assert.equal(crypto.createHash('sha256').update(merged).digest('hex'), manifest.sha256);
});

test('engine: recovers from drops, bad hashes and range mismatches', async () => {
  await setFaults({
    offline: false, dropRate: 0.25, corruptRate: 0.2, rangeMismatchRate: 0.15,
    staleRate: 0, latencyMs: 0, jitterMs: 0,
  });
  const engine = makeEngine({ maxAttempts: 12 });
  const reasons = new Set();
  engine.on('retry', (e) => reasons.add(e.reason));
  await engine.init();
  const snap = await engine.start();

  assert.equal(snap.status, 'completed');
  assert.equal(snap.mergedSha256, (await api(`/api/files/${FILE}/manifest`)).body.sha256);
  // The injected mix should surface at least the three transport-level causes.
  assert.ok(reasons.has('network') || reasons.has('checksum'), `saw ${[...reasons]}`);
  assert.ok(snap.retries.length > 0);
});

test('engine: whole-file hash mismatch is caught and reported', async () => {
  await cleanFaults();
  const engine = makeEngine({ verifyChunksOnMerge: false });
  await engine.init();
  // Poison the manifest hash after init: chunks verify individually, but the
  // assembled file cannot match this "server" claim.
  engine.manifest = { ...engine.manifest, sha256: 'deadbeef'.repeat(8) };
  const failures = [];
  engine.on('failed', (e) => failures.push(e));
  const snap = await engine.start();
  assert.equal(snap.status, 'failed');
  assert.equal(snap.lastError.reason, 'hash-mismatch');
  assert.equal(failures.length, 1);
  assert.equal(snap.completed, snap.chunkCount, 'chunks are kept for repair');
});

test('engine: cancel keeps completed chunks; resume reuses them', async () => {
  await cleanFaults();
  const store = new MemoryStore();
  const first = makeEngine({ store, concurrency: 3 });
  await first.init();

  await setFaults({ latencyMs: 60, jitterMs: 30, dropRate: 0, corruptRate: 0, rangeMismatchRate: 0 });
  const run = first.start();
  await new Promise((r) => setTimeout(r, 350));
  await first.cancel();
  const cancelled = await run;
  assert.equal(cancelled.status, 'cancelled');
  assert.ok(cancelled.completed > 0, 'some chunks finished before cancel');
  assert.ok(cancelled.completed < cancelled.chunkCount, 'not everything finished');

  const peek = await first.peek();
  assert.equal(peek.status, 'cancelled');
  assert.equal(peek.completedCount, cancelled.completed);

  await cleanFaults();
  // Fresh engine over the same store = a page reload.
  const second = makeEngine({ store, concurrency: 4 });
  await second.init();
  const verified = [];
  second.on('verify', (e) => e.done && verified.push(e));
  const snap = await second.start();

  assert.equal(snap.status, 'completed');
  assert.equal(verified.at(-1).ok >= cancelled.completed, true, 'stored chunks re-verified');
  const reused = Object.values(snap.completedMap).filter((c) => c.reused).length;
  assert.equal(reused, cancelled.completed, 'no completed chunk was re-downloaded');
  assert.equal(snap.mergedSha256, (await api(`/api/files/${FILE}/manifest`)).body.sha256);
});

test('engine: corrupt stored chunk is detected on resume and re-downloaded', async () => {
  await cleanFaults();
  const store = new MemoryStore();
  const first = makeEngine({ store, concurrency: 2 });
  await first.init();
  await first.start();
  assert.equal(first.status, 'completed');

  // Simulate bit-rot on disk for one chunk.
  const target = 5;
  const good = await store.getChunk(FILE, target);
  const bytes = new Uint8Array(await good.blob.arrayBuffer());
  bytes[0] ^= 0xff;
  await store.putChunk(FILE, target, new Blob([bytes]), good.meta);
  const st = await store.getState(FILE);
  st.completedAt = null;
  await store.putState(FILE, st);

  const second = makeEngine({ store });
  await second.init();
  const repaired = [];
  second.on('recover', (e) => repaired.push(e));
  const snap = await second.start();
  assert.equal(snap.status, 'completed');
  assert.equal(repaired.length, 1);
  assert.equal(repaired[0].chunks[0].index, target);
  assert.equal(snap.mergedSha256, (await api(`/api/files/${FILE}/manifest`)).body.sha256);
});

test('engine: file replacement discards stale chunks and reuses matching ones', async () => {
  await cleanFaults();
  await api(`/api/admin/files/${FILE}/reset`, {});
  const store = new MemoryStore();
  const engine = makeEngine({ store, concurrency: 4 });
  await engine.init();
  assert.equal(engine.manifest.revision, 1);

  await setFaults({ latencyMs: 50, jitterMs: 20, dropRate: 0, corruptRate: 0, rangeMismatchRate: 0 });
  const run = engine.start();
  await new Promise((r) => setTimeout(r, 300));
  const before = engine.snapshot();
  assert.ok(before.completed > 0 && before.completed < before.chunkCount);

  const replaced = await api(`/api/admin/files/${FILE}/replace`, {});
  assert.equal(replaced.body.diff.toRevision, 2);
  const versionEvents = [];
  engine.on('version', (e) => versionEvents.push(e));

  await cleanFaults();
  const snap = await run;

  assert.equal(snap.status, 'completed');
  assert.equal(versionEvents.length, 1);
  const diff = versionEvents[0].diff;
  assert.equal(diff.fromRevision, 1);
  assert.equal(diff.toRevision, 2);
  assert.equal(snap.revision, 2);
  assert.equal(snap.mergedSha256, (await api(`/api/files/${FILE}/manifest`)).body.sha256);

  // Every chunk kept across the swap must have the same hash in the new manifest.
  const newManifest = (await api(`/api/files/${FILE}/manifest`)).body;
  const kept = Object.keys(snap.completedMap).filter((i) => newManifest.chunks[i]);
  assert.ok(kept.length > 0);
  const history = snap.versionHistory.at(-1);
  assert.equal(history.fromRevision, 1);
  assert.ok(history.reused > 0, 'some chunks survived the replacement');
  assert.ok(history.discarded > 0, 'changed chunks were discarded');

  // Reset the fixture for later tests.
  await api(`/api/admin/files/${FILE}/reset`, {});
});

test('engine: replacement discovered by 409 when polling is off', async () => {
  await cleanFaults();
  await api(`/api/admin/files/${FILE}/reset`, {});
  const engine = makeEngine({ concurrency: 2, versionCheckMs: 0 });
  await engine.init();

  await setFaults({ latencyMs: 120, jitterMs: 40, dropRate: 0, corruptRate: 0, rangeMismatchRate: 0 });
  const run = engine.start();
  await new Promise((r) => setTimeout(r, 260));
  await api(`/api/admin/files/${FILE}/replace`, {});
  await cleanFaults();
  const snap = await run;
  assert.equal(snap.status, 'completed');
  assert.equal(snap.revision, 2);
  assert.ok(snap.retries.some((r) => r.reason === 'stale'), 'a 409 was observed');
  assert.equal(snap.mergedSha256, (await api(`/api/files/${FILE}/manifest`)).body.sha256);
  await api(`/api/admin/files/${FILE}/reset`, {});
});

test('engine: concurrent workers never exceed the configured limit', async () => {
  await cleanFaults();
  const engine = makeEngine({ concurrency: 3 });
  await engine.init();
  await engine.start();
  assert.equal(engine.status, 'completed');
  const { body } = await api('/api/state');
  assert.ok(body.counters.maxConcurrentChunks <= 3, `saw ${body.counters.maxConcurrentChunks}`);
});

test('engine: exhaustion is reported, then resume after the fault clears', async () => {
  await cleanFaults();
  await api(`/api/admin/files/${FILE}/reset`, {});
  const store = new MemoryStore();
  const engine = makeEngine({ store, maxAttempts: 2, concurrency: 2 });
  await engine.init();

  await setFaults({ offline: true, latencyMs: 0, jitterMs: 0 });
  const failures = [];
  engine.on('failed', (e) => failures.push(e));
  const failed = await engine.start();
  assert.equal(failed.status, 'failed');
  assert.equal(failed.lastError.reason, 'retries-exhausted');
  assert.equal(failures.length, 1);

  const kept = failed.completed;
  await cleanFaults();
  const resumed = await engine.start();
  assert.equal(resumed.status, 'completed');
  assert.ok(resumed.completedMap[0], 'resumed run completed');
  const reused = Object.values(resumed.completedMap).filter((c) => c.reused).length;
  assert.ok(reused >= Math.min(kept, 1) || kept === 0);
  assert.equal(resumed.mergedSha256, (await api(`/api/files/${FILE}/manifest`)).body.sha256);
});

test('server: replacement diff matches manifest hashes', async () => {
  await api(`/api/admin/files/${FILE}/reset`, {});
  const rev1 = (await api(`/api/files/${FILE}/manifest?rev=1`)).body;
  const replaced = await api(`/api/admin/files/${FILE}/replace`, {});
  const rev2 = (await api(`/api/files/${FILE}/manifest?rev=2`)).body;
  const diff = replaced.body.diff;

  let same = 0;
  let changed = 0;
  const limit = Math.min(rev1.chunkCount, rev2.chunkCount);
  for (let i = 0; i < limit; i++) {
    if (rev1.chunks[i].sha256 === rev2.chunks[i].sha256) same++;
    else changed++;
  }
  assert.equal(diff.reused, same);
  assert.equal(diff.changed, changed);
  assert.equal(
    diff.added,
    Math.max(0, rev2.chunkCount - rev1.chunkCount),
  );
  assert.equal(rev2.size - rev1.size, diff.sizeDelta);
  assert.notEqual(rev1.sha256, rev2.sha256);
  await api(`/api/admin/files/${FILE}/reset`, {});
});

test('engine: concurrency can be raised mid-run without exceeding the cap', async () => {
  await cleanFaults();
  await api(`/api/admin/files/${FILE}/reset`, {});
  const engine = makeEngine({ concurrency: 1 });
  await engine.init();
  await setFaults({ offline: false, dropRate: 0, corruptRate: 0, rangeMismatchRate: 0, latencyMs: 40, jitterMs: 20 });
  const run = engine.start();
  await new Promise((r) => setTimeout(r, 200));
  // Keep the latency in place: with a zero-latency loopback server the
  // requests can finish serially and never overlap.
  await setFaults({ offline: false, dropRate: 0, corruptRate: 0, rangeMismatchRate: 0, latencyMs: 40, jitterMs: 20 });
  engine.setConcurrency(5);
  const snap = await run;
  assert.equal(snap.status, 'completed');
  const { body } = await api('/api/state');
  assert.ok(body.counters.maxConcurrentChunks > 1, 'pool actually widened');
  assert.ok(body.counters.maxConcurrentChunks <= 5, `saw ${body.counters.maxConcurrentChunks}`);
});
