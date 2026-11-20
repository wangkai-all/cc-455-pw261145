import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { Sha256, sha256Hex, sha256Blob } from '../public/lib/sha256.js';
import { MemoryStore } from '../public/lib/store.js';
import {
  readRange, pageBytes, sizeForRevision, buildManifest, PAGE_SIZE,
} from '../server/catalog.js';

const rand = (n) => crypto.randomBytes(n);

test('sha256 matches node:crypto across block boundaries', () => {
  for (const n of [0, 1, 55, 56, 57, 63, 64, 65, 127, 128, 129, 1000, 65537]) {
    const bytes = rand(n);
    const expected = crypto.createHash('sha256').update(bytes).digest('hex');
    assert.equal(sha256Hex(new Uint8Array(bytes)), expected, `one-shot n=${n}`);

    const hasher = new Sha256();
    let pos = 0;
    while (pos < n) {
      const step = 1 + Math.floor(Math.random() * 23);
      hasher.update(new Uint8Array(bytes.subarray(pos, pos + step)));
      pos += step;
    }
    assert.equal(hasher.digestHex(), expected, `incremental n=${n}`);
  }
});

test('sha256 accepts ArrayBuffer and is digest-repeatable', () => {
  const bytes = rand(300);
  const h = new Sha256().update(bytes.buffer);
  const a = h.digestHex();
  assert.equal(a, sha256Hex(new Uint8Array(bytes)));
  assert.equal(h.digestHex(), a, 'digest must not consume the state');
  h.update(new TextEncoder().encode('tail'));
  assert.equal(
    h.digestHex(),
    crypto.createHash('sha256').update(bytes).update('tail').digest('hex'),
  );
});

test('sha256Blob streams in chunks and reports progress', async () => {
  const blob = new Blob([rand(10_000)]);
  const seen = [];
  const hash = await sha256Blob(blob, {
    chunkSize: 1024,
    onProgress: (done, total) => seen.push([done, total]),
  });
  const buf = Buffer.from(await blob.arrayBuffer());
  assert.equal(hash, crypto.createHash('sha256').update(buf).digest('hex'));
  assert.equal(seen.at(-1)[0], 10_000);
  assert.ok(seen.length > 1);
});

test('memory store keeps blobs and prunes by hash', async () => {
  const store = new MemoryStore();
  const blob = new Blob([new Uint8Array([1, 2, 3])]);
  await store.putChunk('f', 0, blob, { sha256: 'aaa.0', size: 3 });
  await store.putChunk('f', 1, blob, { sha256: 'bbb.0', size: 3 });
  await store.putChunk('g', 0, blob, { sha256: 'ccc.0', size: 3 });
  assert.deepEqual(await store.listChunkIndexes('f'), [0, 1]);
  assert.equal(await store.hasChunk('f', 1), true);

  const removed = await store.pruneChunks('f', new Set(['bbb.0']));
  assert.equal(removed, 1);
  assert.deepEqual(await store.listChunkIndexes('f'), [1]);
  assert.equal((await store.getChunk('f', 1)).meta.sha256, 'bbb.0');
  assert.equal(await store.hasChunk('g', 0), true, 'other files untouched');

  await store.clearFile('f');
  assert.deepEqual(await store.listChunkIndexes('f'), []);
  assert.deepEqual(await store.listChunkIndexes('g'), [0]);
});

test('memory store clones state so callers cannot mutate stored copies', async () => {
  const store = new MemoryStore();
  await store.putState('f', { completed: { 0: { sha256: 'x' } } });
  const read = await store.getState('f');
  read.completed[0].sha256 = 'tampered';
  const again = await store.getState('f');
  assert.equal(again.completed[0].sha256, 'x');
});

test('deterministic content: ranges agree with whole pages', () => {
  const size = PAGE_SIZE * 4;
  const whole = readRange('seed', 1, 0, size);
  const inner = readRange('seed', 1, PAGE_SIZE - 10, 20);
  assert.deepEqual(Buffer.from(inner), Buffer.from(whole.subarray(PAGE_SIZE - 10, PAGE_SIZE + 10)));

  const page = pageBytes('seed', 2, 1);
  assert.deepEqual(Buffer.from(page), Buffer.from(whole.subarray(PAGE_SIZE * 2, PAGE_SIZE * 3)));
});

test('revisions change patched pages but keep the rest stable', () => {
  assert.equal(sizeForRevision(1000, 100, 3), 1200);
  const r1page0 = readRange('seed', 1, 0, PAGE_SIZE);
  const r2page0 = readRange('seed', 2, 0, PAGE_SIZE);
  assert.notDeepEqual(Buffer.from(r1page0), Buffer.from(r2page0), 'page 0 always changes');

  const r1page1 = readRange('seed', 1, PAGE_SIZE, PAGE_SIZE);
  const r2page1 = readRange('seed', 2, PAGE_SIZE, PAGE_SIZE);
  assert.deepEqual(Buffer.from(r1page1), Buffer.from(r2page1), 'unpatched page is stable');
});

test('manifest hashes match the served bytes', async () => {
  const manifest = await buildManifest({
    id: 'x', name: 'x.bin', mime: 'application/octet-stream',
    seed: 'x', baseSize: 20_000, growth: 5000, chunkSize: 4096, revision: 2,
  });
  assert.equal(manifest.chunkCount, Math.ceil(25_000 / 4096));
  const whole = readRange('x', 2, 0, manifest.size);
  assert.equal(crypto.createHash('sha256').update(whole).digest('hex'), manifest.sha256);

  for (const chunk of manifest.chunks) {
    const bytes = readRange('x', 2, chunk.offset, chunk.length);
    assert.equal(
      crypto.createHash('sha256').update(bytes).digest('hex'),
      chunk.sha256,
      `chunk ${chunk.index}`,
    );
    assert.equal(chunk.end, chunk.offset + chunk.length - 1);
  }
  const last = manifest.chunks.at(-1);
  assert.equal(last.offset + last.length, manifest.size);
});
