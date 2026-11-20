/**
 * Demo file server for the resumable downloader.
 *
 * Exposes:
 *   GET  /api/files                     catalog (latest revision metadata)
 *   GET  /api/files/:id                 single file metadata
 *   GET  /api/files/:id/manifest        manifest with per-chunk hashes
 *   GET  /api/files/:id/chunk/:index    one chunk, honours Range + If-None-Match
 *   GET  /api/files/:id/content         whole file, honours Range + If-None-Match
 *   GET  /api/events                    SSE stream of request/fault events
 *   GET  /api/state                     counters snapshot
 *   POST /api/admin/faults              configure fault injection
 *   POST /api/admin/files/:id/replace   publish a new revision
 *   POST /api/admin/files/:id/reset     drop back to revision 1
 *   POST /api/admin/reset               reset faults + all files
 *
 * Fault injection is deterministic per seed so a failing scenario can be
 * replayed. Faults only apply to chunk requests (never admin/static).
 */

import http from 'node:http';
import { readFile, stat } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { buildManifest, readRange, sizeForRevision, PAGE_SIZE } from './catalog.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PUBLIC_DIR = path.resolve(__dirname, '..', 'public');
const PORT = Number(process.env.PORT || 4173);
const HOST = process.env.HOST || '127.0.0.1';
const MAX_EVENTS = 300;

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
  '.woff2': 'font/woff2',
};

const FILE_DEFS = [
  {
    id: 'atlas',
    name: 'atlas-copy-2026.bin',
    mime: 'application/octet-stream',
    seed: 'atlas-v1',
    baseSize: 48 * 1024 * 1024,
    growth: 2 * 1024 * 1024,
    chunkSize: 1 * 1024 * 1024,
    note: 'Geospatial tile archive',
  },
  {
    id: 'telemetry',
    name: 'telemetry-archive.bin',
    mime: 'application/octet-stream',
    seed: 'telemetry',
    baseSize: 24 * 1024 * 1024,
    growth: 1024 * 1024,
    chunkSize: 512 * 1024,
    note: 'Sensor logs, 30 day retention',
  },
  {
    id: 'weights',
    name: 'model-weights.safetensors',
    mime: 'application/octet-stream',
    seed: 'weights',
    baseSize: 96 * 1024 * 1024,
    growth: 0,
    chunkSize: 2 * 1024 * 1024,
    note: 'Frozen embedding weights',
  },
];

const MAX_FAULT_RATE = 0.9;

const state = {
  files: new Map(), // id -> { def, revision, updatedAt }
  faultSeed: 0x2f6e2b1,
  faults: {
    offline: false,
    dropRate: 0.06,
    corruptRate: 0.05,
    rangeMismatchRate: 0.05,
    staleRate: 0.03,
    latencyMs: 60,
    jitterMs: 40,
  },
  counters: {
    requests: 0,
    chunkRequests: 0,
    bytesServed: 0,
    inflightChunks: 0,
    maxConcurrentChunks: 0,
    faults: { drop: 0, corrupt: 0, 'range-mismatch': 0, stale: 0, offline: 0 },
    statuses: {},
  },
  events: [],
  clients: new Set(),
  manifestCache: new Map(),
};

for (const def of FILE_DEFS) {
  state.files.set(def.id, { def, revision: 1, updatedAt: Date.now() });
}

/* ---------------------------------------------------------------- helpers */

function randomSource() {
  // mulberry32 — deterministic, advances one shared stream per fault decision.
  let t = (state.faultSeed += 0x6d2b79f5);
  t = Math.imul(t ^ (t >>> 15), t | 1);
  t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
  return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
}

function takeFault() {
  const f = state.faults;
  if (f.offline) return 'offline';
  const r = randomSource();
  let cursor = f.dropRate;
  if (r < cursor) return 'drop';
  cursor += f.corruptRate;
  if (r < cursor) return 'corrupt';
  cursor += f.rangeMismatchRate;
  if (r < cursor) return 'range-mismatch';
  cursor += f.staleRate;
  if (r < cursor) return 'stale';
  return null;
}

function clampRate(n) {
  if (!Number.isFinite(n)) return 0;
  return Math.min(MAX_FAULT_RATE, Math.max(0, n));
}

function recordEvent(type, payload) {
  const event = { type, at: Date.now(), ...payload };
  state.events.push(event);
  if (state.events.length > MAX_EVENTS) state.events.splice(0, state.events.length - MAX_EVENTS);
  const frame = `data: ${JSON.stringify(event)}\n\n`;
  for (const res of state.clients) {
    try {
      res.write(frame);
    } catch {
      state.clients.delete(res);
    }
  }
  return event;
}

function json(res, status, body, extraHeaders = {}) {
  const data = Buffer.from(JSON.stringify(body));
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': data.length,
    'cache-control': 'no-store',
    ...extraHeaders,
  });
  res.end(data);
}

function readBody(req, limit = 64 * 1024) {
  return new Promise((resolve, reject) => {
    let length = 0;
    const parts = [];
    req.on('data', (b) => {
      length += b.length;
      if (length > limit) {
        reject(Object.assign(new Error('body too large'), { status: 413 }));
        req.destroy();
        return;
      }
      parts.push(b);
    });
    req.on('end', () => resolve(Buffer.concat(parts).toString('utf8')));
    req.on('error', reject);
  });
}

async function getRevision(id) {
  const entry = state.files.get(id);
  if (!entry) return null;
  return entry.revision;
}

async function getManifest(id, revision) {
  const entry = state.files.get(id);
  if (!entry) return null;
  const rev = revision ?? entry.revision;
  const cacheKey = `${id}:${rev}`;
  if (state.manifestCache.has(cacheKey)) return state.manifestCache.get(cacheKey);
  const manifest = await buildManifest({
    ...entry.def,
    id,
    revision: rev,
  });
  manifest.updatedAt = state.files.get(id)?.updatedAt ?? Date.now();
  // Only current revisions are worth caching; old ones stay for diff support.
  state.manifestCache.set(cacheKey, manifest);
  if (state.manifestCache.size > 24) {
    const oldest = state.manifestCache.keys().next().value;
    state.manifestCache.delete(oldest);
  }
  return manifest;
}

function summarize(entry, manifest) {
  return {
    id: entry.def.id,
    name: entry.def.name,
    mime: entry.def.mime,
    note: entry.def.note,
    size: manifest.size,
    chunkSize: manifest.chunkSize,
    chunkCount: manifest.chunkCount,
    revision: manifest.revision,
    sha256: manifest.sha256,
    etag: manifest.etag,
    updatedAt: entry.updatedAt,
  };
}

function parseRange(header, totalSize) {
  if (!header) return null;
  const m = /^bytes=(\d*)-(\d*)$/.exec(header.trim().replace(/^bytes\s+/, 'bytes='));
  if (!m) return { invalid: true };
  let start;
  let end;
  if (m[1] === '' && m[2] === '') return { invalid: true };
  if (m[1] === '') {
    const suffix = Number(m[2]);
    if (!Number.isFinite(suffix) || suffix <= 0) return { invalid: true };
    start = Math.max(0, totalSize - suffix);
    end = totalSize - 1;
  } else {
    start = Number(m[1]);
    end = m[2] === '' ? totalSize - 1 : Number(m[2]);
  }
  if (!Number.isFinite(start) || !Number.isFinite(end) || start < 0 || start > end) {
    return { invalid: true };
  }
  end = Math.min(end, totalSize - 1);
  return { start, end };
}

function sendChunkBuffer(res, req, { fileId, revision, bytes, start, end, total, etag, faults }) {
  const len = bytes.length;
  const headers = {
    'content-type': 'application/octet-stream',
    'content-length': len,
    'accept-ranges': 'bytes',
    'cache-control': 'no-store',
    etag,
    'x-file-id': fileId,
    'x-file-revision': String(revision),
  };
  if (faults.length) headers['x-demo-fault'] = faults.join(',');
  if (start === 0 && end === total - 1) {
    res.writeHead(200, headers);
  } else {
    headers['content-range'] = `bytes ${start}-${end}/${total}`;
    res.writeHead(206, headers);
  }
  res.end(bytes);
}

async function sendFileBytes(req, res, { fileId, revision, bytes, total, etag, faults }) {
  const ifNoneMatch = req.headers['if-none-match'];
  if (ifNoneMatch && ifNoneMatch === etag && !faults.length) {
    res.writeHead(304, { etag });
    res.end();
    return;
  }
  const range = parseRange(req.headers.range, total);
  if (range?.invalid) {
    res.writeHead(416, { 'content-range': `bytes */${total}` });
    res.end();
    return;
  }
  if (range) {
    const part = bytes.subarray(range.start, range.end + 1);
    sendChunkBuffer(res, req, {
      fileId, revision, bytes: part, start: range.start, end: range.end, total, etag, faults,
    });
    return;
  }
  sendChunkBuffer(res, req, {
    fileId, revision, bytes, start: 0, end: total - 1, total, etag, faults,
  });
}

/* ------------------------------------------------------------- api routes */

async function handleManifest(req, res, id) {
  const entry = state.files.get(id);
  if (!entry) return json(res, 404, { error: 'unknown file' });
  const requested = req.url.includes('rev=')
    ? Number(new URL(req.url, 'http://x').searchParams.get('rev'))
    : entry.revision;
  if (!Number.isInteger(requested) || requested < 1 || requested > entry.revision) {
    return json(res, 404, { error: 'unknown revision', currentRevision: entry.revision });
  }
  const manifest = await getManifest(id, requested);
  json(res, 200, manifest, {
    etag: manifest.etag,
    'x-current-revision': String(entry.revision),
  });
}

async function handleChunk(req, res, id, indexRaw) {
  const entry = state.files.get(id);
  if (!entry) return json(res, 404, { error: 'unknown file' });

  const url = new URL(req.url, 'http://x');
  const revParam = url.searchParams.get('rev');
  const requestedRevision = revParam ? Number(revParam) : entry.revision;
  const chunkIndex = Number(indexRaw);
  if (!Number.isInteger(chunkIndex) || chunkIndex < 0) {
    return json(res, 400, { error: 'bad chunk index' });
  }

  const fault = takeFault();
  state.counters.chunkRequests++;
  state.counters.inflightChunks++;
  state.counters.maxConcurrentChunks = Math.max(
    state.counters.maxConcurrentChunks,
    state.counters.inflightChunks,
  );
  let released = false;
  const release = () => {
    if (released) return;
    released = true;
    state.counters.inflightChunks = Math.max(0, state.counters.inflightChunks - 1);
  };
  res.on('close', release);
  res.on('finish', release);
  const faults = fault ? [fault] : [];

  if (fault === 'offline') {
    state.counters.faults.offline++;
    recordEvent('fault', { fileId: id, kind: 'offline', chunkIndex, revision: requestedRevision });
    return json(res, 503, { error: 'simulated offline' });
  }

  if (fault === 'stale' || (Number.isInteger(requestedRevision) && requestedRevision < entry.revision)) {
    state.counters.faults.stale++;
    const manifest = await getManifest(id);
    recordEvent('fault', {
      fileId: id, kind: 'stale', chunkIndex,
      requestedRevision, currentRevision: entry.revision,
    });
    return json(res, 409, {
      error: 'file replaced',
      currentRevision: entry.revision,
      etag: manifest.etag,
      sha256: manifest.sha256,
      chunkCount: manifest.chunkCount,
      chunkSize: manifest.chunkSize,
      size: manifest.size,
      currentEtag: manifest.etag,
    }, { 'x-current-revision': String(entry.revision) });
  }

  if (!Number.isInteger(requestedRevision) || requestedRevision < 1 || requestedRevision > entry.revision) {
    return json(res, 404, { error: 'unknown revision' });
  }

  const manifest = await getManifest(id, requestedRevision);
  const chunk = manifest.chunks[chunkIndex];
  if (!chunk) return json(res, 404, { error: 'chunk out of range' });

  const latency = state.faults.latencyMs + Math.floor(randomSource() * state.faults.jitterMs);
  if (latency > 0) await new Promise((r) => setTimeout(r, latency));

  if (fault === 'drop') {
    // Announce the full length, then kill the socket mid-body. The browser
    // sees a network error / truncated stream, exactly like a real drop.
    state.counters.faults.drop++;
    recordEvent('fault', { fileId: id, kind: 'drop', chunkIndex, revision: requestedRevision });
    const headers = {
      'content-type': 'application/octet-stream',
      'content-length': chunk.length,
      'accept-ranges': 'bytes',
      etag: manifest.etag,
      'x-demo-fault': 'drop',
      'x-file-revision': String(requestedRevision),
    };
    res.writeHead(200, headers);
    const bytes = readRange(entry.def.seed, requestedRevision, chunk.offset, chunk.length);
    const cut = Math.max(1, Math.floor(bytes.length * (0.15 + randomSource() * 0.5)));
    res.write(bytes.subarray(0, cut), () => {
      res.socket?.destroy();
    });
    return;
  }

  if (fault === 'range-mismatch') {
    // Serve a shifted window while advertising the requested range. A client
    // that trusts Content-Range alone will splice the wrong bytes in.
    state.counters.faults['range-mismatch']++;
    recordEvent('fault', {
      fileId: id, kind: 'range-mismatch', chunkIndex, revision: requestedRevision,
    });
    const shift = Math.max(1, Math.floor(randomSource() * (chunk.length - 16)));
    const wrongStart = Math.min(chunk.offset + shift, manifest.size - 16);
    const length = Math.min(4096, manifest.size - wrongStart);
    const bytes = readRange(entry.def.seed, requestedRevision, wrongStart, length);
    const headers = {
      'content-type': 'application/octet-stream',
      'content-length': length,
      'accept-ranges': 'bytes',
      etag: manifest.etag,
      'x-demo-fault': 'range-mismatch',
      'x-file-revision': String(requestedRevision),
      // Lies: claims it is the requested range but the body starts elsewhere.
      'content-range': `bytes ${chunk.offset}-${chunk.offset + length - 1}/${manifest.size}`,
    };
    res.writeHead(206, headers);
    res.end(bytes);
    return;
  }

  // Range headers use file-level offsets; a chunk endpoint only serves the
  // sub-range that falls inside its own span, and rejects anything else.
  const requested = parseRange(req.headers.range, manifest.size);
  if (
    requested?.invalid ||
    (requested && (requested.start < chunk.offset || requested.end > chunk.end))
  ) {
    res.writeHead(416, { 'content-range': `bytes */${manifest.size}` });
    res.end();
    return;
  }
  const localStart = requested ? requested.start - chunk.offset : 0;
  const localEnd = requested ? requested.end - chunk.offset : chunk.length - 1;
  const bodyLength = localEnd - localStart + 1;

  if (fault === 'corrupt') {
    state.counters.faults.corrupt++;
    recordEvent('fault', { fileId: id, kind: 'corrupt', chunkIndex, revision: requestedRevision });
    const bytes = readRange(
      entry.def.seed, requestedRevision, chunk.offset + localStart, bodyLength,
    );
    const pos = Math.floor(randomSource() * bytes.length);
    bytes[pos] ^= 0xff;
    state.counters.bytesServed += bytes.length;
    return sendChunkBuffer(res, req, {
      fileId: id, revision: requestedRevision, bytes,
      start: chunk.offset + localStart, end: chunk.offset + localEnd, total: manifest.size,
      etag: `"corrupt-${chunkIndex}-r${requestedRevision}"`,
      faults,
    });
  }

  const bytes = readRange(
    entry.def.seed, requestedRevision, chunk.offset + localStart, bodyLength,
  );
  state.counters.bytesServed += bytes.length;
  sendChunkBuffer(res, req, {
    fileId: id, revision: requestedRevision, bytes,
    start: chunk.offset + localStart, end: chunk.offset + localEnd, total: manifest.size,
    etag: manifest.etag, faults,
  });
}

async function handleContent(req, res, id) {
  const entry = state.files.get(id);
  if (!entry) return json(res, 404, { error: 'unknown file' });
  const url = new URL(req.url, 'http://x');
  const revParam = url.searchParams.get('rev');
  const revision = revParam ? Number(revParam) : entry.revision;
  if (!Number.isInteger(revision) || revision < 1 || revision > entry.revision) {
    return json(res, 404, { error: 'unknown revision' });
  }
  const manifest = await getManifest(id, revision);
  // Whole-file streams are capped: the browser demo never needs the full
  // body, Range requests are what the downloader uses.
  const range = parseRange(req.headers.range, manifest.size);
  if (range?.invalid) {
    res.writeHead(416, { 'content-range': `bytes */${manifest.size}` });
    res.end();
    return;
  }
  if (!range) {
    return json(res, 400, {
      error: 'whole-file responses are disabled in the demo; use Range or /manifest',
      size: manifest.size,
      chunkSize: manifest.chunkSize,
    }, { 'accept-ranges': 'bytes' });
  }
  const length = range.end - range.start + 1;
  if (length > 8 * 1024 * 1024) {
    return json(res, 416, { error: 'range too large for demo (max 8 MiB)', 'content-range': `bytes */${manifest.size}` });
  }
  const bytes = readRange(entry.def.seed, revision, range.start, length);
  state.counters.bytesServed += bytes.length;
  const headers = {
    'content-type': entry.def.mime,
    'content-length': length,
    'accept-ranges': 'bytes',
    'content-range': `bytes ${range.start}-${range.end}/${manifest.size}`,
    etag: manifest.etag,
  };
  res.writeHead(206, headers);
  res.end(bytes);
}

async function handleAdminFaults(req, res) {
  let body;
  try {
    body = JSON.parse((await readBody(req)) || '{}');
  } catch {
    return json(res, 400, { error: 'invalid JSON' });
  }
  const f = state.faults;
  if ('offline' in body) f.offline = Boolean(body.offline);
  if ('dropRate' in body) f.dropRate = clampRate(Number(body.dropRate));
  if ('corruptRate' in body) f.corruptRate = clampRate(Number(body.corruptRate));
  if ('rangeMismatchRate' in body) f.rangeMismatchRate = clampRate(Number(body.rangeMismatchRate));
  if ('staleRate' in body) f.staleRate = clampRate(Number(body.staleRate));
  if ('latencyMs' in body) f.latencyMs = Math.max(0, Math.min(2000, Number(body.latencyMs) || 0));
  if ('jitterMs' in body) f.jitterMs = Math.max(0, Math.min(2000, Number(body.jitterMs) || 0));
  if ('seed' in body) {
    state.faultSeed = Number(body.seed) >>> 0 || 1;
    state.manifestCache.clear();
  }
  if (body.resetCounters) {
    for (const key of Object.keys(state.counters.faults)) state.counters.faults[key] = 0;
    state.counters.requests = 0;
    state.counters.chunkRequests = 0;
    state.counters.bytesServed = 0;
    state.counters.statuses = {};
    state.counters.maxConcurrentChunks = state.counters.inflightChunks;
  }
  recordEvent('config', { faults: { ...f }, seed: state.faultSeed });
  json(res, 200, { faults: { ...f }, seed: state.faultSeed });
}

async function handleAdminReplace(req, res, id) {
  const entry = state.files.get(id);
  if (!entry) return json(res, 404, { error: 'unknown file' });
  const previous = await getManifest(id);
  entry.revision += 1;
  entry.updatedAt = Date.now();
  const manifest = await getManifest(id, entry.revision);
  let reused = 0;
  let changed = 0;
  let added = 0;
  let removed = 0;
  const limit = Math.max(previous.chunkCount, manifest.chunkCount);
  for (let i = 0; i < limit; i++) {
    const a = previous.chunks[i];
    const b = manifest.chunks[i];
    if (a && b) {
      if (a.sha256 === b.sha256) reused++;
      else changed++;
    } else if (b) added++;
    else removed++;
  }
  recordEvent('replace', {
    fileId: id,
    fromRevision: previous.revision,
    toRevision: manifest.revision,
    sizeDelta: manifest.size - previous.size,
    reused, changed, added, removed,
  });
  json(res, 200, {
    file: summarize(entry, manifest),
    previous: summarize(entry, previous),
    diff: {
      fromRevision: previous.revision,
      toRevision: manifest.revision,
      fromSize: previous.size,
      toSize: manifest.size,
      sizeDelta: manifest.size - previous.size,
      fromChunkCount: previous.chunkCount,
      toChunkCount: manifest.chunkCount,
      reused,
      changed,
      added,
      removed,
      fromEtag: previous.etag,
      toEtag: manifest.etag,
      chunks: manifest.chunks.map((chunk, i) => ({
        index: i,
        status: previous.chunks[i]
          ? previous.chunks[i].sha256 === chunk.sha256 ? 'same' : 'changed'
          : 'added',
      })).concat(
        previous.chunks.slice(manifest.chunkCount).map((chunk) => ({
          index: chunk.index,
          status: 'removed',
        })),
      ),
    },
  });
}

async function handleAdminReset(req, res, id) {
  const entry = state.files.get(id);
  if (!entry) return json(res, 404, { error: 'unknown file' });
  entry.revision = 1;
  entry.updatedAt = Date.now();
  state.manifestCache.delete(`${id}:1`);
  const manifest = await getManifest(id, 1);
  recordEvent('replace', { fileId: id, toRevision: 1, reset: true });
  json(res, 200, { file: summarize(entry, manifest) });
}

/* ---------------------------------------------------------- static files */

async function serveStatic(req, res, pathname) {
  let rel = pathname === '/' ? '/index.html' : pathname;
  const target = path.resolve(PUBLIC_DIR, `.${rel}`);
  if (!target.startsWith(PUBLIC_DIR + path.sep) && target !== PUBLIC_DIR) {
    return json(res, 403, { error: 'forbidden' });
  }
  try {
    const info = await stat(target);
    if (!info.isFile()) return json(res, 404, { error: 'not found' });
    const data = await readFile(target);
    const type = MIME[path.extname(target).toLowerCase()] || 'application/octet-stream';
    res.writeHead(200, {
      'content-type': type,
      'content-length': data.length,
      'cache-control': 'no-cache',
    });
    res.end(data);
  } catch {
    json(res, 404, { error: 'not found' });
  }
}

/* -------------------------------------------------------------- dispatch */

const server = http.createServer(async (req, res) => {
  const started = Date.now();
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const pathname = decodeURIComponent(url.pathname);
  state.counters.requests++;
  const isChunk = /^\/api\/files\/[^/]+\/chunk\//.test(pathname);
  if (!isChunk) {
    recordEvent('request', { method: req.method, path: pathname, status: 200 });
  }
  res.on('finish', () => {
    state.counters.statuses[res.statusCode] = (state.counters.statuses[res.statusCode] || 0) + 1;
    if (isChunk) {
      recordEvent('chunk', {
        path: pathname,
        status: res.statusCode,
        ms: Date.now() - started,
        bytes: Number(res.getHeader('content-length') || 0),
      });
    }
  });
  res.on('close', () => {
    for (const client of state.clients) {
      if (client === res) state.clients.delete(res);
    }
  });

  try {
    if (pathname === '/api/files' && req.method === 'GET') {
      const files = [];
      for (const [id, entry] of state.files) {
        files.push(summarize(entry, await getManifest(id)));
      }
      return json(res, 200, { files, faults: { ...state.faults }, seed: state.faultSeed });
    }

    let m = /^\/api\/files\/([^/]+)$/.exec(pathname);
    if (m && req.method === 'GET') {
      const entry = state.files.get(m[1]);
      if (!entry) return json(res, 404, { error: 'unknown file' });
      return json(res, 200, summarize(entry, await getManifest(m[1])));
    }

    m = /^\/api\/files\/([^/]+)\/manifest$/.exec(pathname);
    if (m && req.method === 'GET') return handleManifest(req, res, m[1]);

    m = /^\/api\/files\/([^/]+)\/chunk\/(\d+)$/.exec(pathname);
    if (m && req.method === 'GET') return handleChunk(req, res, m[1], m[2]);

    m = /^\/api\/files\/([^/]+)\/content$/.exec(pathname);
    if (m && req.method === 'GET') return handleContent(req, res, m[1]);

    if (pathname === '/api/events' && req.method === 'GET') {
      res.writeHead(200, {
        'content-type': 'text/event-stream',
        'cache-control': 'no-store',
        connection: 'keep-alive',
        'x-accel-buffering': 'no',
      });
      res.write(`retry: 2000\n\n`);
      res.write(`data: ${JSON.stringify({ type: 'hello', at: Date.now() })}\n\n`);
      state.clients.add(res);
      const ping = setInterval(() => {
        try {
          res.write(`: ping\n\n`);
        } catch {
          clearInterval(ping);
        }
      }, 15000);
      req.on('close', () => {
        clearInterval(ping);
        state.clients.delete(res);
      });
      return;
    }

    if (pathname === '/api/state' && req.method === 'GET') {
      return json(res, 200, {
        faults: { ...state.faults },
        seed: state.faultSeed,
        counters: state.counters,
        files: [...state.files.values()].map((f) => ({
          id: f.def.id, revision: f.revision, updatedAt: f.updatedAt,
          size: sizeForRevision(f.def.baseSize, f.def.growth, f.revision),
        })),
        events: state.events.slice(-60),
      });
    }

    if (pathname === '/api/admin/faults' && (req.method === 'POST' || req.method === 'PUT')) {
      return handleAdminFaults(req, res);
    }

    m = /^\/api\/admin\/files\/([^/]+)\/replace$/.exec(pathname);
    if (m && req.method === 'POST') return handleAdminReplace(req, res, m[1]);

    m = /^\/api\/admin\/files\/([^/]+)\/reset$/.exec(pathname);
    if (m && req.method === 'POST') return handleAdminReset(req, res, m[1]);

    if (req.method === 'GET' || req.method === 'HEAD') {
      return serveStatic(req, res, pathname);
    }

    json(res, 404, { error: 'not found' });
  } catch (err) {
    console.error('[server]', err);
    if (!res.headersSent) json(res, 500, { error: 'internal error', detail: String(err?.message || err) });
    else res.destroy();
  }
});

export { server, state, getManifest, PAGE_SIZE };

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  server.listen(PORT, HOST, async () => {
    // Warm the manifests so the first UI load is instant.
    for (const id of state.files.keys()) await getManifest(id);
    console.log(`[downloader] http://${HOST}:${PORT}`);
    console.log(`[downloader] files: ${[...state.files.keys()].join(', ')}`);
    console.log('[downloader] faults:', state.faults);
  });
}
