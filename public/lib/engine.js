/**
 * Resumable, concurrent, checksum-verified chunk downloader.
 *
 * Guarantees:
 *  - every chunk is verified against the manifest hash before it is kept;
 *  - the HTTP response must actually cover the requested byte range
 *    (status 206 + matching Content-Range + declared length), not merely
 *    claim to;
 *  - completed chunks survive cancel / reload and are re-verified on resume;
 *  - when the file is replaced, old chunks are pruned and only chunks whose
 *    checksum still matches the new manifest are reused;
 *  - the merged file is hashed end-to-end and compared with the manifest
 *    before it is exposed as complete.
 */

import { sha256Hex, sha256Blob } from './sha256.js';
import { createStore } from './store.js';

export const RETRY_REASONS = {
  network: '网络中断',
  offline: '连接失败',
  checksum: '分片校验失败',
  'range-mismatch': '范围响应不一致',
  'short-body': '响应长度不足',
  timeout: '请求超时',
  http: '服务端错误',
  'retries-exhausted': '重试耗尽',
  'hash-mismatch': '整体哈希不匹配',
  'chunk-missing': '分片缺失',
  'store-corrupt': '本地分片损坏',
  stale: '文件已被替换',
  cancelled: '已取消',
};

const NOOP = () => {};

function nowIso() {
  return new Date().toISOString();
}

function abortError() {
  const err = new Error('aborted');
  err.name = 'AbortError';
  return err;
}

function isAbort(err) {
  return err?.name === 'AbortError' || err?.code === 20;
}

function sleep(ms, signal) {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(abortError());
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    function onAbort() {
      clearTimeout(timer);
      reject(abortError());
    }
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

export class DownloadError extends Error {
  constructor(reason, message, detail = {}) {
    super(message || RETRY_REASONS[reason] || reason);
    this.reason = reason;
    this.detail = detail;
  }
}

export class DownloadEngine {
  /**
   * @param {object} opts
   * @param {string} opts.fileId
   * @param {string} [opts.baseUrl]
   * @param {number} [opts.concurrency]
   * @param {number} [opts.maxAttempts]
   * @param {object} [opts.store]
   * @param {typeof fetch} [opts.fetchImpl]
   * @param {number} [opts.versionCheckMs]
   */
  constructor(opts) {
    this.fileId = opts.fileId;
    this.baseUrl = (opts.baseUrl || '').replace(/\/$/, '');
    this.concurrency = Math.max(1, Math.min(16, opts.concurrency ?? 4));
    this.maxAttempts = opts.maxAttempts ?? 6;
    this.fetchImpl = opts.fetchImpl ?? ((...args) => globalThis.fetch(...args));
    this.versionCheckMs = opts.versionCheckMs ?? 4000;
    this.retryBaseMs = opts.retryBaseMs ?? 350;
    this.retryMaxMs = opts.retryMaxMs ?? 8000;
    this.requestTimeoutMs = opts.requestTimeoutMs ?? 15000;
    this.verifyStoredOnResume = opts.verifyStoredOnResume ?? true;
    this.verifyChunksOnMerge = opts.verifyChunksOnMerge ?? false;
    this._storeOverride = opts.store || null;
    this.store = null;

    this.manifest = null;
    this.state = null;
    this.status = 'idle';
    this.resultBlob = null;
    this.lastError = null;

    this._listeners = new Map();
    this._runAbort = null;
    this._stopReason = null; // 'cancel' | 'pause' | 'stale' | 'exhausted'
    this._versionDirty = false;
    this._exhausted = false;
    this._running = false;
    this._active = 0;
    this._doneBytes = 0;
    this._retryLog = [];
    this._chunkStatus = {};
    this._versionDiff = null;
    this._speed = { at: 0, bytes: 0, bps: 0 };
    this._versionTimer = null;
    this._lastProgressEmit = 0;
  }

  /* ------------------------------------------------------------ events */

  on(type, handler) {
    if (!this._listeners.has(type)) this._listeners.set(type, new Set());
    this._listeners.get(type).add(handler);
    return () => this._listeners.get(type)?.delete(handler);
  }

  _emit(type, payload) {
    const set = this._listeners.get(type);
    if (set) {
      for (const handler of [...set]) handler(payload);
    }
    const any = this._listeners.get('*');
    if (any) for (const handler of [...any]) handler({ type, payload });
  }

  /* ------------------------------------------------------------- setup */

  async init() {
    if (!this.store) this.store = this._storeOverride || (await createStore());
    if (this._running) return this.snapshot();
    this.manifest = await this._fetchJson(`${this.baseUrl}/api/files/${this.fileId}/manifest`);
    this.state = await this.store.getState(this.fileId);

    if (this.state && this.state.revision !== this.manifest.revision) {
      const oldRevision = this.state.revision;
      await this._reconcileRevision(oldRevision, this.manifest, { initial: true });
    } else if (!this.state) {
      await this._createState();
    } else {
      this._reconcileStateWithManifest();
    }

    if (this.state.status === 'completed' && this._isComplete()) this.status = 'completed';
    else if (this.state.status && this.state.status !== 'downloading') this.status = this.state.status;
    if (this.status === 'downloading' || this.status === 'verifying') {
      // Left over from a tab that was closed mid-download.
      this.status = 'paused';
      this.state.status = 'paused';
      await this.store.putState(this.fileId, this.state);
    }

    this._retryLog = Array.isArray(this.state.retryLog) ? [...this.state.retryLog] : [];
    this._recomputeDoneBytes();
    this._emit('state', this.snapshot());
    return this.snapshot();
  }

  async _createState() {
    const m = this.manifest;
    this.state = {
      fileId: this.fileId,
      name: m.name,
      mime: m.mime,
      revision: m.revision,
      etag: m.etag,
      sha256: m.sha256,
      size: m.size,
      chunkSize: m.chunkSize,
      chunkCount: m.chunkCount,
      status: 'idle',
      completed: {},
      attempts: {},
      retryLog: [],
      createdAt: nowIso(),
      updatedAt: nowIso(),
      completedAt: null,
      resumedAt: null,
      lastError: null,
      mergedSha256: null,
      versionHistory: [],
    };
    await this.store.putState(this.fileId, this.state);
  }

  _reconcileStateWithManifest() {
    const valid = {};
    for (const [idx, info] of Object.entries(this.state.completed || {})) {
      const chunk = this.manifest.chunks[Number(idx)];
      if (chunk && chunk.sha256 === info.sha256) valid[idx] = info;
    }
    this.state.completed = valid;
    Object.assign(this.state, {
      etag: this.manifest.etag,
      sha256: this.manifest.sha256,
      size: this.manifest.size,
      chunkSize: this.manifest.chunkSize,
      chunkCount: this.manifest.chunkCount,
      name: this.manifest.name,
      mime: this.manifest.mime,
    });
  }

  _recomputeDoneBytes() {
    let sum = 0;
    for (const idx of Object.keys(this.state?.completed || {})) {
      const i = Number(idx);
      sum += this.manifest?.chunks[i]?.length ?? this.state.completed[idx].size ?? 0;
    }
    this._doneBytes = sum;
    this._speed = { at: performance.now(), bytes: sum, bps: 0 };
    this._lastProgressEmit = 0;
  }

  /* ------------------------------------------------------------ resume */

  /**
   * Inspect persisted state without starting a download. Used by the UI to
   * offer "resume" before any network work happens.
   */
  async peek() {
    if (!this.store) this.store = this._storeOverride || (await createStore());
    const state = await this.store.getState(this.fileId);
    if (!state) return null;
    const completed = Object.keys(state.completed || {}).length;
    const stored = await this.store.listChunkIndexes(this.fileId);
    return {
      status: state.status,
      revision: state.revision,
      completedCount: completed,
      storedCount: stored.length,
      chunkCount: state.chunkCount || 0,
      bytesDone: Object.values(state.completed || {}).reduce((a, c) => a + (c.size || 0), 0),
      updatedAt: state.updatedAt,
      size: state.size,
      complete: state.chunkCount > 0 && completed >= state.chunkCount,
    };
  }

  /**
   * Re-hash every stored chunk against the manifest. Corrupt chunks are
   * discarded so they get re-downloaded; the rest are trusted and reused.
   */
  async verifyStoredChunks() {
    if (!this.manifest) throw new Error('init() must run before verifyStoredChunks()');
    const indexes = await this.store.listChunkIndexes(this.fileId);
    const storedSet = new Set(indexes);
    let dropped = 0;
    const broken = [];

    // State may claim chunks whose blob is gone (cleared storage, eviction).
    for (const key of Object.keys(this.state.completed)) {
      if (!storedSet.has(Number(key))) {
        delete this.state.completed[key];
        dropped++;
        broken.push({ index: Number(key), reason: 'chunk-missing' });
      }
    }

    let ok = 0;
    let checked = 0;
    for (const idx of indexes) {
      const chunk = this.manifest.chunks[idx];
      const rec = await this.store.getChunk(this.fileId, idx);
      checked++;
      if (!chunk || !rec) {
        await this.store.deleteChunk(this.fileId, idx);
        delete this.state.completed[idx];
        dropped++;
        continue;
      }
      let hash = '';
      try {
        hash = await sha256Blob(rec.blob, { chunkSize: 1024 * 1024 });
      } catch {
        hash = '';
      }
      if (hash === chunk.sha256 && rec.blob.size === chunk.length) {
        ok++;
        this.state.completed[idx] = {
          ...(this.state.completed[idx] || {}),
          sha256: hash,
          size: chunk.length,
          reused: true,
        };
        this._chunkStatus[idx] = { status: 'done', verified: true };
      } else {
        dropped++;
        broken.push({ index: idx, reason: 'store-corrupt', expected: chunk.sha256, actual: hash });
        delete this.state.completed[idx];
        this._chunkStatus[idx] = { status: 'pending' };
        await this.store.deleteChunk(this.fileId, idx);
      }
      if (checked % 4 === 0) {
        this._emit('verify', { phase: 'stored', checked, total: indexes.length, ok, dropped });
        this._emitProgress();
      }
    }
    await this.store.putState(this.fileId, this.state);
    this._recomputeDoneBytes();
    this._emit('verify', {
      phase: 'stored', checked, total: indexes.length, ok, dropped, done: true,
    });
    if (broken.length) this._emit('recover', { dropped: broken.length, chunks: broken });
    this._emitProgress();
    return { ok, dropped, checked, broken };
  }

  /* ------------------------------------------------------------ control */

  async start() {
    if (this._running) return this._runPromise || this.snapshot();
    if (!this.manifest || !this.state) await this.init();
    if (this._running) return this._runPromise || this.snapshot();

    if (this.status === 'completed' && this._isComplete() && this.resultBlob) {
      return this.snapshot();
    }
    if (this.status === 'completed' && this._isComplete()) {
      // A reload has the state but not the merged blob. Re-verify what is on
      // disk before trusting it again — a completed record is not proof that
      // the stored bytes still hash to the manifest.
      this.status = 'paused';
      this.verifyStoredOnResume = true;
    }

    this._running = true;
    this._stopReason = null;
    this._exhausted = false;
    this._versionDirty = false;
    this._runAbort = new AbortController();
    this._chunkStatus = {};
    this.state.attempts = {};
    this.state.resumedAt = this.state.resumedAt || nowIso();
    this.state.lastError = null;
    this._recomputeDoneBytes();
    this._persistState('downloading');
    this._setStatus('downloading');
    this._startVersionPoll();

    this._runPromise = (async () => {
      try {
        if (this.verifyStoredOnResume && (this._storedCount() > 0 || this._hasStoredBlobs())) {
          this._setStatus('verifying');
          const result = await this.verifyStoredChunks();
          if (!this._stopRequested()) {
            this._emit('log', {
              level: 'info',
              message: `恢复校验完成：${result.ok} 个分片可复用，${result.dropped} 个被丢弃`,
            });
          }
        }
        await this._run();
      } catch (err) {
        if (!isAbort(err) && this.status !== 'failed' && !this._stopRequested()) {
          this._fail(err.reason || 'network', err.message || String(err), err.detail || {});
        }
      } finally {
        this._stopVersionPoll();
        const stop = this._stopReason;
        this._running = false;
        this._runAbort = null;
        this._runPromise = null;
        const terminal = this.status === 'completed' || this.status === 'failed';
        if (stop === 'cancel' && !terminal) {
          this.state.resumedAt = null;
          await this._persistState('cancelled');
          this._setStatus('cancelled');
          this._emit('cancelled', this.snapshot());
        } else if (this.status === 'failed') {
          this._emit('state', this.snapshot());
        } else if (this._exhausted && this._missingIndexes().length > 0) {
          this._fail(
            'retries-exhausted',
            `${this._missingIndexes().length} 个分片重试耗尽，恢复后可从已完成分片继续`,
            { missing: this._missingIndexes().length },
          );
        } else if (
          !terminal &&
          (stop === 'pause' ||
            ['downloading', 'verifying', 'pausing', 'cancelling'].includes(this.status))
        ) {
          // pause() flips the status to 'pausing' before the run unwinds, so
          // the transitional states must land here too.
          await this._persistState('paused');
          this._setStatus('paused');
        } else {
          this._emit('state', this.snapshot());
        }
      }
    })();
    await this._runPromise;
    return this.snapshot();
  }

  _hasStoredBlobs() {
    return this._doneBytes > 0 || this._storedCount() > 0;
  }

  _storedCount() {
    return Object.keys(this.state?.completed || {}).length;
  }

  async _run() {
    for (;;) {
      const missing = this._missingIndexes();
      if (missing.length === 0) {
        const outcome = await this._finalize();
        if (outcome !== 'retry') return;
        continue;
      }
      await this._runPool(missing);
      if (this._stopRequested()) return;
      if (this._versionDirty) {
        if (!(await this._applyNewRevision())) return;
        continue;
      }
      if (this._exhausted) return;
      const still = this._missingIndexes();
      if (still.length === 0) {
        const outcome = await this._finalize();
        if (outcome !== 'retry') return;
        continue;
      }
      if (still.length >= missing.length) {
        // No progress at all and nothing left to try — avoid a hot loop.
        this._exhausted = true;
        return;
      }
    }
  }

  _missingIndexes() {
    const out = [];
    for (let i = 0; i < this.manifest.chunkCount; i++) {
      if (!this.state.completed[i]) out.push(i);
    }
    return out;
  }

  _isComplete() {
    return this.manifest && Object.keys(this.state.completed).length >= this.manifest.chunkCount;
  }

  _stopRequested() {
    return this._stopReason === 'cancel' || this._stopReason === 'pause';
  }

  async pause() {
    this._stopReason = 'pause';
    this._runAbort?.abort();
    if (!this._running) {
      await this._persistState('paused');
      this._setStatus('paused');
      return this.snapshot();
    }
    // The running start() loop persists and emits on exit; wait for it so the
    // caller never observes a half-finished state.
    this.status = 'pausing';
    this._emit('state', this.snapshot());
    await this._runPromise?.catch(NOOP);
    return this.snapshot();
  }

  async cancel() {
    this._stopReason = 'cancel';
    this._runAbort?.abort();
    this._stopVersionPoll();
    this.resultBlob = null;
    if (this._running) {
      this.status = 'cancelling';
      this._emit('state', this.snapshot());
      await this._runPromise?.catch(NOOP);
      return this.snapshot();
    }
    await this._persistState('cancelled', { resumedAt: null });
    this._setStatus('cancelled');
    this._emit('cancelled', this.snapshot());
    return this.snapshot();
  }

  async resume() {
    if (!this.manifest || !this.state) await this.init();
    this.verifyStoredOnResume = true;
    return this.start();
  }

  /** Delete all local data for this file and start from scratch. */
  async reset() {
    this._stopReason = 'cancel';
    this._runAbort?.abort();
    this._stopVersionPoll();
    // Let the current run unwind before deleting, otherwise a late chunk write
    // can land after the cleanup and resurrect orphans.
    await this._runPromise?.catch(NOOP);
    this.resultBlob = null;
    await this.store.clearFile(this.fileId);
    this.state = null;
    this.manifest = null;
    this._doneBytes = 0;
    this._retryLog = [];
    this._chunkStatus = {};
    this._versionDiff = null;
    this._exhausted = false;
    this._stopReason = null;
    this._active = 0;
    this._setStatus('idle');
    return this.snapshot();
  }

  setConcurrency(n) {
    this.concurrency = Math.max(1, Math.min(16, Math.round(n) || 1));
    this._emit('state', this.snapshot());
    return this.concurrency;
  }

  /* ------------------------------------------------------------ workers */

  async _runPool(indexes) {
    const queue = indexes.filter((i) => !this.state.completed[i]);
    let next = 0;
    const active = new Set();

    const take = () => {
      while (next < queue.length) {
        const idx = queue[next++];
        if (!this.state.completed[idx]) return idx;
      }
      return undefined;
    };

    // One chunk per worker promise, so the scheduler re-evaluates the slot
    // count after every completion and honours concurrency changes mid-run.
    const runOne = async () => {
      const idx = take();
      if (idx === undefined) return;
      await this._downloadChunk(idx);
    };

    while (next < queue.length || active.size > 0) {
      if (this._stopRequested() || this._versionDirty || this._exhausted) break;
      while (
        active.size < this.concurrency &&
        next < queue.length &&
        !this._stopRequested() &&
        !this._versionDirty &&
        !this._exhausted
      ) {
        const p = runOne().finally(() => active.delete(p));
        active.add(p);
        if (queue[next] !== undefined && this.state.completed[queue[next]]) next++;
      }
      if (active.size === 0) break;
      await Promise.race([...active]);
    }
    await Promise.allSettled([...active]);
  }

  async _downloadChunk(index) {
    const chunk = this.manifest.chunks[index];
    if (!chunk) return;
    const key = String(index);
    const startedAt = performance.now();
    const previousAttempts = this.state.attempts[key] || 0;

    for (let attempt = previousAttempts + 1; attempt <= this.maxAttempts; attempt++) {
      if (this._stopRequested() || this._versionDirty || this._exhausted) return;
      this.state.attempts[key] = attempt;
      this._chunkStatus[index] = { status: 'downloading', attempt, at: nowIso() };
      this._emit('chunk', { index, status: 'downloading', attempt, maxAttempts: this.maxAttempts });
      this._active++;
      try {
        const blob = await this._fetchChunk(chunk);
        await this.store.putChunk(this.fileId, index, blob, {
          sha256: chunk.sha256,
          size: chunk.length,
          revision: this.manifest.revision,
        });
        this.state.completed[index] = {
          sha256: chunk.sha256,
          size: chunk.length,
          attempts: attempt,
          ms: Math.round(performance.now() - startedAt),
        };
        delete this.state.attempts[key];
        this._doneBytes += chunk.length;
        this._trackSpeed();
        this._chunkStatus[index] = { status: 'done', attempt };
        await this._persistState();
        this._emit('chunk', {
          index,
          status: 'done',
          attempts: attempt,
          bytes: chunk.length,
          durationMs: Math.round(performance.now() - startedAt),
        });
        this._emitProgress();
        return;
      } catch (err) {
        if (isAbort(err)) {
          this._clearChunkTransient(index);
          return;
        }
        if (err instanceof DownloadError && err.reason === 'stale') {
          this._versionDirty = true;
          this._chunkStatus[index] = { status: 'pending', reason: 'stale' };
          this._logRetry(index, attempt, 'stale', err.message, 0);
          this._runAbort?.abort();
          return;
        }
        const reason = err.reason || 'network';
        const message = err.message || String(err);
        const final = attempt >= this.maxAttempts;

        if (final) {
          this._chunkStatus[index] = { status: 'failed', reason, message, attempt };
          this._logRetry(index, attempt, reason, message, 0, true);
          this._exhausted = true;
          this._runAbort?.abort();
          return;
        }

        const delay = this._backoff(attempt);
        this._chunkStatus[index] = {
          status: 'retrying', reason, message, attempt,
          maxAttempts: this.maxAttempts, nextRetryMs: Math.round(delay), at: nowIso(),
        };
        this._logRetry(index, attempt, reason, message, delay);
        try {
          await sleep(delay, this._runAbort?.signal);
        } catch {
          this._clearChunkTransient(index);
          return;
        }
      } finally {
        this._active = Math.max(0, this._active - 1);
      }
    }
  }

  _clearChunkTransient(index) {
    const cur = this._chunkStatus[index];
    if (cur && (cur.status === 'downloading' || cur.status === 'retrying')) {
      this._chunkStatus[index] = { status: 'pending' };
    }
  }

  _backoff(attempt) {
    const base = Math.min(this.retryMaxMs, this.retryBaseMs * 2 ** (attempt - 1));
    return base + Math.random() * Math.min(400, base * 0.4);
  }

  _logRetry(index, attempt, reason, message, delay, final = false) {
    const entry = {
      at: Date.now(),
      index,
      attempt,
      reason,
      label: RETRY_REASONS[reason] || reason,
      message,
      delayMs: Math.round(delay),
      final,
    };
    this._retryLog.push(entry);
    if (this._retryLog.length > 200) this._retryLog.shift();
    if (this.state) this.state.retryLog = this._retryLog.slice(-60);
    this._emit('retry', entry);
    this._persistState().catch(NOOP);
  }

  async _fetchChunk(chunk) {
    const expectedRange = `bytes=${chunk.offset}-${chunk.end}`;
    const url = `${this.baseUrl}/api/files/${this.fileId}/chunk/${chunk.index}?rev=${this.manifest.revision}`;
    const controller = new AbortController();
    let timedOut = false;
    const timeout = setTimeout(() => {
      timedOut = true;
      controller.abort(abortError());
    }, this.requestTimeoutMs);
    const onRunAbort = () => controller.abort(abortError());
    this._runAbort?.signal.addEventListener('abort', onRunAbort, { once: true });

    let response;
    try {
      response = await this.fetchImpl(url, {
        headers: { range: expectedRange, 'cache-control': 'no-store' },
        signal: controller.signal,
        cache: 'no-store',
      });
    } catch (err) {
      if (this._stopRequested() || this._versionDirty) throw abortError();
      if (timedOut) throw new DownloadError('timeout', '请求超时');
      throw new DownloadError('network', '网络连接中断');
    } finally {
      clearTimeout(timeout);
      this._runAbort?.signal.removeEventListener('abort', onRunAbort);
    }

    if (response.status === 409) {
      let detail = {};
      try {
        detail = await response.json();
      } catch {
        /* ignore body */
      }
      throw new DownloadError('stale', '服务端报告文件已更新', detail);
    }
    if (response.status === 503) throw new DownloadError('offline', '连接失败：服务端不可达');
    if (response.status !== 206 && response.status !== 200) {
      throw new DownloadError('http', `HTTP ${response.status}`, { status: response.status });
    }

    const revisionHeader = response.headers.get('x-file-revision');
    if (revisionHeader && Number(revisionHeader) !== this.manifest.revision) {
      throw new DownloadError('stale', `分片来自修订 r${revisionHeader}`, {
        currentRevision: Number(revisionHeader),
      });
    }

    if (response.status === 206) {
      const contentRange = response.headers.get('content-range');
      const m = /^bytes (\d+)-(\d+)\/(\d+)$/.exec(contentRange || '');
      if (!m) {
        throw new DownloadError('range-mismatch', '缺少或无法解析 Content-Range', {
          contentRange: contentRange || null,
        });
      }
      const start = Number(m[1]);
      const end = Number(m[2]);
      const total = Number(m[3]);
      if (start !== chunk.offset || end !== chunk.end || total !== this.manifest.size) {
        throw new DownloadError(
          'range-mismatch',
          `期望 ${expectedRange}/${this.manifest.size}，收到 ${contentRange}`,
          { expected: expectedRange, actual: contentRange },
        );
      }
    }

    let buffer;
    try {
      buffer = await response.arrayBuffer();
    } catch (err) {
      if (this._stopRequested() || this._versionDirty) throw abortError();
      if (timedOut) throw new DownloadError('timeout', '读取响应超时');
      throw new DownloadError('network', '传输中断，连接被重置');
    }

    if (buffer.byteLength !== chunk.length) {
      throw new DownloadError(
        'short-body',
        `分片 ${chunk.index} 收到 ${buffer.byteLength} 字节，应为 ${chunk.length}`,
        { received: buffer.byteLength, expected: chunk.length },
      );
    }

    const declared = response.headers.get('content-length');
    if (declared !== null && Number(declared) !== buffer.byteLength) {
      throw new DownloadError('short-body', 'Content-Length 与实际响应体不一致', {
        declared: Number(declared),
        received: buffer.byteLength,
      });
    }

    const actual = sha256Hex(new Uint8Array(buffer));
    if (actual !== chunk.sha256) {
      throw new DownloadError('checksum', `分片 ${chunk.index} 校验和不匹配`, {
        expected: chunk.sha256,
        actual,
      });
    }
    return new Blob([buffer], { type: 'application/octet-stream' });
  }

  /* --------------------------------------------------------- revisions */

  _startVersionPoll() {
    this._stopVersionPoll();
    if (!this.versionCheckMs) return;
    this._versionTimer = setInterval(async () => {
      if (!this._running || this._stopRequested() || this._versionDirty) return;
      if (typeof document !== 'undefined' && document.hidden) return;
      try {
        const info = await this._fetchJson(`${this.baseUrl}/api/files/${this.fileId}`);
        if (info.revision !== this.manifest.revision) {
          this._versionDirty = true;
          this._logRetry(-1, 0, 'stale', `服务端已发布 r${info.revision}`, 0);
          this._runAbort?.abort();
        }
      } catch {
        /* polling is best-effort */
      }
    }, this.versionCheckMs);
  }

  _stopVersionPoll() {
    if (this._versionTimer) clearInterval(this._versionTimer);
    this._versionTimer = null;
  }

  /** @returns {Promise<boolean>} true if a new run should continue */
  async _applyNewRevision() {
    this._versionDirty = false;
    if (!this._running) return false;
    const oldManifest = this.manifest;
    let newManifest;
    try {
      newManifest = await this._fetchJson(`${this.baseUrl}/api/files/${this.fileId}/manifest`);
    } catch (err) {
      this._fail('network', '无法获取新版本清单', { detail: String(err) });
      return false;
    }
    const diff = this._diffManifests(oldManifest, newManifest);
    this.manifest = newManifest;
    await this._reconcileRevision(oldManifest.revision, newManifest, {
      diff,
      initial: false,
    });
    // The previous AbortController was aborted to stop in-flight workers.
    this._runAbort = new AbortController();
    this._exhausted = false;
    this._chunkStatus = {};
    this.state.attempts = {};
    this._recomputeDoneBytes();
    this._persistState('downloading');
    this._setStatus('downloading');
    this._emit('version', { fileId: this.fileId, diff, snapshot: this.snapshot() });
    return true;
  }

  async _reconcileRevision(oldRevision, newManifest, { diff = null, initial = false } = {}) {
    const hadState = !!this.state;
    const previousCompleted = { ...(this.state?.completed || {}) };
    const previousStored = await this.store.listChunkIndexes(this.fileId);
    const valid = {};
    let reused = 0;
    let discarded = 0;

    for (let i = 0; i < newManifest.chunkCount; i++) {
      const prev = previousCompleted[i];
      if (prev && prev.sha256 === newManifest.chunks[i].sha256) {
        // The bytes already on disk are still correct for the new revision.
        valid[i] = initial ? prev : { ...prev, reused: true, reusedFrom: oldRevision };
        reused++;
      } else if (prev) {
        discarded++;
      }
    }

    // Delete stored blobs whose bytes do not belong to the new revision.
    for (const idx of previousStored) {
      const chunk = newManifest.chunks[idx];
      const rec = await this.store.getChunk(this.fileId, idx);
      if (!chunk || !rec || rec.meta?.sha256 !== chunk.sha256) {
        await this.store.deleteChunk(this.fileId, idx);
        delete valid[idx];
      }
    }

    if (!hadState) await this._createState();
    this.state.completed = valid;
    Object.assign(this.state, {
      revision: newManifest.revision,
      etag: newManifest.etag,
      sha256: newManifest.sha256,
      size: newManifest.size,
      chunkSize: newManifest.chunkSize,
      chunkCount: newManifest.chunkCount,
      name: newManifest.name,
      mime: newManifest.mime,
      attempts: {},
      completedAt: null,
      mergedSha256: null,
      lastError: null,
    });

    if (!initial) {
      const oldCompleted = Object.values(previousCompleted).length;
      const entry = {
        at: nowIso(),
        fromRevision: oldRevision,
        toRevision: newManifest.revision,
        diff: diff || null,
        reused,
        discarded,
        // Chunks completed under the old revision that no longer apply.
        obsolete: Math.max(0, oldCompleted - reused),
      };
      this.state.versionHistory = [...(this.state.versionHistory || []), entry].slice(-10);
      this._versionDiff = diff;
    }
    this._recomputeDoneBytes();
    await this.store.putState(this.fileId, this.state);
  }

  _diffManifests(prev, next) {
    const chunks = [];
    let reused = 0;
    let changed = 0;
    let added = 0;
    let removed = 0;
    const limit = Math.max(prev.chunkCount, next.chunkCount);
    for (let i = 0; i < limit; i++) {
      const a = prev.chunks[i];
      const b = next.chunks[i];
      if (a && b) {
        if (a.sha256 === b.sha256) {
          reused++;
          chunks.push({ index: i, status: 'same' });
        } else {
          changed++;
          chunks.push({ index: i, status: 'changed' });
        }
      } else if (b) {
        added++;
        chunks.push({ index: i, status: 'added' });
      } else if (a) {
        removed++;
        chunks.push({ index: i, status: 'removed' });
      }
    }
    return {
      fromRevision: prev.revision,
      toRevision: next.revision,
      fromSize: prev.size,
      toSize: next.size,
      sizeDelta: next.size - prev.size,
      fromChunkCount: prev.chunkCount,
      toChunkCount: next.chunkCount,
      reused,
      changed,
      added,
      removed,
      fromEtag: prev.etag,
      toEtag: next.etag,
      chunks,
    };
  }

  /* ------------------------------------------------------------- merge */

  async _mergeBlob() {
    const parts = [];
    for (let i = 0; i < this.manifest.chunkCount; i++) {
      const rec = await this.store.getChunk(this.fileId, i);
      if (!rec) throw new DownloadError('chunk-missing', `缺少分片 ${i}`, { index: i });
      parts.push(rec.blob);
    }
    return new Blob(parts, { type: this.manifest.mime || 'application/octet-stream' });
  }

  /**
   * Verify the assembled file end-to-end.
   * @returns {Promise<'done'|'retry'|'failed'>}
   */
  async _finalize() {
    // Last line of defence: the server may have swapped the file while the
    // last chunks were in flight without any 409 being observed.
    try {
      const info = await this._fetchJson(`${this.baseUrl}/api/files/${this.fileId}`);
      if (info.revision !== this.manifest.revision) {
        this._versionDirty = true;
        return (await this._applyNewRevision()) ? 'retry' : 'failed';
      }
    } catch (err) {
      if (isAbort(err) || this._stopRequested()) return 'failed';
      this._fail('network', '合并前无法确认文件版本', { detail: String(err) });
      return 'failed';
    }

    this._setStatus('verifying');
    this._emit('verify', { phase: 'merge', started: true, total: this.manifest.size });

    if (this.verifyChunksOnMerge) {
      const result = await this.verifyStoredChunks();
      if (result.dropped > 0) {
        this._emit('log', {
          level: 'warn',
          message: `合并前复检丢弃 ${result.dropped} 个损坏分片，将重新下载`,
        });
        return 'retry';
      }
    }

    let blob;
    try {
      blob = await this._mergeBlob();
    } catch (err) {
      if (err instanceof DownloadError && err.reason === 'chunk-missing') {
        this._emit('log', {
          level: 'warn',
          message: `分片 ${err.detail.index} 在本地缺失，将重新下载`,
        });
        delete this.state.completed[err.detail.index];
        this._recomputeDoneBytes();
        await this._persistState('downloading');
        this._setStatus('downloading');
        return 'retry';
      }
      this._fail('chunk-missing', '合并分片失败', { detail: String(err) });
      return 'failed';
    }

    this._emit('verify', { phase: 'merge', hashing: true, total: blob.size });
    let actual;
    try {
      actual = await sha256Blob(blob, {
        chunkSize: 4 * 1024 * 1024,
        onProgress: (done, total) => this._emit('verify-progress', { done, total }),
      });
    } catch (err) {
      if (isAbort(err) || this._stopRequested()) return 'failed';
      this._fail('hash-mismatch', '整体哈希计算失败', { detail: String(err) });
      return 'failed';
    }

    if (actual !== this.manifest.sha256) {
      this._emit('verify', {
        phase: 'merge', ok: false,
        expected: this.manifest.sha256, actual,
      });
      this.state.completedAt = null;
      this._fail('hash-mismatch', '合并后整体哈希不匹配，分片已保留用于重试', {
        expected: this.manifest.sha256,
        actual,
      });
      return 'failed';
    }

    this.resultBlob = blob;
    this.state.mergedSha256 = actual;
    this.state.completedAt = nowIso();
    this.state.resumedAt = null;
    this.state.lastError = null;
    this._persistState('completed');
    this._emit('verify', { phase: 'merge', ok: true, expected: this.manifest.sha256, actual });
    this._setStatus('completed');
    this._emit('completed', {
      blob,
      sha256: actual,
      size: blob.size,
      revision: this.manifest.revision,
      snapshot: this.snapshot(),
    });
    return 'done';
  }

  /* ------------------------------------------------------------- utils */

  async _fetchJson(url) {
    const res = await this.fetchImpl(url, {
      headers: { accept: 'application/json', 'cache-control': 'no-store' },
      cache: 'no-store',
    });
    if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
    return res.json();
  }

  _trackSpeed() {
    const t = performance.now();
    const dt = t - this._speed.at;
    if (dt >= 400) {
      this._speed = {
        at: t,
        bytes: this._doneBytes,
        bps: ((this._doneBytes - this._speed.bytes) / dt) * 1000,
      };
    }
  }

  _emitProgress() {
    const t = performance.now();
    if (t - this._lastProgressEmit < 80) return;
    this._lastProgressEmit = t;
    this._emit('progress', this.snapshot());
  }

  _setStatus(status) {
    this.status = status;
    if (this.state) {
      this.state.status = status;
      this.state.updatedAt = nowIso();
    }
    this._emit('state', this.snapshot());
  }

  async _persistState(status, extra = {}) {
    if (!this.state) return;
    if (status) this.state.status = status;
    this.state.updatedAt = nowIso();
    Object.assign(this.state, extra);
    const snapshot = JSON.parse(JSON.stringify({
      ...this.state,
      completed: this.state.completed,
    }));
    const fileId = this.fileId;
    // Serialise writes: IndexedDB transactions may otherwise commit out of
    // order and a stale snapshot could resurrect deleted completions.
    this._persistChain = (this._persistChain || Promise.resolve())
      .then(() => this.store.putState(fileId, snapshot))
      .catch((err) => {
        this._emit('log', { level: 'error', message: `状态保存失败: ${err?.message || err}` });
      });
    return this._persistChain;
  }

  _fail(reason, message, detail = {}) {
    this.lastError = { reason, message, label: RETRY_REASONS[reason] || reason, ...detail };
    this.state.lastError = this.lastError;
    this.status = 'failed';
    this.state.status = 'failed';
    this.state.updatedAt = nowIso();
    this._persistState('failed');
    this._emit('failed', { ...this.lastError, snapshot: this.snapshot() });
    this._emit('state', this.snapshot());
  }

  snapshot() {
    const m = this.manifest;
    const completed = this.state ? Object.keys(this.state.completed).length : 0;
    const chunkCount = m?.chunkCount ?? this.state?.chunkCount ?? 0;
    const total = m?.size ?? 0;
    const doneBytes = this._doneBytes;
    const bps = this._speed.bps;
    const remaining = Math.max(0, total - doneBytes);
    return {
      fileId: this.fileId,
      name: m?.name || this.state?.name || this.fileId,
      revision: m?.revision ?? this.state?.revision ?? 1,
      etag: m?.etag ?? this.state?.etag ?? null,
      sha256: m?.sha256 ?? this.state?.sha256 ?? null,
      size: total,
      chunkSize: m?.chunkSize ?? this.state?.chunkSize ?? 0,
      chunkCount,
      completed,
      completedMap: this.state ? { ...this.state.completed } : {},
      chunkStatus: { ...this._chunkStatus },
      bytesDone: doneBytes,
      percent: total > 0 ? Math.min(100, (doneBytes / total) * 100) : 0,
      status: this.status,
      concurrency: this.concurrency,
      active: this._active,
      speedBps: bps,
      etaMs: bps > 1024 ? (remaining / bps) * 1000 : null,
      retries: this._retryLog.slice(-60),
      versionDiff: this._versionDiff,
      versionHistory: this.state?.versionHistory || [],
      mergedSha256: this.state?.mergedSha256 || null,
      completedAt: this.state?.completedAt || null,
      lastError: this.lastError || this.state?.lastError || null,
      resumedAt: this.state?.resumedAt || null,
    };
  }
}
