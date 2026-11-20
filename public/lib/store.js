/**
 * Storage abstraction for chunk data + download state.
 *
 * Two implementations:
 *  - IndexedDbStore: durable across reloads (browser default)
 *  - MemoryStore: used by tests and when IndexedDB is unavailable
 *
 * Chunk blobs are stored one record per chunk, keyed by [fileId, rev?, index].
 * State is stored separately, so cancelling or reloading keeps partial data.
 */

const DB_NAME = 'resumable-downloads';
const DB_VERSION = 1;
const STORE_STATE = 'downloads';
const STORE_CHUNKS = 'chunks';

function reqToPromise(request) {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

export class MemoryStore {
  constructor() {
    this.states = new Map();
    this.chunks = new Map();
    this.kind = 'memory';
  }

  static available() {
    return true;
  }

  _chunkKey(fileId, index) {
    return `${fileId}::${index}`;
  }

  async getState(fileId) {
    const rec = this.states.get(fileId);
    return rec ? structuredCloneSafe(rec) : null;
  }

  async putState(fileId, state) {
    this.states.set(fileId, structuredCloneSafe(state));
  }

  async deleteState(fileId) {
    this.states.delete(fileId);
  }

  async putChunk(fileId, index, blob, meta) {
    this.chunks.set(this._chunkKey(fileId, index), { blob, meta: { ...meta } });
  }

  async getChunk(fileId, index) {
    return this.chunks.get(this._chunkKey(fileId, index)) || null;
  }

  async hasChunk(fileId, index) {
    return this.chunks.has(this._chunkKey(fileId, index));
  }

  async listChunkIndexes(fileId) {
    const prefix = `${fileId}::`;
    const out = [];
    for (const key of this.chunks.keys()) {
      if (key.startsWith(prefix)) out.push(Number(key.slice(prefix.length)));
    }
    return out.sort((a, b) => a - b);
  }

  async deleteChunk(fileId, index) {
    this.chunks.delete(this._chunkKey(fileId, index));
  }

  /** Remove chunks of this file whose sha256 no longer appears in `validHashes`. */
  async pruneChunks(fileId, validHashes) {
    const prefix = `${fileId}::`;
    let removed = 0;
    for (const [key, rec] of this.chunks) {
      if (!key.startsWith(prefix)) continue;
      if (!validHashes.has(rec.meta.sha256)) {
        this.chunks.delete(key);
        removed++;
      }
    }
    return removed;
  }

  async clearFile(fileId) {
    await this.deleteState(fileId);
    const prefix = `${fileId}::`;
    for (const key of [...this.chunks.keys()]) {
      if (key.startsWith(prefix)) this.chunks.delete(key);
    }
  }

  async clearAll() {
    this.states.clear();
    this.chunks.clear();
  }
}

export class IndexedDbStore {
  constructor(db) {
    this.db = db;
    this.kind = 'indexeddb';
  }

  static available() {
    return typeof indexedDB !== 'undefined';
  }

  static async open() {
    if (!IndexedDbStore.available()) throw new Error('IndexedDB unavailable');
    if (IndexedDbStore._shared) return IndexedDbStore._shared;
    const db = await new Promise((resolve, reject) => {
      const open = indexedDB.open(DB_NAME, DB_VERSION);
      open.onupgradeneeded = () => {
        const database = open.result;
        if (!database.objectStoreNames.contains(STORE_STATE)) {
          database.createObjectStore(STORE_STATE, { keyPath: 'fileId' });
        }
        if (!database.objectStoreNames.contains(STORE_CHUNKS)) {
          const chunks = database.createObjectStore(STORE_CHUNKS, { keyPath: 'key' });
          chunks.createIndex('byFile', 'fileId', { unique: false });
        }
      };
      open.onsuccess = () => resolve(open.result);
      open.onerror = () => reject(open.error);
      open.onblocked = () => reject(new Error('IndexedDB open blocked'));
    });
    IndexedDbStore._shared = new IndexedDbStore(db);
    return IndexedDbStore._shared;
  }

  _tx(storeName, mode, fn) {
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction(storeName, mode);
      const store = tx.objectStore(storeName);
      let result;
      try {
        result = fn(store);
      } catch (err) {
        reject(err);
        return;
      }
      tx.oncomplete = () => resolve(result);
      tx.onerror = () => reject(tx.error);
      tx.onabort = () => reject(tx.error || new Error('transaction aborted'));
    });
  }

  async getState(fileId) {
    const rec = await reqToPromise(
      this.db.transaction(STORE_STATE, 'readonly').objectStore(STORE_STATE).get(fileId),
    );
    return rec ? rec.state : null;
  }

  async putState(fileId, state) {
    const clone = structuredCloneSafe(state);
    await this._tx(STORE_STATE, 'readwrite', (store) => store.put({ fileId, state: clone }));
  }

  async deleteState(fileId) {
    await this._tx(STORE_STATE, 'readwrite', (store) => store.delete(fileId));
  }

  _chunkKey(fileId, index) {
    return `${fileId}::${index}`;
  }

  async putChunk(fileId, index, blob, meta) {
    await this._tx(STORE_CHUNKS, 'readwrite', (store) =>
      store.put({
        key: this._chunkKey(fileId, index),
        fileId,
        index,
        blob,
        meta: { ...meta },
      }),
    );
  }

  async getChunk(fileId, index) {
    const rec = await reqToPromise(
      this.db.transaction(STORE_CHUNKS, 'readonly').objectStore(STORE_CHUNKS).get(this._chunkKey(fileId, index)),
    );
    return rec ? { blob: rec.blob, meta: rec.meta } : null;
  }

  async hasChunk(fileId, index) {
    const count = await reqToPromise(
      this.db.transaction(STORE_CHUNKS, 'readonly').objectStore(STORE_CHUNKS).count(this._chunkKey(fileId, index)),
    );
    return count > 0;
  }

  async listChunkIndexes(fileId) {
    const items = await reqToPromise(
      this.db
        .transaction(STORE_CHUNKS, 'readonly')
        .objectStore(STORE_CHUNKS)
        .index('byFile')
        .getAllKeys(IDBKeyRange.only(fileId)),
    );
    const prefix = `${fileId}::`;
    return items
      .map((key) => Number(String(key).slice(prefix.length)))
      .filter((n) => Number.isInteger(n))
      .sort((a, b) => a - b);
  }

  async deleteChunk(fileId, index) {
    await this._tx(STORE_CHUNKS, 'readwrite', (store) => store.delete(this._chunkKey(fileId, index)));
  }

  async pruneChunks(fileId, validHashes) {
    const records = await reqToPromise(
      this.db
        .transaction(STORE_CHUNKS, 'readonly')
        .objectStore(STORE_CHUNKS)
        .index('byFile')
        .getAll(IDBKeyRange.only(fileId)),
    );
    const stale = records.filter((rec) => !validHashes.has(rec.meta?.sha256));
    if (!stale.length) return 0;
    await this._tx(STORE_CHUNKS, 'readwrite', (store) => {
      for (const rec of stale) store.delete(rec.key);
    });
    return stale.length;
  }

  async clearFile(fileId) {
    await this.deleteState(fileId);
    const keys = await reqToPromise(
      this.db
        .transaction(STORE_CHUNKS, 'readonly')
        .objectStore(STORE_CHUNKS)
        .index('byFile')
        .getAllKeys(IDBKeyRange.only(fileId)),
    );
    if (keys.length) {
      await this._tx(STORE_CHUNKS, 'readwrite', (store) => {
        for (const key of keys) store.delete(key);
      });
    }
  }

  async clearAll() {
    await this._tx(STORE_STATE, 'readwrite', (store) => store.clear());
    await this._tx(STORE_CHUNKS, 'readwrite', (store) => store.clear());
  }
}

function structuredCloneSafe(value) {
  if (typeof structuredClone === 'function') {
    try {
      return structuredClone(value);
    } catch {
      /* fall through to JSON */
    }
  }
  return JSON.parse(JSON.stringify(value));
}

/** Prefer IndexedDB, fall back to memory (e.g. private mode / tests). */
export async function createStore(forceMemory = false) {
  if (!forceMemory && IndexedDbStore.available()) {
    try {
      return await IndexedDbStore.open();
    } catch (err) {
      console.warn('[store] IndexedDB unavailable, using memory store', err);
    }
  }
  return new MemoryStore();
}
