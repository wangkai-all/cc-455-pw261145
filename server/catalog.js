/**
 * Deterministic file content + chunk manifests.
 *
 * The demo needs a "large file" that can be regenerated on demand without
 * keeping it in memory, while staying byte-for-byte stable so chunk hashes
 * are meaningful. Content is a deterministic PRNG stream addressed by page
 * index; each revision patches a subset of pages and appends to the tail.
 */

import crypto from 'node:crypto';

export const PAGE_SIZE = 64 * 1024;

function fnv1a(str) {
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h >>> 0;
}

function isPatchedPage(pageIndex, revision) {
  if (revision <= 1) return false;
  if (pageIndex === 0) return true;
  return pageIndex % 13 === revision % 13;
}

/** Fill `out` with the deterministic base stream for a page. */
function fillBasePage(out, seedName, pageIndex) {
  let s = fnv1a(`${seedName}:page:${pageIndex}`) >>> 0;
  if (s === 0) s = 0x6d2b79f5;
  const dv = new DataView(out.buffer, out.byteOffset, out.byteLength);
  let i = 0;
  const end = out.byteLength - (out.byteLength % 4);
  for (; i < end; i += 4) {
    s ^= s << 13;
    s >>>= 0;
    s ^= s >>> 17;
    s ^= s << 5;
    s >>>= 0;
    dv.setUint32(i, s, true);
  }
  for (; i < out.byteLength; i++) out[i] = s & 0xff;
}

function applyRevisionPatch(out, revision) {
  let s = fnv1a(`revision:${revision}`) >>> 0 || 0x2545f491;
  for (let i = 0; i < out.byteLength; i += 4) {
    s ^= s << 13;
    s >>>= 0;
    s ^= s >>> 17;
    s ^= s << 5;
    s >>>= 0;
    out[i] ^= s & 0xff;
    if (i + 1 < out.byteLength) out[i + 1] ^= (s >>> 8) & 0xff;
  }
}

/** Bytes for one 64 KiB page at a given revision. */
export function pageBytes(seedName, pageIndex, revision) {
  const out = new Uint8Array(PAGE_SIZE);
  fillBasePage(out, seedName, pageIndex);
  if (isPatchedPage(pageIndex, revision)) applyRevisionPatch(out, revision);
  return out;
}

/** Generate `length` bytes starting at `offset` for a revision. */
export function readRange(seedName, revision, offset, length) {
  const out = new Uint8Array(length);
  let written = 0;
  let pos = offset;
  while (written < length) {
    const pageIndex = Math.floor(pos / PAGE_SIZE);
    const inPage = pos % PAGE_SIZE;
    const take = Math.min(PAGE_SIZE - inPage, length - written);
    const page = pageBytes(seedName, pageIndex, revision);
    out.set(page.subarray(inPage, inPage + take), written);
    written += take;
    pos += take;
  }
  return out;
}

export function sizeForRevision(baseSize, growth, revision) {
  return baseSize + (revision - 1) * growth;
}

function sha256(bytes) {
  return crypto.createHash('sha256').update(bytes).digest('hex');
}

/**
 * Build a manifest: whole-file hash plus per-chunk hashes.
 * Yields between chunks so the HTTP server stays responsive on big files.
 */
export async function buildManifest({ id, name, mime, seed, baseSize, growth, chunkSize, revision }) {
  const size = sizeForRevision(baseSize, growth, revision);
  const chunks = [];
  const whole = crypto.createHash('sha256');
  let index = 0;
  for (let offset = 0; offset < size; offset += chunkSize) {
    const length = Math.min(chunkSize, size - offset);
    const bytes = readRange(seed, revision, offset, length);
    whole.update(bytes);
    chunks.push({
      index,
      offset,
      length,
      end: offset + length - 1,
      sha256: sha256(bytes),
    });
    index++;
    if (index % 8 === 0) await new Promise((resolve) => setImmediate(resolve));
  }
  const sha256Hex = whole.digest('hex');
  const etag = `"${id}-r${revision}-${sha256Hex.slice(0, 12)}"`;
  return {
    id,
    name,
    mime,
    size,
    chunkSize,
    chunkCount: chunks.length,
    revision,
    etag,
    sha256: sha256Hex,
    updatedAt: Date.now(),
    chunks,
  };
}
