/**
 * Incremental SHA-256, usable in the browser and in Node.
 *
 * Kept dependency-free on purpose: verifying a multi-gigabyte file with
 * crypto.subtle would require materialising the whole file in one buffer.
 * This implementation can be fed chunk by chunk, so the merged file is hashed
 * exactly the same way the server hashed it.
 */

const K = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1,
  0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
  0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
  0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
  0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
  0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
  0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
  0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
  0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
]);

const IV = new Uint32Array([
  0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
  0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
]);

const rotr = (x, n) => (x >>> n) | (x << (32 - n));

export class Sha256 {
  constructor() {
    this.h = IV.slice();
    this.block = new Uint8Array(64);
    this.blockLen = 0;
    this.totalBytes = 0;
    this.w = new Uint32Array(64);
  }

  _compress(block, off) {
    const w = this.w;
    const h = this.h;
    for (let i = 0; i < 16; i++) {
      const p = off + i * 4;
      w[i] =
        ((block[p] << 24) | (block[p + 1] << 16) | (block[p + 2] << 8) | block[p + 3]) >>> 0;
    }
    for (let i = 16; i < 64; i++) {
      const x = w[i - 15];
      const y = w[i - 2];
      const s0 = (rotr(x, 7) ^ rotr(x, 18) ^ (x >>> 3)) >>> 0;
      const s1 = (rotr(y, 17) ^ rotr(y, 19) ^ (y >>> 10)) >>> 0;
      w[i] = (w[i - 16] + s0 + w[i - 7] + s1) >>> 0;
    }

    let a = h[0], b = h[1], c = h[2], d = h[3];
    let e = h[4], f = h[5], g = h[6], hh = h[7];

    for (let i = 0; i < 64; i++) {
      const S1 = (rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25)) >>> 0;
      const ch = ((e & f) ^ (~e & g)) >>> 0;
      const t1 = (hh + S1 + ch + K[i] + w[i]) >>> 0;
      const S0 = (rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22)) >>> 0;
      const maj = ((a & b) ^ (a & c) ^ (b & c)) >>> 0;
      const t2 = (S0 + maj) >>> 0;

      hh = g;
      g = f;
      f = e;
      e = (d + t1) >>> 0;
      d = c;
      c = b;
      b = a;
      a = (t1 + t2) >>> 0;
    }

    h[0] = (h[0] + a) >>> 0;
    h[1] = (h[1] + b) >>> 0;
    h[2] = (h[2] + c) >>> 0;
    h[3] = (h[3] + d) >>> 0;
    h[4] = (h[4] + e) >>> 0;
    h[5] = (h[5] + f) >>> 0;
    h[6] = (h[6] + g) >>> 0;
    h[7] = (h[7] + hh) >>> 0;
  }

  update(data) {
    if (!(data instanceof Uint8Array)) {
      if (data instanceof ArrayBuffer) data = new Uint8Array(data);
      else throw new TypeError('Sha256.update expects Uint8Array or ArrayBuffer');
    }
    this.totalBytes += data.byteLength;
    let offset = 0;

    if (this.blockLen > 0) {
      const take = Math.min(64 - this.blockLen, data.byteLength);
      this.block.set(data.subarray(0, take), this.blockLen);
      this.blockLen += take;
      offset += take;
      if (this.blockLen === 64) {
        this._compress(this.block, 0);
        this.blockLen = 0;
      }
    }

    const fullEnd = data.byteLength - ((data.byteLength - offset) % 64);
    for (let p = offset; p < fullEnd; p += 64) this._compress(data, p);
    offset = fullEnd;

    if (offset < data.byteLength) {
      this.block.set(data.subarray(offset), 0);
      this.blockLen = data.byteLength - offset;
    }
    return this;
  }

  digest() {
    // Work on a copy so update() can continue after digest().
    const clone = new Sha256();
    clone.h.set(this.h);
    clone.block.set(this.block);
    clone.blockLen = this.blockLen;
    clone.totalBytes = this.totalBytes;
    clone._finish();
    const out = new Uint8Array(32);
    for (let i = 0; i < 8; i++) {
      const v = clone.h[i];
      out[i * 4] = v >>> 24;
      out[i * 4 + 1] = (v >>> 16) & 0xff;
      out[i * 4 + 2] = (v >>> 8) & 0xff;
      out[i * 4 + 3] = v & 0xff;
    }
    return out;
  }

  _finish() {
    const bits = BigInt(this.totalBytes) * 8n;
    // Append 0x80, then pad with zeroes to 56 mod 64, then 8-byte big-endian length.
    const padStart = this.blockLen;
    this.block[padStart] = 0x80;
    this.blockLen = padStart + 1;
    if (this.blockLen > 56) {
      this.block.fill(0, this.blockLen);
      this._compress(this.block, 0);
      this.blockLen = 0;
    }
    this.block.fill(0, this.blockLen, 56);
    const dv = new DataView(this.block.buffer, this.block.byteOffset, 64);
    dv.setBigUint64(56, bits, false);
    this._compress(this.block, 0);
  }

  digestHex() {
    return bytesToHex(this.digest());
  }
}

export function sha256Hex(data) {
  return new Sha256().update(data).digestHex();
}

export function bytesToHex(bytes) {
  let out = '';
  for (let i = 0; i < bytes.length; i++) out += bytes[i].toString(16).padStart(2, '0');
  return out;
}

const defaultYield = () => new Promise((resolve) => setTimeout(resolve, 0));

/**
 * Hash a Blob without loading all of it at once.
 * @param {Blob} blob
 * @param {{ chunkSize?: number, onProgress?: (done:number,total:number)=>void, yieldFn?: ()=>Promise<void> }} [opts]
 */
export async function sha256Blob(blob, opts = {}) {
  const chunkSize = opts.chunkSize ?? 4 * 1024 * 1024;
  const onProgress = opts.onProgress;
  const yieldFn = opts.yieldFn ?? defaultYield;
  const hasher = new Sha256();
  const total = blob.size;
  let done = 0;
  while (done < total) {
    const end = Math.min(total, done + chunkSize);
    const slice = blob.slice(done, end);
    hasher.update(new Uint8Array(await slice.arrayBuffer()));
    done = end;
    onProgress?.(done, total);
    if (done < total) await yieldFn();
  }
  if (total === 0) onProgress?.(0, 0);
  return hasher.digestHex();
}
