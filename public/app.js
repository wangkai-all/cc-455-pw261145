/**
 * Dashboard controller: one engine per file, all sharing one store and the
 * server's fault settings. Rendering is keyed and diffed — the chunk grid
 * reuses its DOM nodes so 10k-chunk files don't thrash the layout.
 */

import { DownloadEngine, RETRY_REASONS } from './lib/engine.js';
import { createStore } from './lib/store.js';

const $ = (id) => document.getElementById(id);
const fmtBytes = (n) => {
  if (!Number.isFinite(n) || n < 0) return '—';
  if (n < 1024) return `${n} B`;
  const units = ['KiB', 'MiB', 'GiB', 'TiB'];
  let v = n / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(v >= 100 ? 0 : v >= 10 ? 1 : 2)} ${units[i]}`;
};
const fmtSpeed = (bps) => (bps > 1024 ? `${fmtBytes(bps)}/s` : '—');
const fmtEta = (ms) => {
  if (!Number.isFinite(ms) || ms <= 0) return '—';
  const s = Math.round(ms / 1000);
  if (s < 60) return `${s} 秒`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m} 分 ${s % 60} 秒`;
  return `${Math.floor(m / 60)} 小时 ${m % 60} 分`;
};
const fmtTime = (ts) => {
  const d = new Date(ts);
  return d.toLocaleTimeString('zh-CN', { hour12: false });
};
const fmtDateTime = (iso) => {
  if (!iso) return '—';
  const d = new Date(iso);
  return d.toLocaleString('zh-CN', { hour12: false, month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' });
};
const el = (tag, cls, text) => {
  const node = document.createElement(tag);
  if (cls) node.className = cls;
  if (text !== undefined) node.textContent = text;
  return node;
};

const STATUS_LABELS = {
  idle: '空闲',
  downloading: '下载中',
  verifying: '校验中',
  pausing: '暂停中',
  cancelling: '取消中',
  paused: '已暂停',
  cancelled: '已取消',
  failed: '失败',
  completed: '已完成',
};

const CHUNK_LABELS = {
  pending: '待下载',
  downloading: '下载中',
  retrying: '重试中',
  failed: '失败',
  done: '已完成',
};

const app = {
  store: null,
  files: [],
  engines: new Map(), // fileId -> DownloadEngine
  snapshots: new Map(), // fileId -> snapshot
  peeks: new Map(), // fileId -> peek() result
  activeId: null,
  faults: null,
  seed: null,
  view: 'grid',
  chunkNodes: new Map(),
  versionNodes: new Map(),
  timeline: [],
  eventSource: null,
  faultDebounce: null,
  urlCache: new Map(),
  renderQueued: false,
};

/* ------------------------------------------------------------------ boot */

async function boot() {
  applyStoredTheme();
  wireStaticControls();
  app.store = await createStore();
  await refreshFiles();
  await connectEvents();

  const params = new URLSearchParams(location.search);
  const wanted = params.get('file');
  app.activeId = app.files.some((f) => f.id === wanted) ? wanted : app.files[0]?.id;
  if (!app.activeId) {
    toast('bad', '无法加载文件列表', '请确认服务端已启动');
    return;
  }
  for (const file of app.files) await ensureEngine(file.id);
  await selectFile(app.activeId, { history: false });
  startAllPeeks();
}

async function refreshFiles() {
  const res = await fetch('/api/files', { cache: 'no-store' });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  app.files = data.files;
  app.faults = data.faults;
  app.seed = data.seed;
  syncFaultControls();
  renderFileTabs();
  renderFileList();
}

async function ensureEngine(fileId) {
  if (app.engines.has(fileId)) return app.engines.get(fileId);
  const engine = new DownloadEngine({
    fileId,
    store: app.store,
    concurrency: Number($('concurrency').value) || 4,
    versionCheckMs: 3500,
  });
  app.engines.set(fileId, engine);
  wireEngine(engine);
  await engine.init();
  app.snapshots.set(fileId, engine.snapshot());
  return engine;
}

function wireEngine(engine) {
  const id = engine.fileId;
  const update = () => {
    app.snapshots.set(id, engine.snapshot());
    scheduleRender();
  };
  engine.on('state', update);
  engine.on('progress', update);
  engine.on('chunk', (payload) => {
    if (payload.status === 'done' && payload.attempts > 1) {
      pushTimeline('good', `分片 #${payload.index} 第 ${payload.attempts} 次尝试校验通过`);
    } else if (payload.status === 'failed') {
      pushTimeline('bad', `分片 #${payload.index} 重试耗尽：${RETRY_REASONS[payload.reason] || payload.reason}`);
    }
    update();
  });
  engine.on('retry', (entry) => {
    if (id === app.activeId && entry.index >= 0) {
      pushTimeline('warn', `#${entry.index} ${entry.label}${entry.final ? '（放弃）' : ''}`);
    }
    update();
  });
  engine.on('version', ({ diff }) => {
    pushTimeline(
      'info',
      `文件已更新到 r${diff.toRevision}：复用 ${diff.reused}，作废 ${diff.changed + diff.removed}，新增 ${diff.added}`,
    );
    toast(
      'warn',
      '服务端替换了文件',
      `r${diff.fromRevision} → r${diff.toRevision}，${diff.reused} 个分片校验和一致可复用`,
    );
    refreshFiles().catch(() => {});
    update();
  });
  engine.on('verify', (payload) => {
    if (payload.phase !== 'merge') return;
    if (payload.started) pushTimeline('info', '开始合并并计算整体 SHA-256');
    if (payload.ok === true) pushTimeline('good', '整体哈希校验通过');
    if (payload.ok === false) pushTimeline('bad', '整体哈希不匹配，分片已保留');
    update();
  });
  engine.on('recover', ({ dropped }) => {
    pushTimeline('warn', `恢复时发现 ${dropped} 个本地分片损坏并丢弃`);
  });
  engine.on('log', (entry) => {
    pushTimeline(entry.level === 'error' ? 'bad' : 'info', entry.message);
  });
  engine.on('completed', (payload) => {
    pushTimeline('good', `下载完成，整体哈希 ${payload.sha256.slice(0, 12)}… 校验通过`);
    toast('good', '下载完成', `${payload.snapshot.name} · ${fmtBytes(payload.size)} 已通过整体校验`);
    update();
  });
  engine.on('failed', (payload) => {
    pushTimeline('bad', `失败：${payload.message}`);
    toast('bad', '下载失败', payload.message);
    update();
  });
}

async function selectFile(fileId, { history = true } = {}) {
  if (!fileId) return;
  app.activeId = fileId;
  app.chunkNodes.clear();
  app.versionNodes.clear();
  const grid = $('chunk-grid');
  delete grid.dataset.count;
  grid.replaceChildren();
  delete $('version-strip').dataset.count;
  $('chunk-table').querySelector('tbody').replaceChildren();
  $('version-strip').replaceChildren();
  const engine = await ensureEngine(fileId);
  if (history) {
    const url = new URL(location.href);
    url.searchParams.set('file', fileId);
    window.history.replaceState(null, '', url);
  }
  app.snapshots.set(fileId, engine.snapshot());
  app.peeks.set(fileId, await engine.peek());
  renderAll();
  renderFileTabs();
  renderFileList();
}

async function startAllPeeks() {
  for (const file of app.files) {
    const engine = app.engines.get(file.id);
    if (!engine) continue;
    try {
      app.peeks.set(file.id, await engine.peek());
    } catch {
      /* ignore */
    }
  }
  scheduleRender();
}

/* ------------------------------------------------------------- rendering */

function scheduleRender() {
  if (app.renderQueued) return;
  app.renderQueued = true;
  requestAnimationFrame(() => {
    app.renderQueued = false;
    renderAll();
  });
}

function renderAll() {
  const snap = app.snapshots.get(app.activeId);
  if (!snap) return;
  renderDownloadCard(snap);
  renderChunkGrid(snap);
  renderVersion(snap);
  renderRetries(snap);
  renderKpis(snap);
  renderFileTabs();
  renderFileList();
  renderTimeline();
}

function activeEngine() {
  return app.engines.get(app.activeId);
}

function renderDownloadCard(snap) {
  const engine = activeEngine();
  const peek = app.peeks.get(app.activeId);
  const isDone = snap.status === 'completed';
  const running = ['downloading', 'verifying', 'pausing', 'cancelling'].includes(snap.status);

  $('dl-title').textContent = snap.name;
  const fileInfo = app.files.find((f) => f.id === snap.fileId);
  $('dl-note').textContent = fileInfo?.note
    ? `${fileInfo.note} · ${snap.chunkCount} 片 × ${fmtBytes(snap.chunkSize)}`
    : `${snap.chunkCount} 片 × ${fmtBytes(snap.chunkSize)}`;
  $('rev-badge').textContent = `r${snap.revision}`;

  $('hero-percent').textContent = Math.floor(snap.percent);
  $('hero-sub').textContent =
    snap.status === 'completed'
      ? '整体校验通过'
      : snap.status === 'failed'
        ? '已停止，可继续'
        : snap.status === 'paused' || snap.status === 'cancelled'
          ? `已保存 ${snap.completed} 个分片`
          : `已下载 ${fmtBytes(snap.bytesDone)} · ${snap.completed}/${snap.chunkCount} 片`;
  $('meta-bytes').textContent = fmtBytes(snap.bytesDone);
  $('meta-size').textContent = fmtBytes(snap.size);
  $('meta-chunks').textContent = `${snap.completed} / ${snap.chunkCount}`;
  $('meta-concurrency').textContent = String(snap.concurrency);

  const meter = $('progress-meter');
  meter.setAttribute('aria-valuenow', String(Math.round(snap.percent)));
  $('meter-fill').style.width = `${snap.percent}%`;
  const hasRetrying = Object.values(snap.chunkStatus || {}).some((c) => c.status === 'retrying');
  const meterState =
    snap.status === 'completed' ? 'completed'
    : snap.status === 'failed' ? 'failed'
    : snap.status === 'paused' || snap.status === 'cancelled' ? 'paused'
    : hasRetrying ? 'retrying'
    : 'downloading';
  meter.dataset.state = meterState;

  $('meter-caption').textContent = `${fmtBytes(snap.bytesDone)} / ${fmtBytes(snap.size)}`;
  $('meter-speed').textContent = running
    ? `${fmtSpeed(snap.speedBps)}${snap.etaMs ? ` · 剩余 ${fmtEta(snap.etaMs)}` : ''}`
    : isDone
      ? '已完成'
      : '—';

  renderMeterTicks(snap);

  const chip = $('status-chip');
  chip.dataset.state = snap.status;
  chip.textContent = STATUS_LABELS[snap.status] || snap.status;
  $('status-detail').textContent = statusDetail(snap, peek);

  // buttons
  const busy = ['pausing', 'cancelling'].includes(snap.status);
  const hasStored = peek && peek.completedCount > 0 && !isDone;
  const canStart = !running && !busy && !isDone && snap.completed < snap.chunkCount;
  $('btn-start').disabled = !canStart;
  $('btn-start').textContent =
    snap.status === 'paused' || snap.status === 'failed' || (snap.status === 'cancelled' && hasStored)
      ? snap.completed > 0 ? '继续下载' : '开始下载'
      : '开始下载';
  $('btn-pause').disabled = !['downloading', 'verifying'].includes(snap.status);
  $('btn-cancel').disabled = !running && snap.completed === 0;
  $('btn-reset').disabled = running || (snap.completed === 0 && !peek);

  // resume banner
  const showResume =
    !running && !isDone && hasStored &&
    ['cancelled', 'paused', 'failed'].includes(snap.status);
  const banner = $('resume-banner');
  banner.hidden = !showResume;
  if (showResume) {
    $('resume-detail').textContent =
      `已完成 ${peek.completedCount} / ${peek.chunkCount} 个分片，恢复时会重新校验后复用`;
  }

  // completed panel
  const done = $('completed-panel');
  done.hidden = !isDone;
  if (isDone) {
    $('merged-hash').textContent = `SHA-256 ${snap.mergedSha256 || snap.sha256}`;
    $('completed-at').textContent = snap.completedAt ? `完成于 ${fmtDateTime(snap.completedAt)}` : '';
    const link = $('download-link');
    const cached = app.urlCache.get(`${snap.fileId}:${snap.revision}:${snap.mergedSha256}`);
    if (cached) {
      link.href = cached;
      link.setAttribute('download', snap.name);
      link.removeAttribute('aria-disabled');
    } else {
      link.href = '#';
      link.setAttribute('aria-disabled', 'true');
    }
  }

  // failure panel
  const failure = $('failure-panel');
  const err = snap.lastError;
  const showFailure = snap.status === 'failed' && err;
  failure.hidden = !showFailure;
  if (showFailure) {
    $('failure-chip').textContent = RETRY_REASONS[err.reason] || '失败';
    $('failure-title').textContent = err.message || '下载失败';
    $('failure-message').textContent =
      err.reason === 'hash-mismatch'
        ? '整体哈希与服务端清单不一致。已完成分片已保留，可重试；若持续失败请清除本地分片后重下。'
        : err.reason === 'retries-exhausted'
          ? '已达到最大尝试次数。恢复网络后点击「继续下载」，已完成的分片会按校验和复用。'
          : '可修正故障设置后继续下载，已完成分片会被保留。';
    const detail = $('failure-detail');
    detail.replaceChildren();
    if (err.expected) detail.append(el('code', null, `期望 ${String(err.expected).slice(0, 16)}…`));
    if (err.actual) detail.append(el('code', null, `实际 ${String(err.actual).slice(0, 16)}…`));
    if (err.missing) detail.append(el('code', null, `缺失 ${err.missing} 片`));
  }

  $('concurrency-out').textContent = String(engine?.concurrency ?? snap.concurrency);
}

function statusDetail(snap, peek) {
  switch (snap.status) {
    case 'idle':
      return peek?.completedCount
        ? `已保存 ${peek.completedCount} 个分片，可继续`
        : '尚未开始';
    case 'downloading':
      return `正在下载 ${snap.active} 个并发分片 · 已完成 ${snap.completed}/${snap.chunkCount}`;
    case 'verifying':
      return '正在逐片重新计算 SHA-256，然后校验整体哈希';
    case 'pausing':
      return '正在停止在途请求…';
    case 'cancelling':
      return '正在取消，已完成分片会保留';
    case 'paused':
      return `已暂停 · ${snap.completed}/${snap.chunkCount} 片已保存`;
    case 'cancelled':
      return snap.completed > 0
        ? `已取消 · ${snap.completed}/${snap.chunkCount} 片保留，可继续`
        : '已取消，未保留分片';
    case 'failed':
      return RETRY_REASONS[snap.lastError?.reason] || '请查看失败原因';
    case 'completed':
      return `${snap.chunkCount} 个分片合并后整体哈希校验通过`;
    default:
      return '';
  }
}

function renderMeterTicks(snap) {
  const ticks = $('meter-ticks');
  if (ticks.dataset.count === String(snap.chunkCount)) return;
  ticks.dataset.count = String(snap.chunkCount);
  ticks.style.gridTemplateColumns = `repeat(${snap.chunkCount}, 1fr)`;
  const frag = document.createDocumentFragment();
  for (let i = 1; i < snap.chunkCount; i++) frag.append(el('span'));
  ticks.replaceChildren(frag);
}

/* ------------------------------------------------------------ chunk grid */

function chunkNode(index) {
  let node = app.chunkNodes.get(index);
  if (node) return node;
  node = document.createElement('button');
  node.type = 'button';
  node.className = 'chunk';
  node.dataset.index = String(index);
  node.setAttribute('role', 'listitem');
  node.addEventListener('pointerenter', (e) => showChunkTooltip(index, e.currentTarget));
  node.addEventListener('focus', (e) => showChunkTooltip(index, e.currentTarget));
  node.addEventListener('pointerleave', hideChunkTooltip);
  node.addEventListener('blur', hideChunkTooltip);
  app.chunkNodes.set(index, node);
  return node;
}


function renderChunkGrid(snap) {
  if (app.view !== 'grid') {
    renderChunkTable(snap);
    return;
  }
  const grid = $('chunk-grid');
  grid.hidden = false;
  $('chunk-table-wrap').hidden = true;
  if (tooltipState !== null) refreshChunkTooltip();

  const total = snap.chunkCount;
  const stale =
    grid.dataset.count !== String(total) ||
    grid.childElementCount !== total ||
    app.chunkNodes.size !== total;
  if (stale) {
    grid.dataset.count = String(total);
    app.chunkNodes.clear();
    const frag = document.createDocumentFragment();
    for (let i = 0; i < total; i++) frag.append(chunkNode(i));
    grid.replaceChildren(frag);
  }

  for (let i = 0; i < total; i++) {
    const node = app.chunkNodes.get(i);
    if (!node) continue;
    const info = chunkStatusFor(snap, i);
    if (node.dataset.status !== info.status) node.dataset.status = info.status;
    const reused = info.reused ? 'true' : 'false';
    if (node.dataset.reused !== reused) node.dataset.reused = reused;
    const label = `分片 ${i + 1}：${CHUNK_LABELS[info.status]}${info.reason ? `（${info.reason}）` : ''}`;
    if (node.getAttribute('aria-label') !== label) node.setAttribute('aria-label', label);
  }
}

function chunkStatusFor(snap, index) {
  const done = snap.completedMap?.[index];
  if (done) {
    return {
      status: 'done',
      reused: !!done.reused,
      attempts: done.attempts,
      sha256: done.sha256,
      size: done.size,
      ms: done.ms,
    };
  }
  const live = snap.chunkStatus?.[index];
  if (live) {
    return {
      status: live.status,
      reason: live.reason ? RETRY_REASONS[live.reason] || live.reason : null,
      message: live.message,
      attempt: live.attempt,
      nextRetryMs: live.nextRetryMs,
    };
  }
  if (snap.attempts?.[index]) {
    return { status: 'retrying', attempt: snap.attempts[index] };
  }
  return { status: 'pending' };
}

let tooltipState = null;
let tooltipAnchor = null;

/** Keep an open tooltip current while the snapshot underneath it changes. */
function refreshChunkTooltip() {
  if (tooltipState === null) return;
  const node = app.chunkNodes.get(tooltipState);
  if (!node || !node.isConnected) {
    hideChunkTooltip();
    return;
  }
  showChunkTooltip(tooltipState, tooltipAnchor || node);
}

function showChunkTooltip(index, target) {
  const snap = app.snapshots.get(app.activeId);
  if (!snap) return;
  const info = chunkStatusFor(snap, index);
  const chunkMeta = chunkManifestInfo(snap, index);
  const tip = $('chunk-tooltip');
  tip.replaceChildren();

  const title = el('div', 'tt-title');
  const swatch = el('span', 'swatch', '');
  swatch.dataset.s = info.status === 'retrying' ? 'retry' : info.status;
  title.append(swatch);
  title.append(el('span', null, `分片 #${index + 1} · ${CHUNK_LABELS[info.status] || info.status}`));
  tip.append(title);

  if (chunkMeta) {
    const row = (k, v) => {
      const r = el('div', 'tt-row');
      r.append(el('span', null, k), el('code', null, v));
      return r;
    };
    tip.append(row('字节范围', `${chunkMeta.offset.toLocaleString()}–${chunkMeta.end.toLocaleString()}`));
    tip.append(row('长度', fmtBytes(chunkMeta.length)));
    if (info.sha256 || chunkMeta.sha256) {
      tip.append(row('SHA-256', `${(info.sha256 || chunkMeta.sha256).slice(0, 16)}…`));
    }
  }
  if (info.attempt) {
    const r = el('div', 'tt-row');
    r.append(el('span', null, '尝试次数'), el('code', null, `${info.attempt}${info.nextRetryMs ? ` · 等待 ${info.nextRetryMs}ms` : ''}`));
    tip.append(r);
  }
  if (info.reason) {
    const note = el('div', 'tt-note', `${info.reason}${info.message ? `：${info.message}` : ''}`);
    tip.append(note);
  }
  if (info.reused) {
    tip.append(el('div', 'tt-note', '恢复时按校验和复用，未重新下载'));
  }

  tip.hidden = false;
  const rect = target.getBoundingClientRect();
  const tw = tip.offsetWidth;
  const th = tip.offsetHeight;
  let left = rect.left + rect.width / 2 - tw / 2;
  let top = rect.top - th - 8;
  if (top < 8) top = rect.bottom + 8;
  left = Math.max(8, Math.min(window.innerWidth - tw - 8, left));
  tip.style.left = `${left}px`;
  tip.style.top = `${top}px`;
  tooltipState = index;
  tooltipAnchor = target;
}
function hideChunkTooltip() {
  $('chunk-tooltip').hidden = true;
  tooltipState = null;
  tooltipAnchor = null;
}

function chunkManifestInfo(snap, index) {
  const engine = activeEngine();
  const chunk = engine?.manifest?.chunks?.[index];
  if (!chunk) return null;
  return { offset: chunk.offset, end: chunk.end, length: chunk.length, sha256: chunk.sha256 };
}

function renderChunkTable(snap) {
  $('chunk-grid').hidden = true;
  $('chunk-table-wrap').hidden = false;
  hideChunkTooltip();
  const body = $('chunk-table').querySelector('tbody');
  const rows = document.createDocumentFragment();
  for (let i = 0; i < snap.chunkCount; i++) {
    const info = chunkStatusFor(snap, i);
    const meta = chunkManifestInfo(snap, i);
    const tr = el('tr');
    tr.dataset.status = info.status;
    const cells = [
      String(i + 1),
      meta ? meta.offset.toLocaleString() : '—',
      meta ? fmtBytes(meta.length) : '—',
      null,
      info.attempt ? String(info.attempt) : '—',
      null,
    ];
    for (let c = 0; c < cells.length; c++) {
      const td = el('td');
      if (c === 3) {
        const span = el('span', 'status-dot', CHUNK_LABELS[info.status] || info.status);
        if (info.reason) span.append(document.createTextNode(` · ${info.reason}`));
        td.append(span);
      } else if (c === 5) {
        const hash = info.sha256 || meta?.sha256;
        td.append(el('code', null, hash ? `${hash.slice(0, 12)}…` : '—'));
      } else {
        td.textContent = cells[c];
      }
      tr.append(td);
    }
    rows.append(tr);
  }
  body.replaceChildren(rows);
}

/* -------------------------------------------------------------- versions */

function renderVersion(snap) {
  const diff = snap.versionDiff;
  const history = snap.versionHistory || [];
  $('version-empty').hidden = !!diff;
  $('version-content').hidden = !diff;
  const badge = $('version-badge');
  badge.hidden = !diff;
  if (!diff) return;
  badge.textContent = `r${diff.fromRevision} → r${diff.toRevision}`;

  const summary = $('version-summary');
  const pill = (kind, label, value) => {
    const n = el('span', 'diff-pill');
    n.dataset.kind = kind;
    n.append(document.createTextNode(label), el('strong', null, String(value)));
    return n;
  };
  summary.replaceChildren(
    pill('reused', '校验和一致 · 复用', diff.reused),
    pill('changed', '内容变化 · 作废', diff.changed),
    pill('added', '新增', diff.added),
    pill('removed', '移除', diff.removed),
    pill('size', '大小变化', `${diff.sizeDelta >= 0 ? '+' : ''}${fmtBytes(diff.sizeDelta)}`),
    pill('chunks', '分片数', `${diff.fromChunkCount} → ${diff.toChunkCount}`),
  );

  const strip = $('version-strip');
  if (strip.dataset.count !== String(diff.chunks.length)) {
    strip.dataset.count = String(diff.chunks.length);
    app.versionNodes.clear();
    const frag = document.createDocumentFragment();
    for (const c of diff.chunks) {
      const s = el('span');
      s.dataset.index = String(c.index);
      app.versionNodes.set(c.index, s);
      frag.append(s);
    }
    strip.replaceChildren(frag);
  }
  for (const c of diff.chunks) {
    const s = app.versionNodes.get(c.index);
    if (s && s.dataset.status !== c.status) s.dataset.status = c.status;
  }

  const list = $('version-history-list');
  list.replaceChildren(
    ...history.slice().reverse().map((h) => {
      const li = el('li');
      li.append(el('span', 'vh-rev', `r${h.fromRevision} → r${h.toRevision}`));
      if (h.diff) {
        li.append(el('span', null, `复用 ${h.reused} · 作废 ${h.diff.changed + h.diff.removed} · 新增 ${h.diff.added}`));
      }
      li.append(el('span', 'vh-time', fmtDateTime(h.at)));
      return li;
    }),
  );
}

/* --------------------------------------------------------------- retries */

function renderRetries(snap) {
  const retries = (snap.retries || []).slice();
  $('retry-count').textContent = `${retries.length} 次`;
  const empty = retries.length === 0;
  $('retry-empty').hidden = !empty;
  $('retry-table-wrap').hidden = empty;
  $('retry-summary').hidden = empty;

  if (!empty) {
    const counts = new Map();
    for (const r of retries) counts.set(r.reason, (counts.get(r.reason) || 0) + 1);
    const max = Math.max(...counts.values());
    const bars = $('retry-bars');
    bars.replaceChildren(
      ...[...counts.entries()]
        .sort((a, b) => b[1] - a[1])
        .map(([reason, count]) => {
          const row = el('div', 'retry-row');
          row.append(el('span', 'rr-label', RETRY_REASONS[reason] || reason));
          const track = el('div', 'rr-track');
          const fill = el('div', 'rr-fill');
          fill.style.width = `${(count / max) * 100}%`;
          track.append(fill);
          row.append(track, el('span', 'rr-count', String(count)));
          return row;
        }),
    );

    const body = $('retry-table').querySelector('tbody');
    body.replaceChildren(
      ...retries
        .slice()
        .reverse()
        .slice(0, 80)
        .map((r) => {
          const tr = el('tr');
          tr.dataset.status = r.final ? 'failed' : 'retrying';
          const cells = [
            fmtTime(r.at),
            r.index >= 0 ? `#${r.index + 1}` : '—',
            `第 ${r.attempt} 次`,
            r.label + (r.final ? '（放弃）' : ''),
            r.message || '',
            r.delayMs ? `${r.delayMs} ms` : '—',
          ];
          for (const c of cells) {
            const td = el('td');
            td.textContent = c;
            tr.append(td);
          }
          return tr;
        }),
    );
  }
}

/* ------------------------------------------------------------------- KPI */

function renderKpis(snap) {
  const complete = Object.values(snap.completedMap || {});
  const reused = complete.filter((c) => c.reused).length;
  const attemptsTotal = complete.reduce((a, c) => a + Math.max(0, (c.attempts || 1) - 1), 0);
  const retryLog = snap.retries || [];
  $('kpi-reused').textContent = String(reused);
  $('kpi-reused-foot').textContent = reused
    ? `共 ${complete.length} 个已完成，未重下`
    : '恢复时按校验和复用';
  $('kpi-retries').textContent = String(retryLog.length);
  const wasted = attemptsTotal;
  $('kpi-retries-foot').textContent = wasted ? `首次成功率 ${Math.round((complete.length / (complete.length + wasted)) * 100)}%` : '无失效尝试';
  $('kpi-speed').textContent = fmtSpeed(snap.speedBps);
  $('kpi-eta').textContent = snap.status === 'completed' ? '已完成' : `预计剩余 ${fmtEta(snap.etaMs)}`;
  $('kpi-verified').textContent = `${complete.length}/${snap.chunkCount}`;
}

/* --------------------------------------------------------------- timeline */

function pushTimeline(kind, message) {
  app.timeline.push({ kind, message, at: Date.now() });
  if (app.timeline.length > 60) app.timeline.shift();
  renderTimeline();
}

function renderTimeline() {
  const list = $('timeline');
  if (!app.timeline.length) {
    list.replaceChildren(el('li', null, '等待事件…'));
    return;
  }
  list.replaceChildren(
    ...app.timeline.slice().reverse().slice(0, 30).map((t) => {
      const li = el('li');
      li.dataset.kind = t.kind;
      li.append(el('span', 'tl-time', fmtTime(t.at)), el('span', 'tl-msg', t.message));
      return li;
    }),
  );
}

/* ------------------------------------------------------------ file chrome */

function renderFileTabs() {
  const wrap = $('file-tabs');
  wrap.replaceChildren(
    ...app.files.map((file) => {
      const snap = app.snapshots.get(file.id);
      const btn = el('button', 'file-tab');
      btn.type = 'button';
      btn.setAttribute('role', 'tab');
      btn.dataset.file = file.id;
      btn.dataset.status = snap?.status || 'idle';
      const selected = file.id === app.activeId;
      btn.setAttribute('aria-selected', String(selected));
      btn.tabIndex = selected ? 0 : -1;
      btn.append(el('span', 'tab-dot'), document.createTextNode(file.name));
      const pct = snap ? Math.floor(snap.percent) : 0;
      btn.append(el('span', 'tab-pct', `${pct}%`));
      btn.addEventListener('click', () => selectFile(file.id));
      btn.addEventListener('keydown', (e) => {
        if (e.key !== 'ArrowRight' && e.key !== 'ArrowLeft') return;
        e.preventDefault();
        const idx = app.files.findIndex((f) => f.id === app.activeId);
        const next = (idx + (e.key === 'ArrowRight' ? 1 : -1) + app.files.length) % app.files.length;
        selectFile(app.files[next].id);
        setTimeout(() => wrap.children[next]?.focus(), 0);
      });
      return btn;
    }),
  );
}

function renderFileList() {
  const list = $('file-list');
  list.replaceChildren(
    ...app.files.map((file) => {
      const snap = app.snapshots.get(file.id);
      const peek = app.peeks.get(file.id);
      const li = el('li');
      li.tabIndex = 0;
      li.setAttribute('role', 'button');
      li.setAttribute('aria-current', String(file.id === app.activeId));
      li.addEventListener('click', () => selectFile(file.id));
      li.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          selectFile(file.id);
        }
      });
      const body = el('div', 'fl-body');
      body.append(el('span', 'fl-name', file.name));
      const pct = snap ? Math.round(snap.percent) : 0;
      const statusLabel = snap ? STATUS_LABELS[snap.status] || snap.status : '未加载';
      body.append(el('span', 'fl-meta', `${fmtBytes(file.size)} · ${statusLabel} · ${pct}%`));
      const mini = el('div', 'fl-mini');
      const fill = el('span');
      fill.style.width = `${snap ? snap.percent : 0}%`;
      mini.append(fill);
      li.append(body, mini);
      if (peek?.completedCount && !snap?.completedAt) {
        li.title = `已保存 ${peek.completedCount} 个分片，可恢复`;
      }
      return li;
    }),
  );
}

/* --------------------------------------------------------------- controls */

function wireStaticControls() {
  $('btn-start').addEventListener('click', async () => {
    const engine = activeEngine();
    if (!engine) return;
    await engine.start();
  });
  $('btn-resume').addEventListener('click', async () => {
    const engine = activeEngine();
    if (!engine) return;
    await engine.resume();
  });
  $('btn-discard').addEventListener('click', async () => {
    const engine = activeEngine();
    if (!engine) return;
    await engine.reset();
    app.peeks.set(engine.fileId, null);
    await engine.init();
    app.snapshots.set(engine.fileId, engine.snapshot());
    pushTimeline('warn', '已丢弃本地分片');
    renderAll();
  });
  $('btn-pause').addEventListener('click', () => activeEngine()?.pause());
  $('btn-cancel').addEventListener('click', async () => {
    const engine = activeEngine();
    if (!engine) return;
    await engine.cancel();
    app.peeks.set(engine.fileId, await engine.peek());
    renderAll();
  });
  $('btn-reset').addEventListener('click', async () => {
    const engine = activeEngine();
    if (!engine) return;
    if (!confirm('将删除该文件在本地保存的所有分片和下载状态，继续？')) return;
    await engine.reset();
    app.peeks.set(engine.fileId, null);
    await engine.init();
    app.snapshots.set(engine.fileId, engine.snapshot());
    pushTimeline('warn', '已清除本地分片与状态');
    renderAll();
  });
  $('start-all').addEventListener('click', async () => {
    for (const file of app.files) {
      const engine = app.engines.get(file.id);
      if (!engine) continue;
      const snap = engine.snapshot();
      if (['downloading', 'verifying', 'completed'].includes(snap.status)) continue;
      engine.start().catch(() => {});
    }
    pushTimeline('info', '已并发启动所有未完成任务');
  });

  $('concurrency').addEventListener('input', (e) => {
    $('concurrency-out').textContent = e.target.value;
    const engine = activeEngine();
    engine?.setConcurrency(Number(e.target.value));
  });

  for (const btn of document.querySelectorAll('.seg-btn')) {
    btn.addEventListener('click', () => {
      app.view = btn.dataset.view;
      for (const b of document.querySelectorAll('.seg-btn')) {
        const active = b === btn;
        b.classList.toggle('is-active', active);
        b.setAttribute('aria-pressed', String(active));
      }
      if (app.view === 'table') {
        renderChunkTable(app.snapshots.get(app.activeId));
      } else {
        app.chunkNodes.clear();
        delete $('chunk-grid').dataset.count;
        $('chunk-grid').replaceChildren();
        renderChunkGrid(app.snapshots.get(app.activeId));
      }
    });
  }

  $('theme-toggle').addEventListener('click', () => {
    const root = document.documentElement;
    const current =
      root.dataset.theme ||
      (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
    const next = current === 'dark' ? 'light' : 'dark';
    root.dataset.theme = next;
    try {
      localStorage.setItem('downloader-theme', next);
    } catch {
      /* ignore */
    }
  });

  wireFaultControls();
  $('btn-replace').addEventListener('click', () => replaceFile());
  $('btn-reset-rev').addEventListener('click', () => resetRevision());

  document.addEventListener('visibilitychange', () => {
    if (!document.hidden) {
      const engine = activeEngine();
      if (engine && !['downloading', 'verifying'].includes(engine.status)) {
        app.peeks.set(engine.fileId, engine.peek().then ? null : null);
      }
    }
  });

  window.addEventListener('resize', hideChunkTooltip);
}

function applyStoredTheme() {
  let stored = null;
  try {
    stored = localStorage.getItem('downloader-theme');
  } catch {
    /* ignore */
  }
  if (stored === 'light' || stored === 'dark') document.documentElement.dataset.theme = stored;
  else document.documentElement.removeAttribute('data-theme');
}

/* ---------------------------------------------------------- fault panel */

function wireFaultControls() {
  const push = debounce(updateFaults, 120);
  $('fault-offline').addEventListener('change', (e) => {
    push({ offline: e.target.checked });
  });
  const slider = (id, key, map) => {
    const input = $(id);
    input.addEventListener('input', () => {
      const value = Number(input.value);
      $(`${id}-out`).textContent = map.out(value);
      push({ [key]: map.value(value) });
    });
  };
  slider('fault-drop', 'dropRate', { out: (v) => `${v}%`, value: (v) => v / 100 });
  slider('fault-corrupt', 'corruptRate', { out: (v) => `${v}%`, value: (v) => v / 100 });
  slider('fault-range', 'rangeMismatchRate', { out: (v) => `${v}%`, value: (v) => v / 100 });
  slider('fault-latency', 'latencyMs', { out: (v) => `${v} ms`, value: (v) => v });
}

function syncFaultControls() {
  if (!app.faults) return;
  const f = app.faults;
  $('fault-offline').checked = !!f.offline;
  $('fault-drop').value = String(Math.round((f.dropRate || 0) * 100));
  $('fault-drop-out').textContent = `${Math.round((f.dropRate || 0) * 100)}%`;
  $('fault-corrupt').value = String(Math.round((f.corruptRate || 0) * 100));
  $('fault-corrupt-out').textContent = `${Math.round((f.corruptRate || 0) * 100)}%`;
  $('fault-range').value = String(Math.round((f.rangeMismatchRate || 0) * 100));
  $('fault-range-out').textContent = `${Math.round((f.rangeMismatchRate || 0) * 100)}%`;
  $('fault-latency').value = String(f.latencyMs || 0);
  $('fault-latency-out').textContent = `${f.latencyMs || 0} ms`;
  const active =
    f.offline ||
    (f.dropRate || 0) + (f.corruptRate || 0) + (f.rangeMismatchRate || 0) > 0;
  $('fault-active').textContent = f.offline ? '断网' : active ? '已注入' : '关闭';
}

async function updateFaults(patch) {
  try {
    const res = await fetch('/api/admin/faults', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(patch),
    });
    const data = await res.json();
    app.faults = data.faults;
    syncFaultControls();
  } catch (err) {
    toast('bad', '故障设置未生效', String(err));
  }
}

async function replaceFile() {
  const engine = activeEngine();
  if (!engine) return;
  try {
    const res = await fetch(`/api/admin/files/${engine.fileId}/replace`, { method: 'POST' });
    const data = await res.json();
    const diff = data.diff;
    pushTimeline(
      'info',
      `服务端发布 r${diff.toRevision}：${diff.reused} 片可复用，${diff.changed} 片内容变化，新增 ${diff.added}`,
    );
    toast(
      'warn',
      '已发布新修订',
      `r${diff.fromRevision} → r${diff.toRevision} · 大小 ${diff.sizeDelta >= 0 ? '+' : ''}${fmtBytes(diff.sizeDelta)}`,
    );
    // The downloader discovers it via poll/409; nudge it now if it is running.
    if (['downloading', 'verifying'].includes(engine.status)) {
      engine._versionDirty = true;
      engine._runAbort?.abort();
    } else {
      await engine.init();
      app.snapshots.set(engine.fileId, engine.snapshot());
    }
    await refreshFiles();
    renderAll();
  } catch (err) {
    toast('bad', '替换失败', String(err));
  }
}

async function resetRevision() {
  const engine = activeEngine();
  if (!engine) return;
  try {
    await fetch(`/api/admin/files/${engine.fileId}/reset`, { method: 'POST' });
    await engine.init();
    app.snapshots.set(engine.fileId, engine.snapshot());
    pushTimeline('warn', '服务端已重置为 r1');
    await refreshFiles();
    renderAll();
  } catch (err) {
    toast('bad', '重置失败', String(err));
  }
}

/* --------------------------------------------------------------- server events */

async function connectEvents() {
  if (typeof EventSource === 'undefined') return;
  const es = new EventSource('/api/events');
  app.eventSource = es;
  let offlineTimer = null;
  es.addEventListener('open', () => setConn(true));
  es.addEventListener('error', () => {
    setConn(false);
    clearTimeout(offlineTimer);
    offlineTimer = setTimeout(() => setConn(true), 3000);
  });
  es.addEventListener('message', (event) => {
    let data;
    try {
      data = JSON.parse(event.data);
    } catch {
      return;
    }
    if (data.type === 'replace') {
      refreshFiles().catch(() => {});
    }
  });
  setConn(true);
}

function setConn(online) {
  const node = $('conn-status');
  node.dataset.state = online ? 'online' : 'offline';
  $('conn-label').textContent = online ? '服务端在线' : '服务端离线';
}

/* ---------------------------------------------------------------- helpers */

function debounce(fn, ms) {
  let timer;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), ms);
  };
}

function toast(kind, title, message) {
  const region = $('toast-region');
  const node = el('div', 'toast');
  node.dataset.kind = kind;
  const body = el('div');
  body.append(el('strong', null, title));
  if (message) body.append(el('p', null, message));
  const close = el('button', 'toast-close', '×');
  close.type = 'button';
  close.setAttribute('aria-label', '关闭提示');
  close.addEventListener('click', () => node.remove());
  node.append(body, close);
  region.append(node);
  setTimeout(() => node.remove(), 6000);
}

/* download link: hand the verified blob to the browser */
document.addEventListener('click', (e) => {
  const link = e.target.closest('#download-link');
  if (!link) return;
  const engine = activeEngine();
  if (!engine?.resultBlob) {
    e.preventDefault();
    toast('warn', '文件尚不可保存', '完成整体校验后才能保存合并文件');
    return;
  }
  const snap = engine.snapshot();
  const key = `${snap.fileId}:${snap.revision}:${snap.mergedSha256}`;
  let url = app.urlCache.get(key);
  if (!url) {
    url = URL.createObjectURL(engine.resultBlob);
    app.urlCache.set(key, url);
  }
  link.href = url;
  link.setAttribute('download', snap.name);
});

boot().catch((err) => {
  console.error(err);
  toast('bad', '初始化失败', err?.message || String(err));
});

export { app };
