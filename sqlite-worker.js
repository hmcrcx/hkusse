const WA_SQLITE_BASE = 'https://cdn.jsdelivr.net/gh/rhashimoto/wa-sqlite@v1.1.1';
const WORKER_BUILD = '20260925-pim-fast-path-1';
let SQLiteESMFactory = null;
let SQLite = null;
let OPFSAnyContextVFS = null;
let sqliteImportsPromise = null;

async function loadSQLiteModules() {
  if (!sqliteImportsPromise) {
    sqliteImportsPromise = Promise.all([
      import(`${WA_SQLITE_BASE}/dist/wa-sqlite-async.mjs`),
      import(`${WA_SQLITE_BASE}/src/sqlite-api.js`),
      import(`${WA_SQLITE_BASE}/src/examples/OPFSAnyContextVFS.js`)
    ]).then(([factoryModule, apiModule, vfsModule]) => {
      SQLiteESMFactory = factoryModule.default;
      SQLite = apiModule;
      OPFSAnyContextVFS = vfsModule.OPFSAnyContextVFS;
      if (!SQLiteESMFactory || !SQLite || !OPFSAnyContextVFS) {
        throw new Error('wa-sqlite module exports are incomplete.');
      }
    });
  }
  return sqliteImportsPromise;
}

self.addEventListener('error', (event) => {
  try {
    self.postMessage({ id: 0, type: 'bootstrapError', error: event?.message || 'Worker script error' });
  } catch (_) {}
});

self.postMessage({ id: 0, type: 'workerReady', build: WORKER_BUILD });

self.addEventListener('unhandledrejection', (event) => {
  try {
    const reason = event?.reason?.message || String(event?.reason || 'Unhandled worker rejection');
    self.postMessage({ id: 0, type: 'bootstrapError', error: reason });
  } catch (_) {}
});

const VFS_NAME = 'usse-opfs-read-vfs';
const DB_FILE_PREFIX = 'USSE_PRN_';

function dbFileForVersion(version) {
  const clean = String(version || '').trim().replace(/[^a-fA-F0-9]/g, '').slice(0, 24);
  if (!clean) throw new Error('Database version is missing.');
  return `${DB_FILE_PREFIX}${clean}.db`;
}
let sqliteModule = null;
let sqlite3 = null;
let vfs = null;
let db = null;

function ensureSupported() {
  if (!navigator.storage?.getDirectory) throw new Error('Origin Private File System (OPFS) is not available.');
  if (typeof DecompressionStream === 'undefined') throw new Error('Streaming gzip decompression is not available in this browser.');
}

async function ensureSQLite() {
  ensureSupported();
  if (sqlite3) return;
  await loadSQLiteModules();
  sqliteModule = await SQLiteESMFactory();
  sqlite3 = SQLite.Factory(sqliteModule);
  vfs = await OPFSAnyContextVFS.create(VFS_NAME, sqliteModule);
  sqlite3.vfs_register(vfs, true);
}

async function opfsRoot() { ensureSupported(); return navigator.storage.getDirectory(); }
async function removeFile(name) { try { await (await opfsRoot()).removeEntry(name); } catch (_) {} }
async function fileSize(name) {
  try { return (await (await (await opfsRoot()).getFileHandle(name)).getFile()).size; } catch (_) { return 0; }
}

async function hasValidDatabase(name, expectedBytes = null) {
  const size = await fileSize(name);
  if (!size || (expectedBytes && size !== Number(expectedBytes))) return false;
  try {
    const root = await opfsRoot();
    const file = await (await root.getFileHandle(name)).getFile();
    const header = new TextDecoder().decode(new Uint8Array(await file.slice(0,16).arrayBuffer()));
    return header === 'SQLite format 3\0';
  } catch (_) { return false; }
}

async function removeEmptyDatabaseSidecars(fileName) {
  // This database is immutable and was built in journal_mode=delete. Empty
  // -wal/-journal files can be leftovers from an interrupted/older VFS open.
  // Remove only zero-byte sidecars; never delete a non-empty journal/WAL.
  for (const suffix of ['-wal', '-journal']) {
    const sidecar = fileName + suffix;
    if (await fileSize(sidecar) === 0) await removeFile(sidecar);
  }
}

async function importCompressedDatabase(url, fileName, expectedRawBytes) {
  ensureSupported();
  const response = await fetch(url, { cache: 'no-store' });
  if (!response.ok) throw new Error(`Database download failed: HTTP ${response.status}`);
  if (!response.body) throw new Error('Database download failed: streaming response body unavailable.');

  const root = await opfsRoot();
  const handle = await root.getFileHandle(fileName, { create: true });
  let writable = null;
  let closed = false;
  try {
    // One-time import uses the normal OPFS writable stream. The SQLite reader
    // uses OPFSAnyContextVFS and never needs an exclusive sync access handle.
    writable = await handle.createWritable({ keepExistingData: false });

    const total = Number(response.headers.get('Content-Length')) || 0;
    const reader = response.body.getReader();
    let received = 0;

    const monitored = new ReadableStream({
      async pull(controller) {
        const next = await reader.read();
        if (next.done) {
          controller.close();
          return;
        }
        const chunk = next.value;
        if (!chunk?.byteLength) return;
        received += chunk.byteLength;
        self.postMessage({ type: 'progress', stage: 'download', received, total });
        controller.enqueue(chunk);
      },
      cancel(reason) { reader.cancel(reason).catch(() => {}); }
    });

    const source = monitored.pipeThrough(new DecompressionStream('gzip'));
    let written = 0;
    const progress = new TransformStream({
      transform(chunk, controller) {
        written += chunk.byteLength;
        if (expectedRawBytes) {
          self.postMessage({
            type: 'progress',
            stage: 'decompress',
            written,
            totalRaw: Number(expectedRawBytes)
          });
        }
        controller.enqueue(chunk);
      }
    });

    await source.pipeThrough(progress).pipeTo(writable);
    closed = true;
  } catch (error) {
    try { if (writable && !closed) await writable.abort(error); } catch (_) {}
    await removeFile(fileName);
    throw error;
  }

  const actual = await fileSize(fileName);
  if (!actual || (expectedRawBytes && actual !== Number(expectedRawBytes))) {
    await removeFile(fileName);
    throw new Error(`Database installation size check failed (expected ${expectedRawBytes}, got ${actual}).`);
  }
  if (!(await hasValidDatabase(fileName, expectedRawBytes))) {
    await removeFile(fileName);
    throw new Error('Database installation failed SQLite header validation.');
  }
  await removeEmptyDatabaseSidecars(fileName);
}


async function openDatabase(fileName) {
  await ensureSQLite();
  if (!(await hasValidDatabase(fileName))) throw new Error(`SQLite database file is missing or invalid: ${fileName}`);
  if (db) { try { await sqlite3.close(db); } catch (_) {} db = null; }

  // The shipped database is a read-only, journal_mode=delete snapshot. Clear
  // only empty stale sidecars left by older VFS attempts before opening it.
  await removeEmptyDatabaseSidecars(fileName);

  try {
    db = await sqlite3.open_v2(fileName, SQLite.SQLITE_OPEN_READONLY, VFS_NAME);
  } catch (error) {
    try {
      error.vfsError = vfs?.lastError?.message || '';
      error.databaseFile = fileName;
      error.vfsName = VFS_NAME;
    } catch (_) {}
    throw error;
  }
}

async function cleanupOldDatabaseFiles(keepFileName) {
  try {
    const root = await opfsRoot();
    for await (const [name, handle] of root.entries()) {
      if (handle.kind === 'file' && name.startsWith(DB_FILE_PREFIX) && name.endsWith('.db') && name !== keepFileName) {
        try { await root.removeEntry(name); } catch (_) {}
        for (const suffix of ['-wal', '-journal']) {
          try { await root.removeEntry(name + suffix); } catch (_) {}
        }
      }
    }
  } catch (_) {}
}

async function execOne(sql, params = []) {
  for await (const stmt of sqlite3.statements(db, sql)) {
    await sqlite3.bind_collection(stmt, params);
    const columns = sqlite3.column_names(stmt);
    if (await sqlite3.step(stmt) === SQLite.SQLITE_ROW) {
      const row = sqlite3.row(stmt);
      return Object.fromEntries(columns.map((name, i) => [name, row[i]]));
    }
    return null;
  }
  return null;
}

async function execAll(sql, params = []) {
  const rows = [];
  for await (const stmt of sqlite3.statements(db, sql)) {
    await sqlite3.bind_collection(stmt, params);
    const columns = sqlite3.column_names(stmt);
    while (await sqlite3.step(stmt) === SQLite.SQLITE_ROW) {
      const row = sqlite3.row(stmt);
      rows.push(Object.fromEntries(columns.map((name, i) => [name, row[i]])));
    }
  }
  return rows;
}

function escapeLike(value) { return String(value).replace(/[\\%_]/g, '\\$&'); }

async function lookupPRN(prn) {
  let row = await execOne('SELECT * FROM addresses WHERE PRN = ?', [prn]);
  let lotFallback = false;
  if (!row) {
    row = await execOne('SELECT * FROM addresses WHERE FIRST_REFERENCE_LOT_PRN = ? LIMIT 1', [prn]);
    lotFallback = !!row;
  }
  return { row, lotFallback };
}

function addTextFilter(parts, params, columnEng, columnChi, value) {
  if (!value) return;
  const term = escapeLike(value);
  parts.push("( " + columnEng + " = ? OR " + columnChi + " = ? OR " + columnEng + " LIKE ? ESCAPE '\\' OR " + columnChi + " LIKE ? ESCAPE '\\' )");
  params.push(value, value, term + '%', term + '%');
}

function addFreeFormatFilters(parts, params, value) {
  if (!value) return;
  const tokens = value.split(/\s+/).filter(Boolean).slice(0, 12);
  for (const token of tokens) {
    const term = '%' + escapeLike(token) + '%';
    parts.push("(FREE_FORMAT_ADDR_ENG LIKE ? ESCAPE '\\' OR FREE_FORMAT_ADDR_CHN LIKE ? ESCAPE '\\')");
    params.push(term, term);
  }
}

async function searchPIM(input) {
  const district = (input.district || '').trim().toUpperCase();
  const dev = (input.dev || '').trim().toUpperCase();
  const street = (input.street || '').trim().toUpperCase();
  const freeFormat = (input.freeFormat || '').trim().toUpperCase();
  const streetNo = (input.streetNo || '').trim().toUpperCase();
  const block = (input.block || '').trim().toUpperCase();
  const floor = (input.floor || '').trim().toUpperCase();
  const flat = (input.flat || '').trim().toUpperCase();

  if (!dev && !street && !freeFormat) {
    return {
      rows: [],
      truncated: false,
      error: 'Please enter a Building/Estate Name, Street Name, or Free Format Search.'
    };
  }

  function addExact(parts, params, engCol, chiCol, value) {
    if (!value) return;
    parts.push(`(${engCol} = ? OR ${chiCol} = ?)`);
    params.push(value, value);
  }

  function addPrefix(parts, params, engCol, chiCol, value) {
    if (!value) return;
    parts.push(`(${engCol} LIKE ? OR ${chiCol} LIKE ?)`);
    params.push(escapeLike(value) + '%', escapeLike(value) + '%');
  }

  function addMicroFilters(parts, params) {
    if (streetNo) {
      parts.push('(HOUSE_NUM_PREFIX = ? OR (HOUSE_NUM_PREFIX || HOUSE_NUM_SUFFIX) = ?)');
      params.push(streetNo, streetNo);
    }
    if (block) { parts.push('BLOCK COLLATE NOCASE = ?'); params.push(block); }
    if (floor) { parts.push('FLOOR COLLATE NOCASE = ?'); params.push(floor); }
    if (flat) { parts.push('FLAT COLLATE NOCASE = ?'); params.push(flat); }
  }

  function selectSQL(where) {
    return 'SELECT PRN, FREE_FORMAT_ADDR_ENG, FREE_FORMAT_ADDR_CHN, ' +
      'AREA_DESC_ENG, AREA_DESC_CHN, DEV_NAME_ENG, DEV_NAME_CHN, ' +
      'STREET_NAME_ENG, STREET_NAME_CHN, HOUSE_NUM_PREFIX, HOUSE_NUM_SUFFIX, ' +
      'BLOCK, FLOOR, FLAT FROM addresses WHERE ' +
      (where.length ? where.join(' AND ') : '1') + ' LIMIT 21';
  }

  const candidateMap = new Map();

  async function collect(where, params) {
    const rows = await execAll(selectSQL(where), params);
    for (const row of rows) {
      const key = String(row.PRN || '');
      if (key && !candidateMap.has(key)) candidateMap.set(key, row);
    }
    return rows.length;
  }

  // Fast path: exact values first. The old query calculated a relevance score
  // for every matching row and then sorted the entire result set. On a 2.75M-row
  // OPFS database that turns a simple Finder request into a full-table operation.
  const exactWhere = [], exactParams = [];
  addExact(exactWhere, exactParams, 'AREA_DESC_ENG', 'AREA_DESC_CHN', district);
  addExact(exactWhere, exactParams, 'DEV_NAME_ENG', 'DEV_NAME_CHN', dev);
  addExact(exactWhere, exactParams, 'STREET_NAME_ENG', 'STREET_NAME_CHN', street);
  if (freeFormat) {
    const tokens = freeFormat.split(/\s+/).filter(Boolean).slice(0, 12);
    for (const token of tokens) {
      const term = '%' + escapeLike(token) + '%';
      exactWhere.push("(FREE_FORMAT_ADDR_ENG LIKE ? OR FREE_FORMAT_ADDR_CHN LIKE ?)");
      exactParams.push(term, term);
    }
  }
  addMicroFilters(exactWhere, exactParams);

  let got = await collect(exactWhere, exactParams);
  let usedFallback = false;

  // Prefix fallback for partially typed building/street names.
  if (got < 21 && !freeFormat && (dev || street)) {
    const prefixWhere = [], prefixParams = [];
    addExact(prefixWhere, prefixParams, 'AREA_DESC_ENG', 'AREA_DESC_CHN', district);
    addPrefix(prefixWhere, prefixParams, 'DEV_NAME_ENG', 'DEV_NAME_CHN', dev);
    addPrefix(prefixWhere, prefixParams, 'STREET_NAME_ENG', 'STREET_NAME_CHN', street);
    addMicroFilters(prefixWhere, prefixParams);
    const before = candidateMap.size;
    await collect(prefixWhere, prefixParams);
    got = candidateMap.size;
    usedFallback = candidateMap.size > before;
  }

  // Free-format already uses contains matching; no additional full-table sort is
  // needed. The 21-row cap lets SQLite stop as soon as enough candidates exist.
  const rows = Array.from(candidateMap.values());

  function relevance(row) {
    let score = 0;
    const eq = (a, b) => a && b && String(a).toUpperCase() === b;
    const starts = (a, b) => a && b && String(a).toUpperCase().startsWith(b);
    if (district && (eq(row.AREA_DESC_ENG, district) || eq(row.AREA_DESC_CHN, district))) score += 30;
    if (dev) {
      if (eq(row.DEV_NAME_ENG, dev) || eq(row.DEV_NAME_CHN, dev)) score += 100;
      else if (starts(row.DEV_NAME_ENG, dev) || starts(row.DEV_NAME_CHN, dev)) score += 60;
    }
    if (street) {
      if (eq(row.STREET_NAME_ENG, street) || eq(row.STREET_NAME_CHN, street)) score += 90;
      else if (starts(row.STREET_NAME_ENG, street) || starts(row.STREET_NAME_CHN, street)) score += 50;
    }
    if (streetNo && (String(row.HOUSE_NUM_PREFIX || '').toUpperCase() === streetNo ||
                     String(row.HOUSE_NUM_PREFIX || '').toUpperCase() + String(row.HOUSE_NUM_SUFFIX || '').toUpperCase() === streetNo)) score += 60;
    if (block && String(row.BLOCK || '').toUpperCase() === block) score += 30;
    if (floor && String(row.FLOOR || '').toUpperCase() === floor) score += 20;
    if (flat && String(row.FLAT || '').toUpperCase() === flat) score += 20;
    if (freeFormat && ((row.FREE_FORMAT_ADDR_ENG || '').toUpperCase() === freeFormat || (row.FREE_FORMAT_ADDR_CHN || '').toUpperCase() === freeFormat)) score += 40;
    return score;
  }

  rows.sort((a, b) => relevance(b) - relevance(a) || String(a.PRN || '').localeCompare(String(b.PRN || '')));
  const display = rows.slice(0, 21);
  return {
    rows: display,
    truncated: display.length > 20,
    fallbackUsed: usedFallback
  };
}

self.onmessage = async event => {
  const { id, type, payload } = event.data || {};
  try {
    if (type === 'prepare') {
      await ensureSQLite();
      self.postMessage({ id, ok: true, type, result: { opfs: true } });
      return;
    }

    if (type === 'openCached') {
      const fileName = dbFileForVersion(payload.version);
      const found = await hasValidDatabase(fileName, payload.expectedBytes);
      if (!found) {
        self.postMessage({ id, ok: true, type, result: { found: false, fileName } });
        return;
      }
      await openDatabase(fileName);
      self.postMessage({ id, ok: true, type, result: { found: true, fileName, size: await fileSize(fileName) } });
      return;
    }

    if (type === 'download') {
      const version = String(payload.version || '').trim();
      const fileName = dbFileForVersion(version);
      // Each database version gets its own OPFS file. This avoids trying to
      // acquire an exclusive write handle on a file another tab/worker may
      // currently have open for read-only SQLite queries.
      await removeFile(fileName);
      await removeEmptyDatabaseSidecars(fileName);
      await importCompressedDatabase(payload.url, fileName, payload.expectedBytes);
      self.postMessage({ id: 0, type: 'progress', stage: 'opening' });
      await openDatabase(fileName);
      self.postMessage({ id, ok: true, type, result: { found: true, fileName, size: await fileSize(fileName) } });
      return;
    }

    if (!db) throw new Error('Database is not initialized.');
    if (type === 'lookupPRN') {
      self.postMessage({ id, ok: true, type, result: await lookupPRN(payload.prn) });
      return;
    }
    if (type === 'searchPIM') {
      self.postMessage({ id, ok: true, type, result: await searchPIM(payload) });
      return;
    }
    if (type === 'close') {
      if (db) { try { await sqlite3.close(db); } catch (_) {} db = null; }
      self.postMessage({ id, ok: true, type });
      self.close();
      return;
    }
    throw new Error('Unknown worker request: ' + type);
  } catch (error) {
    self.postMessage({
      id,
      ok: false,
      type,
      error: error?.message || String(error),
      errorName: error?.name || '',
      errorStack: error?.stack || '',
      vfsError: error?.vfsError || vfs?.lastError?.message || '',
      databaseFile: error?.databaseFile || '',
      vfsName: error?.vfsName || VFS_NAME
    });
  }
};
