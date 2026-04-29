const DATA_DB_NAME = 'otters_wasm_data_v1';
const DATA_DB_STORE = 'kv';
const DATA_DB_PARQUET_SNAPSHOTS_KEY = 'parquet_snapshots';
const CODE_ENTRIES_KEY = 'otters_wasm_code_entries_v1';
const CODE_SEQ_KEY = 'otters_wasm_code_seq_v1';

function safeJsonParse(raw, fallback) {
  try {
    return JSON.parse(raw);
  } catch (_) {
    return fallback;
  }
}

function nowTs() {
  return Date.now();
}

function nextCodeId() {
  let seq = 1;
  try {
    seq = Number(localStorage.getItem(CODE_SEQ_KEY) || '1');
    if (!Number.isFinite(seq) || seq < 1) seq = 1;
    localStorage.setItem(CODE_SEQ_KEY, String(seq + 1));
  } catch (_) {
    seq = nowTs();
  }
  return `code_${seq}`;
}

export function loadCodeEntries() {
  try {
    const raw = localStorage.getItem(CODE_ENTRIES_KEY);
    const parsed = safeJsonParse(raw || '[]', []);
    if (!Array.isArray(parsed)) return [];
    return parsed
      .filter((item) => item && typeof item === 'object')
      .map((item) => ({
        id: String(item.id || nextCodeId()),
        name: String(item.name || '未命名代码'),
        script: String(item.script || ''),
        updatedAt: Number(item.updatedAt || 0),
      }));
  } catch (_) {
    return [];
  }
}

export function saveCodeEntries(entries) {
  const normalized = Array.from(entries || []).map((item) => ({
    id: String(item.id || nextCodeId()),
    name: String(item.name || '未命名代码'),
    script: String(item.script || ''),
    updatedAt: Number(item.updatedAt || nowTs()),
  }));
  localStorage.setItem(CODE_ENTRIES_KEY, JSON.stringify(normalized));
}

export function createCodeEntry({ name, script }) {
  return {
    id: nextCodeId(),
    name: String(name || '新建代码'),
    script: String(script || ''),
    updatedAt: nowTs(),
  };
}

export function touchCodeEntry(entry, patch = {}) {
  return {
    ...entry,
    ...patch,
    id: String((patch && patch.id) || entry.id || nextCodeId()),
    name: String((patch && patch.name) || entry.name || '未命名代码'),
    script: String((patch && Object.prototype.hasOwnProperty.call(patch, 'script')) ? patch.script : entry.script || ''),
    updatedAt: nowTs(),
  };
}

function openDataDb() {
  return new Promise((resolve) => {
    try {
      if (!('indexedDB' in globalThis)) {
        resolve(null);
        return;
      }
      const req = indexedDB.open(DATA_DB_NAME, 1);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains(DATA_DB_STORE)) {
          db.createObjectStore(DATA_DB_STORE);
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => resolve(null);
    } catch (_) {
      resolve(null);
    }
  });
}

function idbPut(db, key, value) {
  return new Promise((resolve, reject) => {
    try {
      const tx = db.transaction(DATA_DB_STORE, 'readwrite');
      tx.objectStore(DATA_DB_STORE).put(value, key);
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error || new Error('indexeddb put failed'));
      tx.onabort = () => reject(tx.error || new Error('indexeddb put aborted'));
    } catch (err) {
      reject(err);
    }
  });
}

function idbGet(db, key) {
  return new Promise((resolve, reject) => {
    try {
      const tx = db.transaction(DATA_DB_STORE, 'readonly');
      const req = tx.objectStore(DATA_DB_STORE).get(key);
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error || new Error('indexeddb get failed'));
    } catch (err) {
      reject(err);
    }
  });
}

function idbDelete(db, key) {
  return new Promise((resolve, reject) => {
    try {
      const tx = db.transaction(DATA_DB_STORE, 'readwrite');
      tx.objectStore(DATA_DB_STORE).delete(key);
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error || new Error('indexeddb delete failed'));
      tx.onabort = () => reject(tx.error || new Error('indexeddb delete aborted'));
    } catch (err) {
      reject(err);
    }
  });
}

function normalizeParquetSnapshots(raw) {
  const items = Array.isArray(raw) ? raw : [];
  return items
    .filter((item) => item && typeof item === 'object' && item.bytes)
    .map((item) => {
      const bytes = item.bytes instanceof Uint8Array
        ? item.bytes
        : (item.bytes instanceof ArrayBuffer ? new Uint8Array(item.bytes) : null);
      if (!bytes || bytes.byteLength === 0) return null;
      return {
        name: typeof item.name === 'string' && item.name ? item.name : 'last.parquet',
        savedAt: Number(item.savedAt || 0),
        bytes,
      };
    })
    .filter(Boolean);
}

export async function saveParquetSnapshot(fileName, bytes) {
  const db = await openDataDb();
  if (!db) return;
  try {
    const payload = {
      name: fileName || 'last.parquet',
      savedAt: nowTs(),
      bytes: new Uint8Array(bytes),
    };
    const previous = normalizeParquetSnapshots(await idbGet(db, DATA_DB_PARQUET_SNAPSHOTS_KEY));
    const next = previous.filter((item) => item.name !== payload.name);
    next.push(payload);
    await idbPut(db, DATA_DB_PARQUET_SNAPSHOTS_KEY, next);
  } finally {
    db.close();
  }
}

export async function loadParquetSnapshots() {
  const db = await openDataDb();
  if (!db) return [];
  try {
    const raw = await idbGet(db, DATA_DB_PARQUET_SNAPSHOTS_KEY);
    return normalizeParquetSnapshots(raw);
  } finally {
    db.close();
  }
}

export async function loadParquetSnapshot() {
  const items = await loadParquetSnapshots();
  return items.at(-1) ?? null;
}

export async function renameParquetSnapshot(oldName, newName) {
  const db = await openDataDb();
  if (!db) return;
  try {
    const items = normalizeParquetSnapshots(await idbGet(db, DATA_DB_PARQUET_SNAPSHOTS_KEY));
    const next = items.map((item) => {
      if (String(item.name || '') !== String(oldName || '')) return item;
      return {
        ...item,
        name: String(newName || item.name || 'last.parquet'),
        savedAt: nowTs(),
      };
    });
    await idbPut(db, DATA_DB_PARQUET_SNAPSHOTS_KEY, next);
  } finally {
    db.close();
  }
}

export async function deleteParquetSnapshotIfName(name) {
  const db = await openDataDb();
  if (!db) return;
  try {
    const items = normalizeParquetSnapshots(await idbGet(db, DATA_DB_PARQUET_SNAPSHOTS_KEY));
    const next = items.filter((item) => String(item.name || '') !== String(name || ''));
    if (next.length === items.length) return;
    if (next.length === 0) await idbDelete(db, DATA_DB_PARQUET_SNAPSHOTS_KEY);
    else await idbPut(db, DATA_DB_PARQUET_SNAPSHOTS_KEY, next);
  } finally {
    db.close();
  }
}
