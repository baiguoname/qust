const WORKER_URL = new URL("./wasm-eval-worker.js?v=/root/otters/otters-py/wasm/web", import.meta.url);

function createRpcWorker(deps = {}) {
  const worker = new Worker(WORKER_URL, { type: "module" });
  let reqId = 1;
  const pending = new Map();

  worker.onmessage = (ev) => {
    const msg = ev.data || {};
    if (msg.type === "progress") {
      try {
        deps.onProgress?.(msg.progress || {});
      } catch (_) {}
      return;
    }
    const slot = pending.get(msg.id);
    if (!slot) return;
    pending.delete(msg.id);
    if (msg.ok) slot.resolve(msg.result);
    else slot.reject(new Error(String(msg.error || "worker rpc failed")));
  };

  worker.onerror = (ev) => {
    const detail = String(ev?.message || "worker runtime error");
    for (const [, slot] of pending) {
      slot.reject(new Error(detail));
    }
    pending.clear();
  };

  function call(method, params = {}, transfer = []) {
    return new Promise((resolve, reject) => {
      const id = reqId++;
      pending.set(id, { resolve, reject });
      worker.postMessage({ id, method, params }, transfer);
    });
  }

  return { worker, call };
}

function toU8(input) {
  if (input instanceof Uint8Array) return new Uint8Array(input);
  if (input instanceof ArrayBuffer) return new Uint8Array(input);
  if (ArrayBuffer.isView(input)) {
    return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
  }
  if (Array.isArray(input)) return Uint8Array.from(input);
  return new Uint8Array(0);
}

export function createEvalEngine(deps) {
  deps = deps || {};
  let rpc = createRpcWorker(deps);
  let rawScript = "";
  let compileInFlight = null;
  let initialized = false;
  let recovering = false;
  let lastDatasetBytes = null;
  let currentDatasetId = null;
  const datasetAlias = new Map();

  function isRecoverableError(err) {
    const text = String((err && (err.stack || err.message)) || err || "");
    return (
      text.includes("RuntimeError: unreachable")
      || text.includes("pyodide_fatal_error")
      || text.includes("worker runtime error")
      || text.includes("worker rpc failed")
    );
  }

  async function _initNoRecover() {
    if (initialized) return;
    await rpc.call("init");
    initialized = true;
  }

  async function _setScriptNoRecover(script) {
    const nextRaw = String(script ?? "");
    rawScript = nextRaw;
    compileInFlight = rpc.call("setScript", { script: nextRaw });
    try {
      await compileInFlight;
    } finally {
      compileInFlight = null;
    }
  }

  async function recoverRuntime() {
    try {
      rpc.worker.terminate();
    } catch (_) {}
    rpc = createRpcWorker(deps);
    initialized = false;
    compileInFlight = null;
    await _initNoRecover();
    if (rawScript) {
      await _setScriptNoRecover(rawScript);
    }
    if (lastDatasetBytes && lastDatasetBytes.length > 0) {
      const oldId = currentDatasetId;
      const meta = await rpc.call("uploadDataset", {
        parquetBytes: new Uint8Array(lastDatasetBytes),
      });
      if (oldId != null && oldId !== meta?.dataset_id) {
        datasetAlias.set(oldId, meta.dataset_id);
      }
      currentDatasetId = meta?.dataset_id ?? currentDatasetId;
    }
  }

  async function withRecovery(task, maxRetries = 3) {
    let attempt = 0;
    let lastErr = null;
    while (attempt <= maxRetries) {
      try {
        return await task();
      } catch (err) {
        lastErr = err;
        if (recovering || !isRecoverableError(err) || attempt >= maxRetries) {
          throw err;
        }
        recovering = true;
        try {
          await recoverRuntime();
        } finally {
          recovering = false;
        }
        attempt += 1;
      }
    }
    throw lastErr;
  }

  function resolveDatasetId(datasetId) {
    const id = datasetId ?? currentDatasetId;
    if (id == null) return id;
    return datasetAlias.get(id) ?? id;
  }

  async function init() {
    await withRecovery(() => _initNoRecover());
  }

  async function setScript(script) {
    await withRecovery(() => _setScriptNoRecover(script));
  }

  async function ensureScriptNormalized() {
    await withRecovery(async () => {
      if (compileInFlight) {
        await compileInFlight;
        return;
      }
      if (!rawScript) return;
      await _setScriptNoRecover(rawScript);
    });
  }

  async function currentScript() {
    const bytes = await withRecovery(() => rpc.call("currentScript"));
    return bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes ?? []);
  }

  async function scriptMeta() {
    return withRecovery(() => rpc.call("scriptMeta"));
  }

  async function evalLayout(params = {}) {
    return withRecovery(() => rpc.call("evalLayout", { exprBytes: params.exprBytes }));
  }

  async function evalParamsState(params = {}) {
    return withRecovery(() => rpc.call("evalParamsState", {
      exprBytes: params.exprBytes,
      paramsPending: params.paramsPending,
    }));
  }

  async function evalPackets(params = {}) {
    const out = await withRecovery(() => rpc.call("evalPackets", {
      exprBytes: params.exprBytes,
      datasetId: resolveDatasetId(params.datasetId),
      seq: params.seq,
      paramsPending: params.paramsPending,
    }));
    return Array.from(out || []).map((x) =>
      x instanceof Uint8Array ? x : new Uint8Array(x ?? [])
    );
  }

  async function evalCalcSummary(params = {}) {
    return withRecovery(() => rpc.call("evalCalcSummary", {
      exprBytes: params.exprBytes,
      datasetId: resolveDatasetId(params.datasetId),
      paramsPending: params.paramsPending,
    }));
  }

  async function evalCalcOnly(params = {}) {
    return withRecovery(() => rpc.call("evalCalcOnly", {
      exprBytes: params.exprBytes,
      datasetId: resolveDatasetId(params.datasetId),
      paramsPending: params.paramsPending,
    }));
  }

  async function uploadDataset(parquetBytes) {
    const bytes = toU8(parquetBytes);
    const payload = new Uint8Array(bytes);
    const meta = await withRecovery(() => rpc.call("uploadDataset", { parquetBytes: payload }));
    if (currentDatasetId != null && currentDatasetId !== meta?.dataset_id) {
      datasetAlias.set(currentDatasetId, meta.dataset_id);
    }
    currentDatasetId = meta?.dataset_id ?? currentDatasetId;
    lastDatasetBytes = new Uint8Array(payload);
    return meta;
  }

  async function uploadDatasetNamed(fileName, parquetBytes) {
    const bytes = toU8(parquetBytes);
    const payload = new Uint8Array(bytes);
    const meta = await withRecovery(() => rpc.call("uploadDatasetNamed", {
      fileName: String(fileName ?? ""),
      parquetBytes: payload,
    }));
    if (currentDatasetId != null && currentDatasetId !== meta?.dataset_id) {
      datasetAlias.set(currentDatasetId, meta.dataset_id);
    }
    currentDatasetId = meta?.dataset_id ?? currentDatasetId;
    lastDatasetBytes = new Uint8Array(payload);
    return meta;
  }

  async function listLoadedData() {
    const items = await withRecovery(() => rpc.call("listLoadedData"));
    return Array.from(items || []);
  }

  async function listCodeExamples() {
    const items = await withRecovery(() => rpc.call("listCodeExamples"));
    return items && typeof items === "object" ? items : {};
  }

  async function renameLoadedData(oldName, newName) {
    return withRecovery(() => rpc.call("renameLoadedData", {
      oldName: String(oldName ?? ""),
      newName: String(newName ?? ""),
    }));
  }

  async function deleteLoadedData(name) {
    return withRecovery(() => rpc.call("deleteLoadedData", {
      name: String(name ?? ""),
    }));
  }

  async function listSavedFiles() {
    const items = await withRecovery(() => rpc.call("listSavedFiles"));
    return Array.from(items || []);
  }

  async function renameSavedFile(oldName, newName) {
    return withRecovery(() => rpc.call("renameSavedFile", {
      oldName: String(oldName ?? ""),
      newName: String(newName ?? ""),
    }));
  }

  async function deleteSavedFile(fileName) {
    return withRecovery(() => rpc.call("deleteSavedFile", {
      fileName: String(fileName ?? ""),
    }));
  }

  async function getSavedFileBytes(fileName) {
    const out = await withRecovery(() =>
      rpc.call("getSavedFileBytes", { fileName: String(fileName ?? "") })
    );
    return out instanceof Uint8Array ? out : new Uint8Array(out ?? []);
  }

  async function drainLogs() {
    return withRecovery(() => rpc.call("drainLogs"));
  }

  async function buildMonitorQueryResultFromSelectionPayload(params = {}) {
    const selectionPayloadBytes =
      params.selectionPayloadBytes == null ? null : toU8(params.selectionPayloadBytes);
    const transfer = [];
    if (
      selectionPayloadBytes instanceof Uint8Array
      && selectionPayloadBytes.byteOffset === 0
      && selectionPayloadBytes.byteLength === selectionPayloadBytes.buffer.byteLength
    ) {
      transfer.push(selectionPayloadBytes.buffer);
    }
    return withRecovery(() => rpc.call(
      "buildMonitorQueryResultFromSelectionPayload",
      {
        code: String(params.code ?? ""),
        selectionPayloadBytes,
        theme: params.theme,
      },
      transfer,
    ));
  }

  async function buildMonitorQueryResultFromScatterSelectRequest(params = {}) {
    const scatterSelectRequestBytes =
      params.scatterSelectRequestBytes == null ? null : toU8(params.scatterSelectRequestBytes);
    const transfer = [];
    if (
      scatterSelectRequestBytes instanceof Uint8Array
      && scatterSelectRequestBytes.byteOffset === 0
      && scatterSelectRequestBytes.byteLength === scatterSelectRequestBytes.buffer.byteLength
    ) {
      transfer.push(scatterSelectRequestBytes.buffer);
    }
    return withRecovery(() => rpc.call(
      "buildMonitorQueryResultFromScatterSelectRequest",
      {
        code: String(params.code ?? ""),
        scatterSelectRequestBytes,
        theme: params.theme,
      },
      transfer,
    ));
  }

  function terminate() {
    try {
      rpc.worker.terminate();
    } catch (_) {}
    initialized = false;
    compileInFlight = null;
    recovering = false;
  }

  return {
    init,
    setScript,
    ensureScriptNormalized,
    currentScript,
    scriptMeta,
    evalLayout,
    evalParamsState,
    evalPackets,
    evalCalcSummary,
    evalCalcOnly,
    uploadDataset,
    uploadDatasetNamed,
    listLoadedData,
    listCodeExamples,
    renameLoadedData,
    deleteLoadedData,
    listSavedFiles,
    renameSavedFile,
    deleteSavedFile,
    getSavedFileBytes,
    drainLogs,
    buildMonitorQueryResultFromSelectionPayload,
    buildMonitorQueryResultFromScatterSelectRequest,
    terminate,
  };
}
