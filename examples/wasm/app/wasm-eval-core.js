import { createPyodideRuntime } from "./pyodide-runtime.js?v=20260428_092812";

function serializeParamsPending(paramsPending) {
  try {
    return JSON.stringify(paramsPending ?? {});
  } catch (_) {
    return "{}";
  }
}

export function createEvalEngineCore(deps) {
  deps = deps || {};
  const pyRuntime = createPyodideRuntime({ onProgress: deps.onProgress });
  let rawScript = "";
  let compiledExprBytes = new Uint8Array(0);
  let initialized = false;
  let compileInFlight = null;
  let preparedCalcToken = 0;

  async function init() {
    if (initialized) return;
    await pyRuntime.init();
    initialized = true;
  }

  function currentScript() {
    return compiledExprBytes;
  }

  function scriptMeta() {
    return pyRuntime.lastScriptMeta();
  }

  async function setScript(script) {
    const nextRaw = String(script ?? "");
    if (preparedCalcToken > 0) {
      try {
        pyRuntime.releasePreparedCalcOnlyToken(preparedCalcToken);
      } catch (_) {}
      preparedCalcToken = 0;
    }
    rawScript = nextRaw;
    compileInFlight = (async () => {
      await init();
      const bytes = await pyRuntime.compileExpressionBytes(nextRaw);
      compiledExprBytes = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes ?? []);
      // 关闭 prepared token 快路径：
      // 该路径在 wasm/pyodide 下会积累状态并触发致命崩溃（RuntimeError: unreachable）。
      // 当前阶段优先正确性与稳定性。
      preparedCalcToken = 0;
      return compiledExprBytes;
    })();
    return compileInFlight;
  }

  async function ensureScriptNormalized() {
    if (compileInFlight) {
      await compileInFlight;
      return;
    }
    if (!rawScript) return;
    await setScript(rawScript);
  }

  function evalLayout(params = {}) {
    const exprBytes = params.exprBytes ?? currentScript();
    return pyRuntime.evalExprMonitorLayoutBytes(exprBytes);
  }

  function evalParamsState(params = {}) {
    const exprBytes = params.exprBytes ?? currentScript();
    const pending = serializeParamsPending(params.paramsPending);
    return pyRuntime.evalExprParamsStateBytes(exprBytes, pending);
  }

  function evalPackets(params = {}) {
    const exprBytes = params.exprBytes ?? currentScript();
    const datasetId = params.datasetId;
    const seq = params.seq;
    const pending = serializeParamsPending(params.paramsPending);
    return pyRuntime.evalExprToMonitorPacketsDatasetWithParamsBytes(
      exprBytes,
      datasetId,
      seq,
      pending
    );
  }

  function evalCalcSummary(params = {}) {
    const exprBytes = params.exprBytes ?? currentScript();
    const datasetId = params.datasetId;
    const pending = serializeParamsPending(params.paramsPending);
    return pyRuntime.evalExprCalcSummaryDatasetWithParamsBytes(
      exprBytes,
      datasetId,
      pending
    );
  }

  function evalCalcOnly(params = {}) {
    const exprBytes = params.exprBytes ?? currentScript();
    const datasetId = params.datasetId;
    const pending = serializeParamsPending(params.paramsPending);
    return pyRuntime.evalExprCalcOnlyDatasetWithParamsBytes(
      exprBytes,
      datasetId,
      pending
    );
  }

  function uploadDataset(parquetBytes) {
    if (preparedCalcToken > 0) {
      try {
        pyRuntime.releasePreparedCalcOnlyToken(preparedCalcToken);
      } catch (_) {}
      preparedCalcToken = 0;
    }
    return pyRuntime.uploadParquetDatasetBytes(parquetBytes);
  }

  function uploadDatasetNamed(fileName, parquetBytes) {
    if (preparedCalcToken > 0) {
      try {
        pyRuntime.releasePreparedCalcOnlyToken(preparedCalcToken);
      } catch (_) {}
      preparedCalcToken = 0;
    }
    return pyRuntime.uploadParquetNamedBytes(String(fileName ?? ""), parquetBytes);
  }

  function listLoadedData() {
    return pyRuntime.listLoadedData();
  }

  function listCodeExamples() {
    return pyRuntime.listCodeExamples();
  }

  function renameLoadedData(oldName, newName) {
    return pyRuntime.renameLoadedData(String(oldName ?? ""), String(newName ?? ""));
  }

  function deleteLoadedData(name) {
    return pyRuntime.deleteLoadedData(String(name ?? ""));
  }

  function listSavedFiles() {
    return pyRuntime.listSavedFiles();
  }

  function renameSavedFile(oldName, newName) {
    return pyRuntime.renameSavedFile(String(oldName ?? ""), String(newName ?? ""));
  }

  function deleteSavedFile(fileName) {
    return pyRuntime.deleteSavedFile(String(fileName ?? ""));
  }

  function getSavedFileBytes(fileName) {
    return pyRuntime.getSavedFileBytes(String(fileName ?? ""));
  }

  function drainLogs() {
    return pyRuntime.drainLogs();
  }

  function buildMonitorQueryResultFromSelectionPayload(params = {}) {
    return pyRuntime.buildMonitorQueryResultFromSelectionPayload({
      code: params.code,
      selectionPayloadBytes: params.selectionPayloadBytes,
      theme: params.theme,
    });
  }

  function buildMonitorQueryResultFromScatterSelectRequest(params = {}) {
    return pyRuntime.buildMonitorQueryResultFromScatterSelectRequest({
      code: params.code,
      scatterSelectRequestBytes: params.scatterSelectRequestBytes,
      theme: params.theme,
    });
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
  };
}
