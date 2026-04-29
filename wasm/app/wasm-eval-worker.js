import { createEvalEngineCore } from "./wasm-eval-core.js?v=20260429_204125";

function progress(percent, label) {
  self.postMessage({
    type: "progress",
    progress: {
      percent: Number(percent) || 0,
      label: String(label || ""),
    },
  });
}

const engine = createEvalEngineCore({ onProgress: progress });

function asU8(input) {
  if (input instanceof Uint8Array) return input;
  if (input instanceof ArrayBuffer) return new Uint8Array(input);
  if (ArrayBuffer.isView(input)) {
    return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
  }
  if (Array.isArray(input)) return Uint8Array.from(input);
  return new Uint8Array(0);
}

function ok(id, result, transfer = []) {
  self.postMessage({ id, ok: true, result }, transfer);
}

function err(id, e) {
  let detail = String((e && (e.stack || e.message)) || e || "unknown worker error");
  try {
    if (e && typeof e === "object") {
      const keys = Object.getOwnPropertyNames(e);
      const dump = {};
      for (const k of keys) {
        try {
          const v = e[k];
          dump[k] = typeof v === "string" ? v : String(v);
        } catch (_) {}
      }
      detail += `\n[worker_error_detail] ${JSON.stringify(dump)}`;
      if (e.cause != null) {
        detail += `\n[worker_error_cause] ${String(e.cause && (e.cause.stack || e.cause.message) || e.cause)}`;
      }
    }
  } catch (_) {}
  self.postMessage({ id, ok: false, error: detail });
}

self.onmessage = async (ev) => {
  const data = ev.data || {};
  const id = data.id;
  const method = data.method;
  const params = data.params || {};
  try {
    switch (method) {
      case "init": {
        await engine.init();
        ok(id, true);
        return;
      }
      case "setScript": {
        await engine.setScript(String(params.script ?? ""));
        ok(id, true);
        return;
      }
      case "ensureScriptNormalized": {
        await engine.ensureScriptNormalized();
        ok(id, true);
        return;
      }
      case "currentScript": {
        const bytes = asU8(engine.currentScript());
        ok(id, bytes, [bytes.buffer]);
        return;
      }
      case "scriptMeta": {
        ok(id, engine.scriptMeta());
        return;
      }
      case "uploadDataset": {
        const bytes = asU8(params.parquetBytes);
        const meta = engine.uploadDataset(bytes);
        ok(id, meta);
        return;
      }
      case "uploadDatasetNamed": {
        const bytes = asU8(params.parquetBytes);
        const meta = engine.uploadDatasetNamed(String(params.fileName ?? ""), bytes);
        ok(id, meta);
        return;
      }
      case "listLoadedData": {
        const items = Array.from(engine.listLoadedData() || []);
        ok(id, items);
        return;
      }
      case "listCodeExamples": {
        ok(id, engine.listCodeExamples() || {});
        return;
      }
      case "renameLoadedData": {
        const meta = engine.renameLoadedData(String(params.oldName ?? ""), String(params.newName ?? ""));
        ok(id, meta);
        return;
      }
      case "deleteLoadedData": {
        engine.deleteLoadedData(String(params.name ?? ""));
        ok(id, true);
        return;
      }
      case "listSavedFiles": {
        const items = Array.from(engine.listSavedFiles() || []);
        ok(id, items);
        return;
      }
      case "renameSavedFile": {
        const meta = engine.renameSavedFile(String(params.oldName ?? ""), String(params.newName ?? ""));
        ok(id, meta);
        return;
      }
      case "deleteSavedFile": {
        engine.deleteSavedFile(String(params.fileName ?? ""));
        ok(id, true);
        return;
      }
      case "getSavedFileBytes": {
        const bytes = asU8(engine.getSavedFileBytes(String(params.fileName ?? "")));
        ok(id, bytes, [bytes.buffer]);
        return;
      }
      case "evalLayout": {
        const text = engine.evalLayout({ exprBytes: params.exprBytes });
        ok(id, text);
        return;
      }
      case "evalParamsState": {
        const text = engine.evalParamsState({
          exprBytes: params.exprBytes,
          paramsPending: params.paramsPending,
        });
        ok(id, text);
        return;
      }
      case "evalPackets": {
        const packets = Array.from(engine.evalPackets({
          exprBytes: params.exprBytes,
          datasetId: params.datasetId,
          seq: params.seq,
          paramsPending: params.paramsPending,
        }) || []);
        const out = packets.map(asU8);
        const transfer = out
          .filter((x) => x.byteOffset === 0 && x.byteLength === x.buffer.byteLength)
          .map((x) => x.buffer);
        ok(id, out, transfer);
        return;
      }
      case "evalCalcSummary": {
        const text = engine.evalCalcSummary({
          exprBytes: params.exprBytes,
          datasetId: params.datasetId,
          paramsPending: params.paramsPending,
        });
        ok(id, text);
        return;
      }
      case "evalCalcOnly": {
        const n = engine.evalCalcOnly({
          exprBytes: params.exprBytes,
          datasetId: params.datasetId,
          paramsPending: params.paramsPending,
        });
        ok(id, n);
        return;
      }
      case "drainLogs": {
        const lines = Array.from(engine.drainLogs() || []);
        ok(id, lines);
        return;
      }
      case "buildMonitorQueryResultFromSelectionPayload": {
        const payload = engine.buildMonitorQueryResultFromSelectionPayload({
          code: params.code,
          selectionPayloadBytes: params.selectionPayloadBytes == null ? null : asU8(params.selectionPayloadBytes),
          theme: params.theme,
        });
        const packets = Array.from(payload?.packets || []).map(asU8);
        const out = { result: payload?.result || {}, packets };
        const transfer = packets
          .filter((x) => x.byteOffset === 0 && x.byteLength === x.buffer.byteLength)
          .map((x) => x.buffer);
        ok(id, out, transfer);
        return;
      }
      case "buildMonitorQueryResultFromScatterSelectRequest": {
        const payload = engine.buildMonitorQueryResultFromScatterSelectRequest({
          code: params.code,
          scatterSelectRequestBytes: params.scatterSelectRequestBytes == null ? null : asU8(params.scatterSelectRequestBytes),
          theme: params.theme,
        });
        const packets = Array.from(payload?.packets || []).map(asU8);
        const out = { result: payload?.result || {}, packets };
        const transfer = packets
          .filter((x) => x.byteOffset === 0 && x.byteLength === x.buffer.byteLength)
          .map((x) => x.buffer);
        ok(id, out, transfer);
        return;
      }
      default:
        throw new Error(`unknown worker method: ${method}`);
    }
  } catch (e) {
    err(id, e);
  }
};
