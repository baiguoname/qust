function selectionEvent(innerKind, data) {
  return {
    kind: "selection",
    data: {
      kind: innerKind,
      data,
    },
  };
}

function queryEvent(innerKind, data) {
  return {
    kind: "query",
    data: {
      kind: innerKind,
      data,
    },
  };
}

function queryState(requestId, queryId, lifecycle, message = null) {
  return queryEvent("state", {
    request_id: String(requestId ?? ""),
    query_id: String(queryId ?? ""),
    lifecycle: String(lifecycle ?? "submitted"),
    message: message == null ? null : String(message),
  });
}

function queryError(requestId, message) {
  return queryEvent("error", {
    request_id: String(requestId ?? ""),
    message: String(message ?? "unknown query error"),
  });
}

function queryResult(requestId, payload) {
  return queryEvent("result", {
    request_id: String(requestId ?? ""),
    payload: {
      kind: "monitor",
      data: payload ?? {},
    },
  });
}

function pushU32Le(out, value) {
  const n = Number(value) >>> 0;
  out.push(n & 0xff, (n >>> 8) & 0xff, (n >>> 16) & 0xff, (n >>> 24) & 0xff);
}

function queryBinaryPayload(requestId, kind, binaryFormat, payload) {
  const body = payload instanceof Uint8Array ? payload : new Uint8Array(payload ?? []);
  const header = new TextEncoder().encode(JSON.stringify({
    request_id: String(requestId ?? ""),
    kind: String(kind ?? ""),
    binary_format: String(binaryFormat ?? ""),
  }));
  const out = new Uint8Array(4 + 1 + 4 + header.length + body.length);
  let cursor = 0;
  out[cursor++] = 0x4f; // O
  out[cursor++] = 0x4d; // M
  out[cursor++] = 0x51; // Q
  out[cursor++] = 0x50; // P
  out[cursor++] = 1;
  const len = [];
  pushU32Le(len, header.length);
  out.set(len, cursor);
  cursor += 4;
  out.set(header, cursor);
  cursor += header.length;
  out.set(body, cursor);
  return out;
}

function normalizeTextArg(raw, key, fallback = "") {
  const item = raw?.[key];
  if (!item || typeof item !== "object") return fallback;
  switch (item.kind) {
    case "text":
      return String(item.value ?? fallback);
    case "integer":
    case "float_text":
      return String(item.value ?? fallback);
    default:
      return fallback;
  }
}

function normalizeQueryArgs(argumentsMap) {
  return {
    code: normalizeTextArg(argumentsMap, "code", ""),
    theme: normalizeTextArg(argumentsMap, "__theme", "dark") || "dark",
  };
}

export function createMonitorSlotQueryRuntime({
  evalEngine,
  summarizePyError,
  setStatus,
}) {
  const queryPayloads = new Map();

  function decodeBinaryEnvelope(payload) {
    const bytes = payload instanceof Uint8Array ? payload : new Uint8Array(payload ?? []);
    if (bytes.length < 9) throw new Error("query binary payload too short");
    if (
      bytes[0] !== 0x4f || // O
      bytes[1] !== 0x4d || // M
      bytes[2] !== 0x51 || // Q
      bytes[3] !== 0x50    // P
    ) {
      throw new Error("query binary payload magic mismatch");
    }
    if (bytes[4] !== 1) throw new Error(`unsupported query binary payload version: ${bytes[4]}`);
    const headerLen = new DataView(bytes.buffer, bytes.byteOffset + 5, 4).getUint32(0, true);
    const headerEnd = 9 + headerLen;
    if (headerEnd > bytes.length) throw new Error("query binary payload header truncated");
    const headerJson = new TextDecoder().decode(bytes.slice(9, headerEnd));
    const header = JSON.parse(headerJson);
    return {
      header,
      requestId: String(header?.request_id ?? ""),
      payloadBytes: bytes.slice(headerEnd),
    };
  }

  function emit(wsLike, event) {
    if (typeof wsLike?.onmessage !== "function") return;
    wsLike.onmessage(
      new MessageEvent("message", {
        data: JSON.stringify(event),
      }),
    );
  }

  function emitBinary(wsLike, bytes) {
    if (typeof wsLike?.onmessage !== "function") return;
    const arr = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes ?? []);
    const ab = arr.buffer.slice(arr.byteOffset, arr.byteOffset + arr.byteLength);
    wsLike.onmessage(new MessageEvent("message", { data: ab }));
  }

  async function handleQueryExecute(wsLike, request) {
    const requestId = request?.request_id ?? "";
    const queryId = request?.query_id ?? "";
    try {
      const args = normalizeQueryArgs(request?.arguments ?? {});
      const queryPayload = queryPayloads.get(String(requestId ?? "")) || null;
      const payloadBytes = queryPayload?.payloadBytes || null;
      if (!payloadBytes || payloadBytes.length === 0) {
        throw new Error("当前 callback 缺少选区 payload，无法执行区间查询");
      }
      emit(wsLike, queryState(requestId, queryId, "submitted"));
      emit(wsLike, queryState(requestId, queryId, "running"));
      const kind = String(queryPayload?.header?.kind ?? "selection_arrow_ipc");
      const payload = kind === "scatter_select_request"
        ? await evalEngine.buildMonitorQueryResultFromScatterSelectRequest({
          code: args.code,
          scatterSelectRequestBytes: payloadBytes,
          theme: args.theme,
        })
        : await evalEngine.buildMonitorQueryResultFromSelectionPayload({
          code: args.code,
          selectionPayloadBytes: payloadBytes,
          theme: args.theme,
        });
      queryPayloads.delete(String(requestId ?? ""));
      const packets = Array.from(payload?.packets || []);
      for (const packet of packets) {
        emitBinary(
          wsLike,
          queryBinaryPayload(
            requestId,
            "query_monitor_packet",
            "monitor_packet",
            packet instanceof Uint8Array ? packet : new Uint8Array(packet ?? []),
          ),
        );
      }
      emit(wsLike, queryResult(requestId, payload?.result || payload || {}));
      emit(wsLike, queryState(requestId, queryId, "succeeded"));
    } catch (err) {
      const detail = (err && err.stack) ? err.stack : String(err);
      const brief = typeof summarizePyError === "function"
        ? summarizePyError(detail)
        : String(detail);
      if (typeof setStatus === "function") {
        setStatus(`区间查询失败: ${brief}`, true);
      }
      emit(wsLike, queryError(requestId, detail));
      emit(wsLike, queryState(requestId, queryId, "failed", detail));
    }
  }

  async function handleCommand(wsLike, outerKind, innerKind, innerData) {
    if (outerKind === "selection") {
      if (innerKind === "commit" && innerData && typeof innerData === "object") {
        emit(wsLike, selectionEvent("committed", innerData));
        return true;
      }
      if (innerKind === "clear") {
        emit(wsLike, selectionEvent("cleared", {
          slot_id: innerData?.slot_id ?? null,
        }));
        return true;
      }
      return false;
    }

    if (outerKind === "query") {
      if (innerKind === "execute" && innerData && typeof innerData === "object") {
        await handleQueryExecute(wsLike, innerData);
        return true;
      }
      if (innerKind === "request_catalog") {
        emit(wsLike, queryEvent("catalog", {
          revision: 1,
          entries: [],
        }));
        return true;
      }
      if (innerKind === "cancel") {
        emit(
          wsLike,
          queryState(
            innerData?.request_id ?? "",
            "",
            "cancelled",
            "当前本地回调查询不支持取消；已忽略该请求",
          ),
        );
        return true;
      }
      return false;
    }

    return false;
  }

  return {
    handleBinaryPayload(payload) {
      if (!(payload instanceof Uint8Array) || payload.length === 0) return;
      try {
        const decoded = decodeBinaryEnvelope(payload);
        const requestId = String(decoded?.requestId ?? "");
        const bytes = decoded?.payloadBytes;
        if (!requestId || !(bytes instanceof Uint8Array)) return;
        queryPayloads.set(requestId, decoded);
      } catch (_) {}
    },
    handleCommand,
  };
}
