import { buildParamsStateEvent } from "./monitor-callback-runtime.js?v=20260429_204125";
import { createMonitorSlotQueryRuntime } from "./monitor-slot-query-runtime.js?v=20260429_204125";

function packetEvent(innerKind, data) {
  return {
    kind: "packet",
    data: {
      kind: innerKind,
      data,
    },
  };
}

function actionsEvent(innerKind, data) {
  return {
    kind: "actions",
    data: {
      kind: innerKind,
      data,
    },
  };
}

function resizeMonitorCanvasForLayout(rows) {
  const canvas = document.getElementById("monitor_canvas");
  if (!(canvas instanceof HTMLCanvasElement)) return;
  const topbar = document.querySelector(".topbar");
  const verticalChrome =
    (topbar instanceof HTMLElement ? topbar.getBoundingClientRect().height : 0) + 20;
  const available = Math.max(360, Math.floor(window.innerHeight - verticalChrome));
  const rowCount = Math.max(1, Number(rows) || 1);
  const rowHeight = Math.max(520, Math.min(available, 760));
  const targetHeight = rowCount <= 1 ? available : Math.max(available, rowCount * rowHeight);
  canvas.style.setProperty("--monitor-canvas-h", `${Math.round(targetHeight)}px`);
  requestAnimationFrame(() => {
    window.dispatchEvent(new Event("resize"));
  });
}

function createRefreshScheduler(state, emitRefresh) {
  function triggerRefresh() {
    if (!state.socket || !state.scriptReady) return false;
    if (state.refreshInFlight) {
      state.refreshPending = true;
      return false;
    }
    state.refreshInFlight = true;
    state.lastRefreshMs = performance.now();
    state.refreshToken += 1;
    const token = state.refreshToken;
    void emitRefresh(token);
    return true;
  }

  function finishRefresh() {
    state.refreshInFlight = false;
    if (state.refreshPending) {
      state.refreshPending = false;
      scheduleRefresh(16);
    }
  }

  function scheduleRefresh(minIntervalMs = 140) {
    if (!state.scriptReady) return false;
    const now = performance.now();
    const elapsed = now - state.lastRefreshMs;
    if (elapsed >= minIntervalMs) {
      return triggerRefresh();
    }
    if (state.refreshQueued) return false;
    state.refreshQueued = true;
    const waitMs = Math.max(8, minIntervalMs - elapsed);
    if (state.refreshTimer) clearTimeout(state.refreshTimer);
    state.refreshTimer = setTimeout(() => {
      state.refreshTimer = null;
      state.refreshQueued = false;
      triggerRefresh();
    }, waitMs);
    return false;
  }

  return {
    triggerRefresh,
    finishRefresh,
    scheduleRefresh,
  };
}

export function createLocalMonitorSession({
  state,
  evalEngine,
  monitorCallbacksRuntime,
  ensureEngineReady,
  drainRuntimeLogs,
  refreshLoadedFilesPanel,
  refreshSavedFilesPanel,
  setStatus,
  summarizePyError,
}) {
  let session = null;
  const slotQueryRuntime = createMonitorSlotQueryRuntime({
    evalEngine,
    summarizePyError,
    setStatus,
  });

  const refresh = createRefreshScheduler(state, async (token) => {
    if (session) {
      await session.emitResetAndPacket(token);
    }
  });

  class LocalWs {
    constructor(url) {
      this.url = url;
      this.binaryType = "arraybuffer";
      this.readyState = LocalWs.OPEN;
      this.onmessage = null;
      this.onerror = null;
      this._booted = false;
      this._bootRetryTimer = null;
      this._layoutRevision = 0;
      this._paramsRevision = 0;
      this._actionsRevision = 0;
      state.socket = this;
      session = this;
      try {
        globalThis.__otters_active_monitor_socket = this;
      } catch (_) {}
    }

    send(payload) {
      if (payload instanceof Uint8Array || payload instanceof ArrayBuffer || ArrayBuffer.isView(payload)) {
        const bytes = payload instanceof Uint8Array
          ? payload
          : payload instanceof ArrayBuffer
            ? new Uint8Array(payload)
            : new Uint8Array(payload.buffer, payload.byteOffset, payload.byteLength);
        slotQueryRuntime.handleBinaryPayload(new Uint8Array(bytes));
        return;
      }
      let cmd;
      try {
        cmd = JSON.parse(payload);
      } catch {
        return;
      }
      const outerKind = cmd?.kind;
      const innerKind = cmd?.data?.kind;
      const innerData = cmd?.data?.data ?? null;
      if (outerKind === "selection" || outerKind === "query") {
        void slotQueryRuntime.handleCommand(this, outerKind, innerKind, innerData);
        return;
      }
      if (outerKind === "params") {
        if (innerKind === "request_state") {
          void this.emitParamsState();
          return;
        }
        if (innerKind === "set_value") {
          const key = innerData?.key;
          if (typeof key === "string" && key.length > 0) {
            const value = innerData?.value ?? null;
            if (value && typeof value === "object" && Object.prototype.hasOwnProperty.call(value, "kind")) {
              state.paramValues[key] = value.value;
            } else {
              state.paramValues[key] = value;
            }
          }
          void this.emitParamsState();
          refresh.scheduleRefresh(16);
          return;
        }
        if (innerKind === "reset") {
          state.paramValues = {};
          void this.emitParamsState();
          refresh.scheduleRefresh(16);
          return;
        }
        if (innerKind === "close_panel") {
          return;
        }
      }

      if (outerKind === "actions") {
        if (innerKind === "request_state") {
          void this.emitActionsState();
          return;
        }
        if (innerKind === "invoke") {
          const actionId = innerData?.action_id;
          void this.invokeAction(actionId);
          return;
        }
        if (innerKind === "close_panel") {
          return;
        }
      }
    }

    close() {
      this.readyState = LocalWs.CLOSED;
    }

    bootIfReady() {
      if (this._booted) return;
      if (typeof this.onmessage !== "function") {
        if (!this._bootRetryTimer) {
          this._bootRetryTimer = setTimeout(() => {
            this._bootRetryTimer = null;
            this.bootIfReady();
          }, 50);
        }
        return;
      }
      if (this._bootRetryTimer) {
        clearTimeout(this._bootRetryTimer);
        this._bootRetryTimer = null;
      }
      this._booted = true;
      void this.emitParamsState();
      void this.emitActionsState();
    }

    async emitParamsState() {
      this.bootIfReady();
      if (typeof this.onmessage !== "function") return;
      this._paramsRevision += 1;
      if (!state.scriptReady) {
        this.onmessage(
          new MessageEvent("message", {
            data: JSON.stringify(
              buildParamsStateEvent({
                revision: this._paramsRevision,
                busy: false,
                items: [],
                error: null,
              }),
            ),
          }),
        );
        return;
      }
      await monitorCallbacksRuntime.emitParamsState(this, {
        revision: this._paramsRevision,
        busy: false,
        error: null,
      });
    }

    actionCatalog() {
      return [
        {
          action_id: "refresh_monitor",
          label: "重新刷新",
          help: "立即重新计算当前 monitor。",
          inputs: [],
          enabled: !!state.scriptReady,
          destructive: false,
        },
        {
          action_id: "close_monitor",
          label: "关闭监控",
          help: "关闭当前 monitor 会话。",
          inputs: [],
          enabled: true,
          destructive: true,
        },
      ];
    }

    async emitActionsState({ busy = false, error = null } = {}) {
      this.bootIfReady();
      if (typeof this.onmessage !== "function") return;
      this._actionsRevision += 1;
      await monitorCallbacksRuntime.emitActionsState(this, {
        revision: this._actionsRevision,
        busy,
        items: this.actionCatalog(),
        error,
      });
    }

    async emitActionCompleted(actionId, message = null) {
      if (typeof this.onmessage !== "function") return;
      this.onmessage(
        new MessageEvent("message", {
          data: JSON.stringify(
            actionsEvent("completed", {
              action_id: String(actionId ?? ""),
              message,
            }),
          ),
        }),
      );
    }

    async emitActionFailed(actionId, message) {
      if (typeof this.onmessage !== "function") return;
      this.onmessage(
        new MessageEvent("message", {
          data: JSON.stringify(
            actionsEvent("failed", {
              action_id: String(actionId ?? ""),
              message: String(message ?? "unknown action error"),
            }),
          ),
        }),
      );
    }

    async invokeAction(actionId) {
      if (actionId === "refresh_monitor") {
        if (!state.scriptReady) {
          await this.emitActionFailed(actionId, "当前还没有可执行表达式，请先点击运行");
          await this.emitActionsState({ busy: false, error: null });
          return;
        }
        await this.emitActionsState({ busy: true, error: null });
        refresh.scheduleRefresh(16);
        await this.emitActionCompleted(actionId, "已提交刷新");
        return;
      }
      if (actionId === "close_monitor") {
        await this.emitActionCompleted(actionId, "监控已关闭");
        this.close();
        return;
      }
      await this.emitActionFailed(actionId, `未知动作: ${String(actionId ?? "")}`);
    }

    layoutSlotsFromJson(rows, cols, layoutJson) {
      try {
        const parsed = JSON.parse(layoutJson || "[]");
        if (Array.isArray(parsed) && parsed.length > 0) {
          return parsed.map((slot, idx) => ({
            slot_id: String(slot?.slot_id ?? `slot_${idx + 1}`),
            title: String(slot?.title ?? slot?.slot_id ?? `slot_${idx + 1}`),
            row: Math.max(1, Number(slot?.row) || 1),
            col: Math.max(1, Number(slot?.col) || 1),
            row_span: Math.max(1, Number(slot?.row_span) || 1),
            col_span: Math.max(1, Number(slot?.col_span) || 1),
            selection_source:
              slot?.selection_source && typeof slot.selection_source === "object"
                ? {
                    source_id: String(slot.selection_source.source_id ?? ""),
                    x_col: String(slot.selection_source.x_col ?? ""),
                  }
                : null,
            callbacks: Array.isArray(slot?.callbacks) ? slot.callbacks : [],
          }));
        }
      } catch (_) {}
      const slots = [];
      for (let row = 1; row <= Math.max(1, rows); row += 1) {
        for (let col = 1; col <= Math.max(1, cols); col += 1) {
          slots.push({
            slot_id: `r${row}c${col}`,
            title: `r${row}c${col}`,
            row,
            col,
            row_span: 1,
            col_span: 1,
            selection_source: null,
            callbacks: [],
          });
        }
      }
      return slots;
    }

    emitLayout(rows, cols, layoutJson) {
      if (typeof this.onmessage !== "function") return;
      resizeMonitorCanvasForLayout(rows);
      this._layoutRevision += 1;
      this.onmessage(
        new MessageEvent("message", {
          data: JSON.stringify(
            packetEvent("layout_updated", {
              revision: this._layoutRevision,
              rows,
              cols,
              slots: this.layoutSlotsFromJson(rows, cols, layoutJson),
              theme: "dark",
            }),
          ),
        }),
      );
    }

  async emitResetAndPacket(token) {
      this.bootIfReady();
      if (token !== state.refreshToken) {
        refresh.finishRefresh();
        return;
      }
      if (!state.scriptReady) {
        setStatus("请先点击运行，再进行参数刷新", true);
        await this.emitParamsState();
        await this.emitActionsState({ busy: false, error: null });
        refresh.finishRefresh();
        return;
      }
      if (typeof this.onmessage !== "function") {
        setStatus("monitor 未就绪（绘图内核未启动）", true);
        refresh.finishRefresh();
        return;
      }

      await monitorCallbacksRuntime.emitParamsState(this, {
        revision: this._paramsRevision + 1,
        busy: true,
        error: null,
      });
      this._paramsRevision += 1;
      await this.emitActionsState({ busy: true, error: null });
      setStatus("正在刷新");

      let hasMonitorLayout = true;
      try {
        await ensureEngineReady();
        await drainRuntimeLogs();
        const raw = await evalEngine.evalLayout();
        if (token !== state.refreshToken) {
          refresh.finishRefresh();
          return;
        }
        const parsed = JSON.parse(raw || "{}");
        const rows = Math.max(1, Number(parsed.rows) || 1);
        const cols = Math.max(1, Number(parsed.cols) || 1);
        const layoutJson = typeof parsed.layout_json === "string" ? parsed.layout_json : "[]";
        hasMonitorLayout = rows > 0 && cols > 0;
        this.emitLayout(rows, cols, layoutJson);
      } catch (err) {
        console.warn("eval_expr_monitor_layout failed:", err);
      }

      if (token !== state.refreshToken) {
        refresh.finishRefresh();
        return;
      }

      if (!hasMonitorLayout) {
        state.initialRefreshDone = true;
        await refreshLoadedFilesPanel();
        await refreshSavedFilesPanel();
        await drainRuntimeLogs();
        await monitorCallbacksRuntime.emitParamsState(this, {
          revision: this._paramsRevision + 1,
          busy: false,
          error: null,
        });
        this._paramsRevision += 1;
        await this.emitActionsState({ busy: false, error: null });
        setStatus("脚本运行完成（当前无 monitor 输出）");
        refresh.finishRefresh();
        return;
      }

      this.onmessage(
        new MessageEvent("message", {
          data: JSON.stringify(packetEvent("reset_requested", { reason: "wasm_refresh" })),
        }),
      );

      if (typeof this.onmessage !== "function") {
        refresh.finishRefresh();
        return;
      }

      try {
        const seq = state.seq;
        const packets = await evalEngine.evalPackets({
          datasetId: state.datasetId ?? 0,
          seq,
          paramsPending: state.paramValues ?? {},
        });
        if (token !== state.refreshToken) {
          refresh.finishRefresh();
          return;
        }
        const packetList = Array.from(packets || []);
        state.seq += BigInt(packetList.length);
        for (let i = 0; i < packetList.length; i += 1) {
          if (token !== state.refreshToken) {
            refresh.finishRefresh();
            return;
          }
          const packet = packetList[i];
          const arr = packet instanceof Uint8Array ? packet : new Uint8Array(packet);
          const ab = arr.buffer.slice(arr.byteOffset, arr.byteOffset + arr.byteLength);
          this.onmessage(new MessageEvent("message", { data: ab }));
        }
        state.initialRefreshDone = true;
        await refreshLoadedFilesPanel();
        await refreshSavedFilesPanel();
        await drainRuntimeLogs();
        await monitorCallbacksRuntime.emitParamsState(this, {
          revision: this._paramsRevision + 1,
          busy: false,
          error: null,
        });
        this._paramsRevision += 1;
        await this.emitActionsState({ busy: false, error: null });
        setStatus(`刷新完成（${packetList.length} 包）`);
      } catch (err) {
        const detail = (err && err.stack) ? err.stack : String(err);
        console.error("eval_expr_to_monitor_packet_dataset failed:", err);
        const brief = summarizePyError(detail);
        setStatus(`生成监控包失败: ${brief}`, true);
        await drainRuntimeLogs();
        await monitorCallbacksRuntime.emitParamsState(this, {
          revision: this._paramsRevision + 1,
          busy: false,
          error: detail,
        });
        this._paramsRevision += 1;
        await this.emitActionsState({ busy: false, error: detail });
      } finally {
        refresh.finishRefresh();
      }
    }
  }

  LocalWs.CONNECTING = 0;
  LocalWs.OPEN = 1;
  LocalWs.CLOSING = 2;
  LocalWs.CLOSED = 3;

  return {
    createSocket(url) {
      return new LocalWs(url);
    },
    scheduleRefresh: refresh.scheduleRefresh,
    triggerRefresh: refresh.triggerRefresh,
    finishRefresh: refresh.finishRefresh,
  };
}
