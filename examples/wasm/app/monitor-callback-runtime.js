function decodeBackendState(event, outerKind, innerKind) {
  if (!event || event.kind !== outerKind) return null;
  if (!event.data || event.data.kind !== innerKind) return null;
  return event.data.data ?? null;
}

function normalizeLegacyParamItem(item) {
  if (!item || typeof item !== 'object' || Array.isArray(item)) return null;
  const entries = Object.entries(item);
  if (entries.length !== 1) return null;
  const [kind, data] = entries[0];
  if (!data || typeof data !== 'object') return null;
  const title = String(data.title ?? '').trim();
  if (!title) return null;
  if (kind === 'Usize') {
    return {
      key: title,
      label: title,
      help: null,
      editor: {
        kind: 'integer_slider',
        data: {
          min: Number(data.start ?? 0),
          max: Number(data.end ?? 0),
          step: data.step == null ? null : Number(data.step),
        },
      },
      default_value: { kind: 'integer', value: Number(data.value ?? 0) },
      current_value: { kind: 'integer', value: Number(data.value ?? 0) },
      enabled: true,
    };
  }
  if (kind === 'F64') {
    return {
      key: title,
      label: title,
      help: null,
      editor: {
        kind: 'float_slider',
        data: {
          min: Number(data.start ?? 0),
          max: Number(data.end ?? 0),
          step: data.step == null ? null : Number(data.step),
        },
      },
      default_value: { kind: 'float', value: Number(data.value ?? 0) },
      current_value: { kind: 'float', value: Number(data.value ?? 0) },
      enabled: true,
    };
  }
  if (kind === 'VecString') {
    const pool = Array.isArray(data.pool) ? data.pool : [];
    return {
      key: title,
      label: title,
      help: null,
      editor: {
        kind: 'choice',
        data: {
          options: pool.map((x) => ({
            key: String(x),
            label: String(x),
            help: null,
          })),
        },
      },
      default_value: { kind: 'choice', value: String(data.value ?? '') },
      current_value: { kind: 'choice', value: String(data.value ?? '') },
      enabled: true,
    };
  }
  return null;
}

function normalizeLegacyParamsEvent(event) {
  if (!event || typeof event !== 'object' || event.kind !== 'params_state') return event;
  const items = Array.isArray(event.items)
    ? event.items.map(normalizeLegacyParamItem).filter(Boolean)
    : [];
  return buildParamsStateEvent({
    revision: Number(event.revision ?? 0),
    busy: !!event.busy,
    items,
    error: event.error ?? event.error_message ?? null,
    progress: {
      current: Number(event.progress_current ?? 0),
      total: Number(event.progress_total ?? 0),
      label: event.progress_label ?? null,
    },
  });
}

function encodeBackendState(outerKind, innerKind, state) {
  return {
    kind: outerKind,
    data: {
      kind: innerKind,
      data: state,
    },
  };
}

function normalizeProgress(progress) {
  const current = Number(progress?.current ?? 0);
  const total = Number(progress?.total ?? 0);
  return {
    current: Number.isFinite(current) ? current : 0,
    total: Number.isFinite(total) ? total : 0,
    label: progress?.label ?? null,
  };
}

function coerceParamValue(value) {
  if (value == null) return value;
  if (typeof value !== 'object') return value;
  if (!Object.prototype.hasOwnProperty.call(value, 'kind')) return value;
  switch (value.kind) {
    case 'integer':
    case 'float':
      return value.value;
    case 'text':
    case 'choice':
    case 'opaque_debug':
      return value.value ?? '';
    case 'boolean':
      return !!value.value;
    default:
      return value.value ?? value;
  }
}

function syncParamValuesFromEvent(state, event) {
  const panel = decodeBackendState(event, 'params', 'state');
  if (!panel || !Array.isArray(panel.items)) return;
  const next = {};
  for (const item of panel.items) {
    if (!item || !item.key) continue;
    const value = item.current_value ?? item.default_value ?? null;
    next[item.key] = coerceParamValue(value);
  }
  state.paramValues = next;
}

function cloneParamsEventWithOverrides(event, { revision = null, busy = false, error = null } = {}) {
  event = normalizeLegacyParamsEvent(event);
  const panel = decodeBackendState(event, 'params', 'state');
  if (!panel) return event;
  return encodeBackendState('params', 'state', {
    ...panel,
    revision: revision ?? panel.revision ?? 0,
    busy: !!busy,
    error_message: error ?? panel.error_message ?? null,
  });
}

export function buildActionsStateEvent({ revision = 1, busy = false, items = [], error = null } = {}) {
  return encodeBackendState('actions', 'state', {
    revision,
    busy: !!busy,
    items,
    error_message: error ?? null,
  });
}

export function buildParamsStateEvent({
  revision = 1,
  busy = false,
  items = [],
  error = null,
  progress = null,
} = {}) {
  return encodeBackendState('params', 'state', {
    revision,
    busy: !!busy,
    progress: normalizeProgress(progress),
    items,
    error_message: error ?? null,
  });
}

export function buildActionArg(kind, value) {
  return { kind, value };
}

export function protocolParamValueToPrimitive(value) {
  return coerceParamValue(value);
}

export function protocolCommandKind(command) {
  return command?.kind ?? null;
}

export function protocolInnerKind(command) {
  return command?.data?.kind ?? null;
}

export function protocolInnerData(command) {
  return command?.data?.data ?? null;
}

function fallbackParamsStateEvent(error) {
  return buildParamsStateEvent({
    revision: 0,
    busy: false,
    items: [],
    error,
  });
}

export function createMonitorCallbacksRuntime({
  state,
  evalEngine,
  ensureEngineReady,
  drainRuntimeLogs,
  appendOutputLines,
}) {
  async function evalParamsStateEvent({ revision = null, busy = false, error = null } = {}) {
    try {
      await ensureEngineReady();
      await drainRuntimeLogs();
      const payload = await evalEngine.evalParamsState({
        paramsPending: state.paramValues ?? {},
      });
      const event = JSON.parse(payload);
      const patched = cloneParamsEventWithOverrides(event, { revision, busy, error });
      syncParamValuesFromEvent(state, patched);
      return patched;
    } catch (err) {
      const detail = (err && err.stack) ? err.stack : String(err);
      appendOutputLines([`[stderr] ${detail}`]);
      const event = fallbackParamsStateEvent(error ?? detail);
      syncParamValuesFromEvent(state, event);
      return event;
    }
  }

  async function emitParamsState(wsLike, options = {}) {
    if (typeof wsLike?.onmessage !== 'function') return null;
    const event = await evalParamsStateEvent(options);
    wsLike.onmessage(new MessageEvent('message', { data: JSON.stringify(event) }));
    return event;
  }

  async function emitActionsState(wsLike, options = {}) {
    if (typeof wsLike?.onmessage !== 'function') return null;
    const event = buildActionsStateEvent(options);
    wsLike.onmessage(new MessageEvent('message', { data: JSON.stringify(event) }));
    return event;
  }

  return {
    evalParamsStateEvent,
    emitParamsState,
    emitActionsState,
  };
}
