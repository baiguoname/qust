const DEFAULT_PYODIDE_CDN = new URL("../pyodide/", import.meta.url).href;
const QUST_WHEEL_MANIFEST_URL = new URL("../wheels/manifest.json", import.meta.url);
const CODE_EXAMPLES_TOML_URL = new URL("./code_examples.toml", import.meta.url).href;

let _pyScriptPromise = null;
let _pyodidePromise = null;
let _runtimeInstallPromise = null;
let _codeExamplesCache = null;
const _runtimeLogBuffer = [];
const RUNTIME_LOG_MAX_LINES = 4000;
let _progressReporter = null;

function reportProgress(percent, label) {
  try {
    const pct = Math.max(0, Math.min(100, Math.round(Number(percent) || 0)));
    _progressReporter?.(pct, String(label || ""));
  } catch (_) {}
}

function loadDataBytesSync(path) {
  const raw = String(path ?? "").trim();
  if (!raw) throw new Error("load_data path is empty");
  const isRemote = raw.startsWith("http://") || raw.startsWith("https://");

  const candidates = [];
  const addCandidate = (label, url) => {
    if (!url || candidates.some((item) => item.url === url)) return;
    candidates.push({ label, url });
  };

  if (isRemote) {
    let fileName = "";
    try {
      fileName = new URL(raw).pathname.split("/").filter(Boolean).pop() || "";
    } catch (_) {}
    if (fileName) {
      addCandidate("pages-data", new URL(`../data/${fileName}`, self.location.href).href);
      addCandidate("pages-root-data", new URL(`/data/${fileName}`, self.location.origin).href);
      addCandidate("pages-parent-data", new URL(`../../data/${fileName}`, self.location.href).href);
    }
    addCandidate("dev-proxy", `${self.location.origin}/__otters_proxy?url=${encodeURIComponent(raw)}`);
    addCandidate("remote", raw);
  } else {
    addCandidate("relative", new URL(raw, self.location.origin + "/").href);
    addCandidate("page-relative", new URL(raw, self.location.href).href);
  }

  const errors = [];
  for (const { label, url } of candidates) {
    const xhr = new XMLHttpRequest();
    try {
      xhr.open("GET", url, false);
      xhr.responseType = "arraybuffer";
      xhr.send();
    } catch (err) {
      errors.push(`${label}: ${url}: ${err?.message || err}`);
      continue;
    }
    if (xhr.status < 200 || xhr.status >= 300) {
      errors.push(`${label}: status=${xhr.status}, url=${url}`);
      continue;
    }
    const buf = xhr.response;
    if (!buf) {
      errors.push(`${label}: empty response, url=${url}`);
      continue;
    }
    return new Uint8Array(buf);
  }

  throw new Error(`load_data fetch failed: path=${raw}; tried ${errors.join(" | ")}`);
}

function loadTextSync(path) {
  const raw = String(path ?? "").trim();
  if (!raw) throw new Error("load text path is empty");
  const url = raw.startsWith("http://") || raw.startsWith("https://")
    ? raw
    : `${self.location.origin}${raw.startsWith("/") ? raw : `/${raw}`}`;
  const xhr = new XMLHttpRequest();
  xhr.open("GET", url, false);
  xhr.responseType = "text";
  xhr.send();
  if (xhr.status < 200 || xhr.status >= 300) {
    throw new Error(`relative text fetch failed: status=${xhr.status}, url=${url}`);
  }
  return String(xhr.responseText ?? "");
}

globalThis.__otters_load_data_sync = loadDataBytesSync;
globalThis.__otters_load_data_relative_sync = loadDataBytesSync;

function pushRuntimeLog(level, text) {
  const msg = String(text ?? "").replace(/\r\n/g, "\n");
  const lines = msg.split("\n");
  for (const raw of lines) {
    const line = raw.trimEnd();
    if (!line) continue;
    _runtimeLogBuffer.push(`[${level}] ${line}`);
    if (_runtimeLogBuffer.length > RUNTIME_LOG_MAX_LINES) {
      _runtimeLogBuffer.splice(0, _runtimeLogBuffer.length - RUNTIME_LOG_MAX_LINES);
    }
  }
}

function drainRuntimeLogs() {
  if (_runtimeLogBuffer.length === 0) return [];
  const out = _runtimeLogBuffer.slice();
  _runtimeLogBuffer.length = 0;
  return out;
}

function ensureWasmTableCapacity(pyodide, minLen = 220000) {
  try {
    const mods = [
      pyodide?._module,
      globalThis.Module,
      self.Module,
    ].filter(Boolean);
    for (const mod of mods) {
      const table = mod?.wasmTable || mod?.__indirect_function_table || mod?.asm?.__indirect_function_table || null;
      const cur = Number(table?.length ?? 0);
      if (!table || !Number.isFinite(cur) || cur <= 0) continue;
      if (cur >= minLen) continue;
      const delta = minLen - cur;
      if (delta > 0 && typeof table.grow === "function") {
        table.grow(delta);
      }
    }
  } catch (_) {
    // 不阻断安装；失败时后续会在 dlopen 阶段报错。
  }
}

function readUleb(bytes, start) {
  let val = 0;
  let shift = 0;
  let i = start;
  while (i < bytes.length) {
    const b = bytes[i++];
    val |= (b & 0x7f) << shift;
    if ((b & 0x80) === 0) break;
    shift += 7;
  }
  return [val >>> 0, i];
}

function parseLegacyDylinkPayload(payload) {
  const bytes = payload instanceof Uint8Array ? payload : null;
  if (!bytes) return null;
  const meta = {
    neededDynlibs: [],
    tlsExports: new Set(),
    weakImports: new Set(),
    runtimePaths: [],
  };
  try {
    let i = 0;
    [meta.memorySize, i] = readUleb(bytes, i);
    [meta.memoryAlign, i] = readUleb(bytes, i);
    [meta.tableSize, i] = readUleb(bytes, i);
    [meta.tableAlign, i] = readUleb(bytes, i);
    let n = 0;
    [n, i] = readUleb(bytes, i);
    const dec = new TextDecoder();
    for (let k = 0; k < n; k += 1) {
      let ln = 0;
      [ln, i] = readUleb(bytes, i);
      const end = i + ln;
      if (end > bytes.length) return null;
      meta.neededDynlibs.push(dec.decode(bytes.slice(i, end)));
      i = end;
    }
    return meta;
  } catch (_) {
    return null;
  }
}

function extractLegacyDylinkMetadata(binary) {
  try {
    if (binary instanceof WebAssembly.Module) {
      const secs = WebAssembly.Module.customSections(binary, "dylink");
      if (secs && secs.length > 0) {
        return parseLegacyDylinkPayload(new Uint8Array(secs[0]));
      }
      return null;
    }

    const bytes = binary instanceof Uint8Array ? binary : null;
    if (!bytes || bytes.length < 16) return null;
    if (!(bytes[0] === 0x00 && bytes[1] === 0x61 && bytes[2] === 0x73 && bytes[3] === 0x6d)) {
      return null;
    }
    if (bytes[8] !== 0) return null;

    const [secSize, secSizeEnd] = readUleb(bytes, 9);
    const secStart = secSizeEnd;
    const secEnd = secStart + secSize;
    if (secEnd > bytes.length) return null;

    const [nameLen, nameLenEnd] = readUleb(bytes, secStart);
    const nameStart = nameLenEnd;
    const nameEnd = nameStart + nameLen;
    if (nameEnd > secEnd) return null;

    const name = new TextDecoder().decode(bytes.slice(nameStart, nameEnd));
    if (name !== "dylink") return null;
    return parseLegacyDylinkPayload(bytes.slice(nameEnd, secEnd));
  } catch (_) {
    return null;
  }
}

function patchDylinkMetadataParser(pyodide) {
  try {
    const mod = pyodide?._module;
    if (!mod || typeof mod.getDylinkMetadata !== "function") return;
    if (mod.__otters_dylink_patch_applied) return;

    const old = mod.getDylinkMetadata.bind(mod);
    mod.getDylinkMetadata = (binary) => {
      try {
        return old(binary);
      } catch (e) {
        // pyodide 的 getDylinkMetadata 在 name!=dylink.0 分支会抛出空消息 Error，
        // 这里不能依赖 message 文本判断，统一尝试 legacy dylink 回退。
        const legacy = extractLegacyDylinkMetadata(binary);
        if (!legacy) throw e;
        return legacy;
      }
    };
    mod.__otters_dylink_patch_applied = true;
  } catch (_) {
    // 忽略补丁失败；失败会在后续 dlopen 报错暴露。
  }
}

function configureEmscriptenThreadHints() {
  try {
    const mod = (globalThis.Module && typeof globalThis.Module === "object") ? globalThis.Module : {};
    const hw = Number(globalThis.navigator?.hardwareConcurrency || 0);
    // 让 emscripten 在支持线程时预建 worker，避免运行时线程创建失败。
    if (typeof mod.pthreadPoolSize !== "number") {
      mod.pthreadPoolSize = Math.max(2, hw > 1 ? hw - 1 : 4);
    }
    globalThis.Module = mod;
  } catch (_) {
    // no-op
  }
}

function ensurePyodideScript(cdnBase) {
  if (typeof globalThis.loadPyodide === "function") {
    return Promise.resolve();
  }
  if (_pyScriptPromise) return _pyScriptPromise;
  configureEmscriptenThreadHints();
  _pyScriptPromise = new Promise((resolve, reject) => {
    const src = `${cdnBase}pyodide.js`;
    reportProgress(8, "加载 Pyodide 引导脚本");
    if (typeof document === "undefined") {
      (async () => {
        // module worker 不支持 importScripts；这里优先尝试 importScripts，失败后回退 fetch+eval。
        if (typeof globalThis.importScripts === "function") {
          try {
            globalThis.importScripts(src);
            resolve();
            return;
          } catch (_) {
            // fallback below
          }
        }
        try {
          const resp = await fetch(src);
          if (!resp.ok) {
            reject(new Error(`fetch pyodide.js failed: ${src} status=${resp.status}`));
            return;
          }
          const scriptText = await resp.text();
          reportProgress(14, "解析 Pyodide 引导脚本");
          // eslint-disable-next-line no-eval
          (0, eval)(scriptText);
          if (typeof globalThis.loadPyodide !== "function") {
            reject(new Error(`pyodide.js loaded but loadPyodide missing: ${src}`));
            return;
          }
          resolve();
        } catch (e) {
          reject(new Error(`load pyodide.js failed in worker: ${String(e)}`));
        }
      })();
      return;
    }
    const script = document.createElement("script");
    script.src = src;
    script.async = true;
    script.onload = () => resolve();
    script.onerror = () => reject(new Error(`load pyodide.js failed: ${src}`));
    document.head.appendChild(script);
  });
  return _pyScriptPromise;
}

const INSTALL_BOOTSTRAP_CODE = String.raw`
import sys
import types
import ast
import importlib
import os
import json

# wasm 运行时优先开启多线程：根据 hardwareConcurrency 估算线程数。
_hw = 0
try:
    import js
    _hw = int(getattr(js.navigator, "hardwareConcurrency", 0) or 0)
except Exception:
    _hw = 0

# 线程数策略：
# 1) 若外部已设置 OTTERS_WASM_THREADS，则尊重该值；
# 2) 否则按硬件并发自动估算（至少 1，最多 16，优先 hw-1）。
_mt_env = os.environ.get("OTTERS_WASM_THREADS")
if _mt_env is not None and str(_mt_env).strip() != "":
    try:
        _mt = max(1, int(_mt_env))
    except Exception:
        _mt = 1
else:
    if _hw <= 1:
        _mt = 1
    else:
        _mt = max(1, min(16, _hw - 1))

os.environ["OTTERS_WASM_THREADS"] = str(_mt)
os.environ["RAYON_NUM_THREADS"] = str(_mt)
os.environ["POLARS_MAX_THREADS"] = str(_mt)
print(f"wasm thread hint: hw={_hw}, mt={_mt}")

# qust 的 python 包会 import polars；在 wasm 场景先放一个最小 stub（不做计算）
if "polars" not in sys.modules:
    import json

    class _DummyMeta:
        def __init__(self, node):
            self._node = node

        def serialize(self, *args, **kwargs):
            return json.dumps(self._node)

    class _DummyExpr:
        def __init__(self, node=None):
            self._node = node if node is not None else {"Literal": {"Scalar": {"Null": "Null"}}}
            self.meta = _DummyMeta(self._node)

        def __getattr__(self, _name):
            def _f(*_args, **_kwargs):
                return _DummyExpr()
            return _f

    class _ApiNS:
        @staticmethod
        def register_expr_namespace(_name):
            def _deco(cls): return cls
            return _deco
        @staticmethod
        def register_dataframe_namespace(_name):
            def _deco(cls): return cls
            return _deco

    pm = types.ModuleType("polars")
    pm.api = _ApiNS()
    class DataFrame:
        def __init__(self, data=None, *args, **kwargs):
            if isinstance(data, DataFrame):
                self._data = {k: list(v) for k, v in data._data.items()}
            elif isinstance(data, dict):
                normalized = {}
                width = None
                for key, value in data.items():
                    col_name = str(key)
                    if isinstance(value, (list, tuple)):
                        col = list(value)
                    else:
                        col = [value]
                    if width is None:
                        width = len(col)
                    elif len(col) != width:
                        raise ValueError("all DataFrame columns must have the same length")
                    normalized[col_name] = col
                self._data = normalized
            elif data is None:
                self._data = {}
            else:
                raise TypeError(f"unsupported wasm polars DataFrame input: {type(data)!r}")
            self.args = args
            self.kwargs = kwargs

        @property
        def columns(self):
            return list(self._data.keys())

        @property
        def shape(self):
            cols = self.columns
            if not cols:
                return (0, 0)
            return (len(self._data[cols[0]]), len(cols))

        @property
        def height(self):
            return self.shape[0]

        @property
        def width(self):
            return self.shape[1]

        def head(self, n=5):
            try:
                take = max(0, int(n))
            except Exception:
                take = 5
            return DataFrame({k: v[:take] for k, v in self._data.items()})

        def select(self, *names):
            cols = []
            for item in names:
                if isinstance(item, (list, tuple)):
                    cols.extend([str(x) for x in item])
                else:
                    cols.append(str(item))
            if not cols:
                return DataFrame(self)
            return DataFrame({k: list(self._data.get(k, [])) for k in cols})

        def to_dict(self, as_series=False):
            return {k: list(v) for k, v in self._data.items()}

        def to_dicts(self):
            rows, _ = self.shape
            out = []
            cols = self.columns
            for i in range(rows):
                out.append({k: self._data[k][i] for k in cols})
            return out

        def __len__(self):
            return self.shape[0]

        def __repr__(self):
            return f"DataFrame(shape={self.shape}, columns={self.columns})"

    DataFrame.__module__ = "polars.dataframe.frame"

    pm.Expr = _DummyExpr
    pm.DataFrame = DataFrame

    def _pl_col(name):
        if isinstance(name, str):
            return _DummyExpr({"Column": name})
        return _DummyExpr()

    def _pl_nth(idx):
        try:
            idx_i = int(idx)
        except Exception:
            idx_i = 0
        return _DummyExpr({"Selector": {"ByIndex": {"indices": [idx_i], "strict": True}}})

    def _pl_all():
        return _DummyExpr({"Selector": "Wildcard"})

    def _pl_lit(v):
        if v is None:
            return _DummyExpr({"Literal": {"Scalar": {"Null": "Null"}}})
        if isinstance(v, bool):
            return _DummyExpr({"Literal": {"Scalar": {"Boolean": v}}})
        if isinstance(v, int):
            return _DummyExpr({"Literal": {"Dyn": {"Int": int(v)}}})
        if isinstance(v, float):
            return _DummyExpr({"Literal": {"Dyn": {"Float": float(v)}}})
        if isinstance(v, str):
            return _DummyExpr({"Literal": {"Scalar": {"String": v}}})
        return _DummyExpr()

    pm.col = lambda name=None, *_args, **_kwargs: _pl_col(name)
    pm.nth = lambda idx=0, *_args, **_kwargs: _pl_nth(idx)
    pm.all = lambda *_args, **_kwargs: _pl_all()
    pm.lit = lambda v=None, *_args, **_kwargs: _pl_lit(v)
    for _dt in ["UInt32","UInt64","Int32","Int64","Float32","Float64","Boolean","String","Date","Time","Datetime"]:
        setattr(pm, _dt, _dt)
    pm_dataframe = types.ModuleType("polars.dataframe")
    pm_dataframe_frame = types.ModuleType("polars.dataframe.frame")
    pm_dataframe_frame.DataFrame = DataFrame
    pm_dataframe.frame = pm_dataframe_frame
    sys.modules["polars.dataframe"] = pm_dataframe
    sys.modules["polars.dataframe.frame"] = pm_dataframe_frame
    sys.modules["polars"] = pm


def _patch_qust_files_for_wasm():
    import sys
    import pathlib

    def _safe_patch_file(path: pathlib.Path, replacers: list[tuple[str, str]]):
        try:
            if not path.exists():
                return
            old = path.read_text(encoding="utf-8")
            new = old
            for src, dst in replacers:
                new = new.replace(src, dst)
            if new != old:
                path.write_text(new, encoding="utf-8")
        except Exception:
            # 不让补丁失败阻断启动
            pass

    # 仅修 qust/context.py（容错），不改 qust/__init__.py；
    # wasm 下也需要执行 __set_lib_path 以初始化 ExprPl FFI 配置。
    # 这里不 import qust，避免启动阶段触发副作用。
    for _sp in list(sys.path):
        try:
            base = pathlib.Path(_sp)
        except Exception:
            continue
        pkg_dir = base / "qust"
        if not pkg_dir.exists():
            continue
        _safe_patch_file(
            pkg_dir / "context.py",
            [
                (
                    "def set_lib_path(p):\n    set_lib_path_otters(p)",
                    "def set_lib_path(p):\n    try:\n        set_lib_path_otters(p)\n    except Exception:\n        return None",
                )
            ],
        )
        break


_patch_qust_files_for_wasm()


def _patch_qust_expr_runtime():
    try:
        import qust.expr as _qe
    except Exception:
        return

    _old_col = getattr(_qe, "_col", None)
    if _old_col is None:
        return

    def _col_safe(e):
        # wasm 下优先保持 otters-py 原生 _col 路径，避免把普通列选择强制降级到 ExprPl。
        return _old_col(e)

    _qe._col = _col_safe


def _build_scope() -> dict:
    g = {"__builtins__": __builtins__, "__out__": None}
    exec("import qust as qs", g, g)
    exec("from qust import *", g, g)
    # 显式补齐 UDF 基类，避免 __all__/缓存差异导致 NameError: UdfRow not defined
    try:
        from qust.udf import UdfRow as _UdfRow, UdfBatch as _UdfBatch
        g["UdfRow"] = _UdfRow
        g["UdfBatch"] = _UdfBatch
    except Exception:
        pass
    _patch_qust_expr_runtime()
    if "Monitor" in g:
        for i in range(1, 17):
            g[f"monitor{i}"] = g["Monitor"]()

    def _load_data(path, name=None):
        from qust import qust_core
        import js
        name_s = None if name is None else str(name).strip() or None
        if not isinstance(path, str):
            raise TypeError(f"load_data(path, name=None) 需要路径/URL 字符串，当前收到: {type(path)!r}")
        path_s = str(path)
        try:
            items = json.loads(qust_core.list_loaded_data())
        except Exception:
            items = []
        for item in items:
            if not isinstance(item, dict):
                continue
            item_name = str(item.get("name") or "")
            item_source = str(item.get("source") or "")
            if name_s and item_name == name_s:
                return item
            if path_s.startswith(("http://", "https://")):
                if item_source == f"url:{path_s}":
                    return item
            elif item_name == path_s:
                return item
        if path_s.startswith(("http://", "https://")):
            print("__OTTERS_STATUS__:正在远程拉取数据")
            source = f"url:{path_s}"
        else:
            print("__OTTERS_STATUS__:正在加载数据")
            source = f"url:/{path_s.lstrip('/')}"
        payload = bytes(js.__otters_load_data_sync(path_s))
        display_name = name_s or path_s
        raw = qust_core.upload_parquet_named_bytes_with_source(
            str(display_name),
            source,
            payload,
        )
        return json.loads(raw)

    def _save(obj, file_name: str):
        from qust import qust_core
        core = _extract_core_obj(obj)
        payload = _core_to_expr_bytes(core)
        raw = qust_core.save_wasm(str(file_name), payload)
        return json.loads(raw)

    def _save_data(data, file_name: str):
        from qust import qust_core
        if isinstance(data, dict):
            name = data.get("name")
            if not name:
                raise TypeError("save_data(data, file_name): data dict 缺少 name")
            raw = qust_core.save_loaded_data(str(name), str(file_name))
            return json.loads(raw)
        if isinstance(data, str):
            raw = qust_core.save_loaded_data(str(data), str(file_name))
            return json.loads(raw)
        raise TypeError(f"save_data(data, file_name) 不支持该 data 类型: {type(data)!r}")

    g["load_data"] = _load_data
    g["save"] = _save
    g["save_data"] = _save_data
    try:
        import qust as _qust_mod
        setattr(_qust_mod, "load_data", _load_data)
        setattr(_qust_mod, "save", _save)
        setattr(_qust_mod, "save_data", _save_data)
        _all = list(getattr(_qust_mod, "__all__", []))
        if "load_data" not in _all:
            _all.append("load_data")
        if "save" not in _all:
            _all.append("save")
        if "save_data" not in _all:
            _all.append("save_data")
        _qust_mod.__all__ = _all
    except Exception:
        pass
    return g


def _extract_core_obj(out):
    if isinstance(out, dict):
        return None
    if hasattr(out, "_expr"):
        return out._expr
    if hasattr(out, "_DataFrame__df"):
        core_df = getattr(out, "_DataFrame__df")
        return core_df
    if _is_monitor_session_wrapper(out):
        if bool(getattr(out, "_show_local_requested", False)):
            return getattr(out, "_core_session", None)
        return None
    _t = type(out)
    _mod = str(getattr(_t, "__module__", ""))
    _name = str(getattr(_t, "__name__", ""))
    if _name == "DataFrame" and _mod.startswith("polars"):
        return None
    return out


def _extract_dataset_id(out):
    if isinstance(out, dict):
        try:
            if "dataset_id" in out:
                value = out.get("dataset_id")
                if value is None:
                    return None
                return int(value)
        except Exception:
            return None
    if hasattr(out, "_dataset_id"):
        try:
            value = getattr(out, "_dataset_id")
            if value is None:
                return None
            return int(value)
        except Exception:
            return None
    return None

_LAST_CORE_OBJ = None
_LAST_PREPARED_CALC_TOKEN = None
_LAST_DATASET_ID = None
_LAST_SHOW_LOCAL_REQUESTED = False
_LAST_HIDDEN_MONITOR_SESSION = False


def _is_monitor_session_wrapper(obj) -> bool:
    return (
        hasattr(obj, "_core_session")
        and hasattr(obj, "_show_local_requested")
    )


def _is_monitor_session_core_obj(obj) -> bool:
    return (
        hasattr(obj, "_core_session")
        and hasattr(obj, "_dataset_id")
        and hasattr(obj, "show_local")
        and hasattr(obj, "with_params")
        and hasattr(obj, "with_actions")
        and not hasattr(obj, "_monitor")
        and not hasattr(obj, "_df")
    )


def _format_script_result(obj) -> str:
    if obj is None:
        return ""
    try:
        if isinstance(obj, str):
            return obj
    except Exception:
        pass
    try:
        return str(obj)
    except Exception:
        try:
            return repr(obj)
        except Exception:
            return ""


def _print_script_result(obj):
    text = _format_script_result(obj)
    if text:
        print(text)


def _core_to_expr_bytes(core) -> bytes:
    if not hasattr(core, "__reduce__"):
        raise TypeError("输出对象不支持 __reduce__，无法导出 Expr/DataFrame 字节")
    reduced = core.__reduce__()
    if not isinstance(reduced, tuple) or len(reduced) < 3:
        raise TypeError("__reduce__ 返回值非法，期望 (callable, args, state)")
    state = reduced[2]
    if isinstance(state, (bytes, bytearray)):
        return bytes(state)
    if isinstance(state, memoryview):
        return state.tobytes()
    if isinstance(state, dict) and "_node" in state:
        raise TypeError("检测到模板 qust_core(_node)；当前应加载真实 pyo3 wasm wheel")
    raise TypeError(f"__reduce__ state 类型不支持: {type(state)!r}")


def _iter_core_candidates(g):
    if not isinstance(g, dict):
        return []
    candidates = [g.get("__out__")]
    for name in ("df", "e", "expr", "result", "out", "data", "dataset"):
        candidates.append(g.get(name))
    out = []
    seen = set()
    dataset_id_hint = None
    for cand in candidates:
        if cand is None:
            continue
        if dataset_id_hint is None:
            dataset_id_hint = _extract_dataset_id(cand)
        try:
            core = _extract_core_obj(cand)
        except Exception:
            continue
        if core is None:
            continue
        key = id(core)
        if key in seen:
            continue
        seen.add(key)
        out.append(core)
    return out, dataset_id_hint


def _pick_core_obj_from_scope(g):
    cands = _iter_core_candidates(g)
    return cands[0] if cands else None


def _layout_has_visible_monitor(layout_text):
    try:
        data = json.loads(layout_text)
    except Exception:
        return True
    if not isinstance(data, dict):
        return True
    rows = int(data.get("rows") or 0)
    cols = int(data.get("cols") or 0)
    layout_json = data.get("layout_json")
    if rows == 1 and cols == 1 and layout_json in ("[]", "", None):
        return False
    return True


def _choose_core_obj(candidates, dataset_id_hint):
    if not candidates:
        return None
    from qust import qust_core

    first_layout_ok = None
    for core in candidates:
        layout_ok = False
        try:
            if hasattr(qust_core, "eval_monitor_layout_obj"):
                layout_ok = _layout_has_visible_monitor(qust_core.eval_monitor_layout_obj(core))
            elif hasattr(qust_core, "eval_expr_monitor_layout_obj"):
                layout_ok = _layout_has_visible_monitor(qust_core.eval_expr_monitor_layout_obj(core))
            if first_layout_ok is None:
                if layout_ok:
                    first_layout_ok = core
        except Exception:
            continue
        if not layout_ok:
            continue

    if first_layout_ok is not None:
        return first_layout_ok
    return None


def compile_expr_bytes(code: str) -> bytes:
    global _LAST_CORE_OBJ, _LAST_PREPARED_CALC_TOKEN, _LAST_DATASET_ID
    global _LAST_SHOW_LOCAL_REQUESTED, _LAST_HIDDEN_MONITOR_SESSION
    if not isinstance(code, str):
        raise TypeError("expression script must be str")
    _LAST_CORE_OBJ = None
    _LAST_PREPARED_CALC_TOKEN = None
    _LAST_DATASET_ID = None
    _LAST_SHOW_LOCAL_REQUESTED = False
    _LAST_HIDDEN_MONITOR_SESSION = False
    text = code.replace("\r\n", "\n")
    module = ast.parse(text, mode="exec")
    if module.body and isinstance(module.body[-1], ast.Expr):
        module.body[-1] = ast.Assign(
            targets=[ast.Name(id="__out__", ctx=ast.Store())],
            value=module.body[-1].value,
        )
        ast.fix_missing_locations(module)
    compiled = compile(module, "<otters-pyodide-real-qust>", "exec")
    g = _build_scope()
    exec(compiled, g, g)
    out_obj = g.get("__out__")
    if _is_monitor_session_wrapper(out_obj):
        _LAST_DATASET_ID = _extract_dataset_id(out_obj)
        _LAST_SHOW_LOCAL_REQUESTED = bool(getattr(out_obj, "_show_local_requested", False))
        _LAST_HIDDEN_MONITOR_SESSION = not _LAST_SHOW_LOCAL_REQUESTED
        if _LAST_SHOW_LOCAL_REQUESTED:
            _LAST_CORE_OBJ = getattr(out_obj, "_core_session")
            from qust import qust_core
            if hasattr(qust_core, "eval_monitor_layout_obj"):
                qust_core.eval_monitor_layout_obj(_LAST_CORE_OBJ)
                return b""
            if hasattr(qust_core, "eval_expr_monitor_layout_obj"):
                qust_core.eval_expr_monitor_layout_obj(_LAST_CORE_OBJ)
                return b""
            expr_bytes = _core_to_expr_bytes(_LAST_CORE_OBJ)
            qust_core.eval_expr_monitor_layout_bytes(expr_bytes)
            return expr_bytes
        _print_script_result(out_obj)
        return b""
    if _is_monitor_session_core_obj(out_obj):
        core_session = getattr(out_obj, "_core_session", out_obj)
        _LAST_DATASET_ID = _extract_dataset_id(out_obj)
        if _LAST_DATASET_ID is None:
            try:
                normalized = out_obj.show_local(False, False, "100%", 560)
                if normalized is not None:
                    out_obj = normalized
                core_session = getattr(out_obj, "_core_session", out_obj)
                _LAST_DATASET_ID = _extract_dataset_id(out_obj)
            except Exception:
                pass
        _LAST_SHOW_LOCAL_REQUESTED = True
        _LAST_HIDDEN_MONITOR_SESSION = False
        _LAST_CORE_OBJ = core_session
        from qust import qust_core
        if hasattr(qust_core, "eval_monitor_layout_obj"):
            qust_core.eval_monitor_layout_obj(_LAST_CORE_OBJ)
            return b""
        if hasattr(qust_core, "eval_expr_monitor_layout_obj"):
            qust_core.eval_expr_monitor_layout_obj(_LAST_CORE_OBJ)
            return b""
        expr_bytes = _core_to_expr_bytes(_LAST_CORE_OBJ)
        qust_core.eval_expr_monitor_layout_bytes(expr_bytes)
        return expr_bytes
    candidates = []
    if _LAST_CORE_OBJ is not None:
        candidates.append(_LAST_CORE_OBJ)
    core_candidates, dataset_id_hint = _iter_core_candidates(g)
    candidates.extend(core_candidates)
    dedup = []
    seen = set()
    for c in candidates:
        k = id(c)
        if k in seen:
            continue
        seen.add(k)
        dedup.append(c)
    _LAST_DATASET_ID = dataset_id_hint
    _LAST_CORE_OBJ = _choose_core_obj(dedup, _LAST_DATASET_ID)
    if _LAST_CORE_OBJ is None:
        _print_script_result(out_obj)
        return b""
    from qust import qust_core

    if hasattr(qust_core, "eval_monitor_layout_obj"):
        qust_core.eval_monitor_layout_obj(_LAST_CORE_OBJ)
        return b""
    if hasattr(qust_core, "eval_expr_monitor_layout_obj"):
        qust_core.eval_expr_monitor_layout_obj(_LAST_CORE_OBJ)
        return b""

    expr_bytes = _core_to_expr_bytes(_LAST_CORE_OBJ)
    qust_core.eval_expr_monitor_layout_bytes(expr_bytes)
    return expr_bytes


def last_script_meta() -> str:
    return json.dumps({
        "show_local_requested": bool(_LAST_SHOW_LOCAL_REQUESTED),
        "hidden_monitor_session": bool(_LAST_HIDDEN_MONITOR_SESSION),
        "has_core_obj": _LAST_CORE_OBJ is not None,
        "dataset_id": _LAST_DATASET_ID,
    }, ensure_ascii=False)


def upload_parquet_dataset_bytes(parquet_bytes: bytes) -> str:
    from qust import qust_core
    return qust_core.upload_parquet_dataset_bytes(parquet_bytes)


def upload_parquet_named_bytes(name: str, parquet_bytes: bytes) -> str:
    from qust import qust_core
    return qust_core.upload_parquet_named_bytes(str(name), parquet_bytes)


def list_loaded_data() -> str:
    from qust import qust_core
    return qust_core.list_loaded_data()


def rename_loaded_data(old_name: str, new_name: str) -> str:
    from qust import qust_core
    return qust_core.rename_loaded_data(str(old_name), str(new_name))


def delete_loaded_data(name: str):
    from qust import qust_core
    return qust_core.delete_loaded_data(str(name))


def list_saved_files() -> str:
    from qust import qust_core
    return qust_core.list_saved_files()


def rename_saved_file(old_name: str, new_name: str) -> str:
    from qust import qust_core
    return qust_core.rename_saved_file(str(old_name), str(new_name))


def delete_saved_file(file_name: str):
    from qust import qust_core
    return qust_core.delete_saved_file(str(file_name))


def get_saved_file_bytes(file_name: str):
    from qust import qust_core
    return qust_core.get_saved_file_bytes(str(file_name))


def _has_expr_bytes(expr_bytes) -> bool:
    try:
        return expr_bytes is not None and len(expr_bytes) > 0
    except Exception:
        return False


def eval_expr_monitor_layout_bytes(expr_bytes: bytes) -> str:
    from qust import qust_core
    global _LAST_CORE_OBJ
    if _LAST_CORE_OBJ is not None and hasattr(qust_core, "eval_monitor_layout_obj"):
        return qust_core.eval_monitor_layout_obj(_LAST_CORE_OBJ)
    if _LAST_CORE_OBJ is not None and hasattr(qust_core, "eval_expr_monitor_layout_obj"):
        return qust_core.eval_expr_monitor_layout_obj(_LAST_CORE_OBJ)
    return qust_core.eval_expr_monitor_layout_bytes(expr_bytes)


def eval_expr_params_state_bytes(expr_bytes: bytes, params_pending_json: str) -> str:
    from qust import qust_core
    global _LAST_CORE_OBJ
    if _LAST_CORE_OBJ is not None and hasattr(qust_core, "eval_params_state_obj"):
        try:
            return qust_core.eval_params_state_obj(_LAST_CORE_OBJ, params_pending_json)
        except Exception:
            if not _has_expr_bytes(expr_bytes):
                raise
    if _LAST_CORE_OBJ is not None and hasattr(qust_core, "eval_expr_params_state_obj"):
        try:
            return qust_core.eval_expr_params_state_obj(_LAST_CORE_OBJ, params_pending_json)
        except Exception:
            if not _has_expr_bytes(expr_bytes):
                raise
    return qust_core.eval_expr_params_state_bytes(expr_bytes, params_pending_json)


def eval_expr_to_monitor_packets_dataset_with_params_bytes(
    expr_bytes: bytes,
    dataset_id: int,
    seq: int,
    params_pending_json: str,
):
    from qust import qust_core
    global _LAST_CORE_OBJ, _LAST_DATASET_ID
    dataset_id_eff = int(_LAST_DATASET_ID if _LAST_DATASET_ID is not None else dataset_id)
    if hasattr(qust_core, "eval_to_monitor_packets_dataset_with_params_obj"):
        if _LAST_CORE_OBJ is None and _has_expr_bytes(expr_bytes):
            try:
                from qust.qust_core import Expr as _CoreExpr
                _LAST_CORE_OBJ = _CoreExpr._from_bytes(expr_bytes)
            except Exception:
                pass
        if _LAST_CORE_OBJ is None:
            raise RuntimeError("wasm obj 通道未初始化：缺少可执行 Expr/DataFrame 对象")
        return qust_core.eval_to_monitor_packets_dataset_with_params_obj(
            _LAST_CORE_OBJ,
            dataset_id_eff,
            seq,
            params_pending_json,
        )
    if hasattr(qust_core, "eval_expr_to_monitor_packets_dataset_with_params_obj"):
        if _LAST_CORE_OBJ is None and _has_expr_bytes(expr_bytes):
            try:
                from qust.qust_core import Expr as _CoreExpr
                _LAST_CORE_OBJ = _CoreExpr._from_bytes(expr_bytes)
            except Exception:
                pass
        if _LAST_CORE_OBJ is None:
            raise RuntimeError("wasm obj 通道未初始化：缺少可执行 Expr 对象")
        return qust_core.eval_expr_to_monitor_packets_dataset_with_params_obj(
            _LAST_CORE_OBJ,
            dataset_id_eff,
            seq,
            params_pending_json,
        )
    return qust_core.eval_expr_to_monitor_packets_dataset_with_params_bytes(
        expr_bytes,
        dataset_id_eff,
        seq,
        params_pending_json,
    )


def eval_expr_calc_summary_dataset_with_params_bytes(
    expr_bytes: bytes,
    dataset_id: int,
    params_pending_json: str,
) -> str:
    from qust import qust_core
    global _LAST_CORE_OBJ, _LAST_DATASET_ID
    dataset_id_eff = int(_LAST_DATASET_ID if _LAST_DATASET_ID is not None else dataset_id)
    if _LAST_CORE_OBJ is not None and hasattr(qust_core, "eval_expr_calc_summary_dataset_with_params_obj"):
        return qust_core.eval_expr_calc_summary_dataset_with_params_obj(
            _LAST_CORE_OBJ,
            dataset_id_eff,
            params_pending_json,
        )
    return qust_core.eval_expr_calc_summary_dataset_with_params_bytes(
        expr_bytes,
        dataset_id_eff,
        params_pending_json,
    )


def eval_expr_calc_only_dataset_with_params_bytes(
    expr_bytes: bytes,
    dataset_id: int,
    params_pending_json: str,
) -> int:
    from qust import qust_core
    global _LAST_CORE_OBJ, _LAST_DATASET_ID
    dataset_id_eff = int(_LAST_DATASET_ID if _LAST_DATASET_ID is not None else dataset_id)
    if _LAST_CORE_OBJ is not None and hasattr(qust_core, "eval_expr_calc_only_dataset_with_params_obj"):
        return qust_core.eval_expr_calc_only_dataset_with_params_obj(
            _LAST_CORE_OBJ,
            dataset_id_eff,
            params_pending_json,
        )
    return qust_core.eval_expr_calc_only_dataset_with_params_bytes(
        expr_bytes,
        dataset_id_eff,
        params_pending_json,
    )


def prepare_expr_calc_only_obj(dataset_id: int) -> int:
    from qust import qust_core
    global _LAST_CORE_OBJ, _LAST_PREPARED_CALC_TOKEN
    if _LAST_CORE_OBJ is None:
        raise RuntimeError("prepare calc failed: missing core expr obj")
    if not hasattr(qust_core, "prepare_expr_calc_only_obj"):
        raise RuntimeError("qust_core missing API: prepare_expr_calc_only_obj")
    token = int(qust_core.prepare_expr_calc_only_obj(_LAST_CORE_OBJ, int(dataset_id)))
    _LAST_PREPARED_CALC_TOKEN = token
    return token


def eval_prepared_calc_only_token(token: int, params_pending_json: str) -> int:
    from qust import qust_core
    return int(qust_core.eval_prepared_calc_only_token(int(token), params_pending_json))


def release_prepared_calc_only_token(token: int) -> int:
    from qust import qust_core
    if hasattr(qust_core, "release_prepared_calc_only_token"):
        qust_core.release_prepared_calc_only_token(int(token))
    return 0


_bootstrap_mod = types.ModuleType("qust_wasm_bootstrap")
_bootstrap_mod.compile_expr_bytes = compile_expr_bytes
_bootstrap_mod.upload_parquet_dataset_bytes = upload_parquet_dataset_bytes
_bootstrap_mod.upload_parquet_named_bytes = upload_parquet_named_bytes
_bootstrap_mod.list_loaded_data = list_loaded_data
_bootstrap_mod.rename_loaded_data = rename_loaded_data
_bootstrap_mod.delete_loaded_data = delete_loaded_data
_bootstrap_mod.list_saved_files = list_saved_files
_bootstrap_mod.rename_saved_file = rename_saved_file
_bootstrap_mod.delete_saved_file = delete_saved_file
_bootstrap_mod.get_saved_file_bytes = get_saved_file_bytes
_bootstrap_mod.eval_expr_monitor_layout_bytes = eval_expr_monitor_layout_bytes
_bootstrap_mod.eval_expr_params_state_bytes = eval_expr_params_state_bytes
_bootstrap_mod.eval_expr_to_monitor_packets_dataset_with_params_bytes = eval_expr_to_monitor_packets_dataset_with_params_bytes
_bootstrap_mod.eval_expr_calc_summary_dataset_with_params_bytes = eval_expr_calc_summary_dataset_with_params_bytes
_bootstrap_mod.eval_expr_calc_only_dataset_with_params_bytes = eval_expr_calc_only_dataset_with_params_bytes
_bootstrap_mod.prepare_expr_calc_only_obj = prepare_expr_calc_only_obj
_bootstrap_mod.eval_prepared_calc_only_token = eval_prepared_calc_only_token
_bootstrap_mod.release_prepared_calc_only_token = release_prepared_calc_only_token
_bootstrap_mod.last_script_meta = last_script_meta
sys.modules["qust_wasm_bootstrap"] = _bootstrap_mod

# 避免热刷新后拿到旧模块对象
for _name in [
    "qust.expr",
    "qust.params",
    "qust.dataframe",
    "qust.context",
    "qust.monitor",
    "qust.future",
    "qust.ta",
    "qust.stock",
    "qust.fast_path",
    "qust",
]:
    if _name in sys.modules:
        importlib.reload(sys.modules[_name])
`;

async function installQustRuntime(pyodide) {
  if (_runtimeInstallPromise) return _runtimeInstallPromise;
  _runtimeInstallPromise = (async () => {
    patchDylinkMetadataParser(pyodide);

    // qust_core side module 函数较多，先扩容主模块函数表，避免 dlopen 因 table size 不足失败。
    ensureWasmTableCapacity(pyodide, 220000);

    reportProgress(48, "读取 qust wheel manifest");
    const manifestResp = await fetch(QUST_WHEEL_MANIFEST_URL);
    if (!manifestResp.ok) {
      throw new Error(`fetch wheel manifest failed: ${manifestResp.url} status=${manifestResp.status}`);
    }
    const manifest = await manifestResp.json();
    const wheelName = String(manifest?.wheel || "").trim();
    if (!wheelName.endsWith(".whl")) {
      throw new Error(`invalid wheel manifest: ${JSON.stringify(manifest)}`);
    }
    const wheelUrlObj = new URL(`../wheels/${wheelName}`, import.meta.url);
    const wheelVersion = String(manifest?.version || "").trim();
    if (wheelVersion) {
      wheelUrlObj.searchParams.set("v", wheelVersion);
    }
    const wheelUrl = wheelUrlObj.href;

    reportProgress(55, "加载 micropip");
    await pyodide.loadPackage("micropip");
    pyodide.globals.set("__otters_qust_wheel_url__", wheelUrl);
    try {
      reportProgress(66, "安装 qust wasm wheel");
      await pyodide.runPythonAsync(String.raw`
import micropip

# 兼容 wheel tag 与当前 pyodide emscripten 版本不一致的场景：
# 跳过 micropip 内部 check_compatible 检查（_utils + transaction 两处）。
try:
    import micropip._utils as _mu
    if hasattr(_mu, "check_compatible"):
        _mu.check_compatible = lambda *_args, **_kwargs: None
except Exception:
    pass

try:
    import micropip.transaction as _mt
    if hasattr(_mt, "check_compatible"):
        _mt.check_compatible = lambda *_args, **_kwargs: None
except Exception:
    pass

await micropip.install(__otters_qust_wheel_url__, deps=False, reinstall=True)
await micropip.install("cloudpickle", deps=False)
`);
    } catch (e) {
      try {
        const mod = pyodide?._module || globalThis.Module || self.Module || {};
        const table = mod?.wasmTable || mod?.__indirect_function_table || mod?.asm?.__indirect_function_table || null;
        const tableLen = Number(table?.length ?? 0);
        const memBytes = Number(mod?.wasmMemory?.buffer?.byteLength ?? mod?.HEAP8?.buffer?.byteLength ?? 0);
        const dylinkPatch = Boolean(mod?.__otters_dylink_patch_applied);
        const dylinkFnHead = String(mod?.getDylinkMetadata || "").slice(0, 120);
        const detail = String((e && (e.stack || e.message)) || e);
        throw new Error(`${detail}\n[js_probe] wasm_table_len=${tableLen} mem_bytes=${memBytes} dylink_patch=${dylinkPatch} dylink_fn=${dylinkFnHead}`);
      } catch (probeErr) {
        throw probeErr;
      }
    } finally {
      try {
        pyodide.globals.delete("__otters_qust_wheel_url__");
      } catch (_) {}
    }

    reportProgress(84, "初始化 qust wasm runtime");
    await pyodide.runPythonAsync(INSTALL_BOOTSTRAP_CODE);
    // 启动即验证 qust 包可用，并强校验 obj 通道 API，避免命中旧 wheel/旧扩展缓存。
    reportProgress(92, "校验 qust_core wasm API");
    await pyodide.runPythonAsync(String.raw`
import qust
from qust import col, UdfRow, UdfBatch, qust_core

_need = [
    "eval_expr_monitor_layout_obj",
    "eval_expr_params_state_obj",
    "eval_expr_to_monitor_packets_dataset_with_params_obj",
]
_miss = [x for x in _need if not hasattr(qust_core, x)]
if _miss:
    raise RuntimeError("qust_core 缺少 wasm obj API: " + ", ".join(_miss))
print("qust_core wasm obj API check: ok")
`);
  })();
  return _runtimeInstallPromise;
}

async function ensurePyodide(cdnBase) {
  if (_pyodidePromise) return _pyodidePromise;
  _pyodidePromise = (async () => {
    await ensurePyodideScript(cdnBase);
    reportProgress(22, "初始化 Pyodide");
    const pyodide = await globalThis.loadPyodide({
      indexURL: cdnBase,
      stdout: (line) => pushRuntimeLog("stdout", line),
      stderr: (line) => pushRuntimeLog("stderr", line),
    });
    reportProgress(45, "Pyodide 已就绪");
    await installQustRuntime(pyodide);
    reportProgress(96, "Python runtime 已就绪");
    return pyodide;
  })();
  return _pyodidePromise;
}

export function createPyodideRuntime(opts = {}) {
  const cdnBase = opts.cdnBase || DEFAULT_PYODIDE_CDN;
  if (typeof opts.onProgress === "function") {
    _progressReporter = opts.onProgress;
  }
  let pyodide = null;

  async function init() {
    if (pyodide) return pyodide;
    pyodide = await ensurePyodide(cdnBase);
    return pyodide;
  }

  function ensureReady() {
    if (!pyodide) {
      throw new Error("Pyodide runtime not initialized");
    }
    return pyodide;
  }

  function toUint8(input) {
    if (input instanceof Uint8Array) return input;
    if (input instanceof ArrayBuffer) return new Uint8Array(input);
    if (ArrayBuffer.isView(input)) {
      return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
    }
    if (Array.isArray(input)) return Uint8Array.from(input);
    return new Uint8Array(input ?? []);
  }

  function toTextResult(res) {
    if (typeof res === "string") return res;
    if (res && typeof res.toJs === "function") {
      return String(res.toJs({ create_proxies: false }) ?? "");
    }
    return String(res ?? "");
  }

  function toPacketList(res) {
    const finalize = (arr) =>
      (arr ?? []).map((x) => {
        if (x instanceof Uint8Array) return new Uint8Array(x);
        if (x instanceof ArrayBuffer) return new Uint8Array(x);
        if (ArrayBuffer.isView(x)) return new Uint8Array(x.buffer, x.byteOffset, x.byteLength);
        if (Array.isArray(x)) return Uint8Array.from(x);
        if (typeof x === "string") {
          const enc = new TextEncoder();
          return enc.encode(x);
        }
        return new Uint8Array(0);
      });

    if (Array.isArray(res)) {
      return finalize(res);
    }
    if (res && typeof res.toJs === "function") {
      const jsVal = res.toJs({ create_proxies: false });
      return finalize(jsVal);
    }
    return [];
  }

  function runPyText(code, vars = {}) {
    const py = ensureReady();
    let res = null;
    const keys = Object.keys(vars);
    for (const k of keys) {
      py.globals.set(k, vars[k]);
    }
    try {
      res = py.runPython(code);
      return toTextResult(res);
    } catch (err) {
      pushRuntimeLog("stderr", String((err && (err.stack || err.message)) || err));
      throw err;
    } finally {
      for (const k of keys) {
        try {
          py.globals.delete(k);
        } catch (_) {}
      }
      if (res && typeof res.destroy === "function") {
        try {
          res.destroy();
        } catch (_) {}
      }
    }
  }

  function runPyPackets(code, vars = {}) {
    const py = ensureReady();
    let res = null;
    const keys = Object.keys(vars);
    for (const k of keys) {
      py.globals.set(k, vars[k]);
    }
    try {
      res = py.runPython(code);
      return toPacketList(res);
    } catch (err) {
      pushRuntimeLog("stderr", String((err && (err.stack || err.message)) || err));
      throw err;
    } finally {
      for (const k of keys) {
        try {
          py.globals.delete(k);
        } catch (_) {}
      }
      if (res && typeof res.destroy === "function") {
        try {
          res.destroy();
        } catch (_) {}
      }
    }
  }

  function runPyBytes(code, vars = {}) {
    const py = ensureReady();
    let res = null;
    const keys = Object.keys(vars);
    for (const k of keys) {
      py.globals.set(k, vars[k]);
    }
    try {
      res = py.runPython(code);
      if (res instanceof Uint8Array) return new Uint8Array(res);
      if (res && typeof res.toJs === "function") {
        const jsVal = res.toJs({ create_proxies: false });
        return toUint8(jsVal);
      }
      return toUint8(res);
    } catch (err) {
      pushRuntimeLog("stderr", String((err && (err.stack || err.message)) || err));
      throw err;
    } finally {
      for (const k of keys) {
        try {
          py.globals.delete(k);
        } catch (_) {}
      }
      if (res && typeof res.destroy === "function") {
        try {
          res.destroy();
        } catch (_) {}
      }
    }
  }

  async function compileExpressionBytes(script) {
    const py = await init();
    let res = null;
    const runCompile = async () => {
      const payload = JSON.stringify(String(script ?? ""));
      return py.runPythonAsync(
        `import qust_wasm_bootstrap as _qb
_qb.compile_expr_bytes(${payload})`
      );
    };
    try {
      try {
        res = await runCompile();
      } catch (err) {
        const msg = String((err && (err.stack || err.message)) || err || "");
        if (
          msg.includes("No module named 'qust'")
          || msg.includes("ModuleNotFoundError: No module named 'qust'")
        ) {
          // 运行期兜底：若 qust 模块丢失，自动重装 wheel 后重试一次。
          _runtimeInstallPromise = null;
          await installQustRuntime(py);
          res = await runCompile();
        } else {
          throw err;
        }
      }
      if (res instanceof Uint8Array) {
        return new Uint8Array(res);
      }
      if (res && typeof res.toJs === "function") {
        const jsVal = res.toJs({ create_proxies: false });
        if (jsVal instanceof Uint8Array) {
          return new Uint8Array(jsVal);
        }
        if (Array.isArray(jsVal)) {
          return Uint8Array.from(jsVal);
        }
      }
      if (Array.isArray(res)) {
        return Uint8Array.from(res);
      }
      throw new Error(`compile_expr_bytes 返回类型不支持: ${Object.prototype.toString.call(res)}`);
    } finally {
      if (res && typeof res.destroy === "function") {
        try {
          res.destroy();
        } catch (_) {}
      }
    }
  }

  function uploadParquetDatasetBytes(parquetBytes) {
    const text = runPyText(
      "import qust_wasm_bootstrap as _qb\n_qb.upload_parquet_dataset_bytes(bytes(__otters_parquet_bytes))",
      { __otters_parquet_bytes: toUint8(parquetBytes) }
    );
    return JSON.parse(text || "{}");
  }

  function uploadParquetNamedBytes(name, parquetBytes) {
    const text = runPyText(
      "import qust_wasm_bootstrap as _qb\n_qb.upload_parquet_named_bytes(str(__otters_file_name), bytes(__otters_parquet_bytes))",
      {
        __otters_file_name: String(name ?? ""),
        __otters_parquet_bytes: toUint8(parquetBytes),
      }
    );
    return JSON.parse(text || "{}");
  }

  function listLoadedData() {
    const text = runPyText(
      "import qust_wasm_bootstrap as _qb\n_qb.list_loaded_data()",
      {}
    );
    return JSON.parse(text || "[]");
  }

  function listCodeExamples() {
    if (_codeExamplesCache && typeof _codeExamplesCache === "object") {
      return _codeExamplesCache;
    }
    const tomlText = loadTextSync(`${CODE_EXAMPLES_TOML_URL}?t=${Date.now()}`);
    const text = runPyText(
      [
        "from qust import qust_core",
        "qust_core.parse_code_examples_toml(str(__otters_code_examples_toml))",
      ].join("\n"),
      { __otters_code_examples_toml: tomlText }
    );
    _codeExamplesCache = JSON.parse(text || "{}");
    return _codeExamplesCache;
  }

  function renameLoadedData(oldName, newName) {
    const text = runPyText(
      "import qust_wasm_bootstrap as _qb\n_qb.rename_loaded_data(str(__otters_old_name), str(__otters_new_name))",
      {
        __otters_old_name: String(oldName ?? ""),
        __otters_new_name: String(newName ?? ""),
      }
    );
    return JSON.parse(text || "{}");
  }

  function deleteLoadedData(name) {
    runPyText(
      "import qust_wasm_bootstrap as _qb\n_qb.delete_loaded_data(str(__otters_name))",
      { __otters_name: String(name ?? "") }
    );
    return 0;
  }

  function listSavedFiles() {
    const text = runPyText(
      "import qust_wasm_bootstrap as _qb\n_qb.list_saved_files()",
      {}
    );
    return JSON.parse(text || "[]");
  }

  function renameSavedFile(oldName, newName) {
    const text = runPyText(
      "import qust_wasm_bootstrap as _qb\n_qb.rename_saved_file(str(__otters_old_name), str(__otters_new_name))",
      {
        __otters_old_name: String(oldName ?? ""),
        __otters_new_name: String(newName ?? ""),
      }
    );
    return JSON.parse(text || "{}");
  }

  function deleteSavedFile(fileName) {
    runPyText(
      "import qust_wasm_bootstrap as _qb\n_qb.delete_saved_file(str(__otters_file_name))",
      { __otters_file_name: String(fileName ?? "") }
    );
    return 0;
  }

  function getSavedFileBytes(fileName) {
    return runPyBytes(
      "import qust_wasm_bootstrap as _qb\n_qb.get_saved_file_bytes(str(__otters_file_name))",
      { __otters_file_name: String(fileName ?? "") }
    );
  }

  function evalExprMonitorLayoutBytes(exprBytes) {
    return runPyText(
      "import qust_wasm_bootstrap as _qb\n_qb.eval_expr_monitor_layout_bytes(bytes(__otters_expr_bytes))",
      { __otters_expr_bytes: toUint8(exprBytes) }
    );
  }

  function evalExprParamsStateBytes(exprBytes, paramsPendingJson) {
    return runPyText(
      "import qust_wasm_bootstrap as _qb\n_qb.eval_expr_params_state_bytes(bytes(__otters_expr_bytes), __otters_params_pending_json)",
      {
        __otters_expr_bytes: toUint8(exprBytes),
        __otters_params_pending_json: String(paramsPendingJson ?? "{}"),
      }
    );
  }

  function evalExprToMonitorPacketsDatasetWithParamsBytes(
    exprBytes,
    datasetId,
    seq,
    paramsPendingJson
  ) {
    return runPyPackets(
      "import qust_wasm_bootstrap as _qb\n_qb.eval_expr_to_monitor_packets_dataset_with_params_bytes(bytes(__otters_expr_bytes), int(__otters_dataset_id), int(__otters_seq), __otters_params_pending_json)",
      {
        __otters_expr_bytes: toUint8(exprBytes),
        __otters_dataset_id: Number(datasetId ?? 0),
        __otters_seq: Number(seq ?? 0),
        __otters_params_pending_json: String(paramsPendingJson ?? "{}"),
      }
    );
  }

  function evalExprCalcSummaryDatasetWithParamsBytes(
    exprBytes,
    datasetId,
    paramsPendingJson
  ) {
    return runPyText(
      "import qust_wasm_bootstrap as _qb\n_qb.eval_expr_calc_summary_dataset_with_params_bytes(bytes(__otters_expr_bytes), int(__otters_dataset_id), __otters_params_pending_json)",
      {
        __otters_expr_bytes: toUint8(exprBytes),
        __otters_dataset_id: Number(datasetId ?? 0),
        __otters_params_pending_json: String(paramsPendingJson ?? "{}"),
      }
    );
  }

  function evalExprCalcOnlyDatasetWithParamsBytes(
    exprBytes,
    datasetId,
    paramsPendingJson
  ) {
    const text = runPyText(
      "import qust_wasm_bootstrap as _qb\n_qb.eval_expr_calc_only_dataset_with_params_bytes(bytes(__otters_expr_bytes), int(__otters_dataset_id), __otters_params_pending_json)",
      {
        __otters_expr_bytes: toUint8(exprBytes),
        __otters_dataset_id: Number(datasetId ?? 0),
        __otters_params_pending_json: String(paramsPendingJson ?? "{}"),
      }
    );
    const n = Number(text);
    return Number.isFinite(n) ? n : 0;
  }

  function prepareExprCalcOnlyObj(datasetId) {
    const text = runPyText(
      "import qust_wasm_bootstrap as _qb\n_qb.prepare_expr_calc_only_obj(int(__otters_dataset_id))",
      { __otters_dataset_id: Number(datasetId ?? 0) }
    );
    const token = Number(text);
    return Number.isFinite(token) ? token : 0;
  }

  function evalPreparedCalcOnlyToken(token, paramsPendingJson) {
    const text = runPyText(
      "import qust_wasm_bootstrap as _qb\n_qb.eval_prepared_calc_only_token(int(__otters_token), __otters_params_pending_json)",
      {
        __otters_token: Number(token ?? 0),
        __otters_params_pending_json: String(paramsPendingJson ?? "{}"),
      }
    );
    const n = Number(text);
    return Number.isFinite(n) ? n : 0;
  }

  function releasePreparedCalcOnlyToken(token) {
    runPyText(
      "import qust_wasm_bootstrap as _qb\n_qb.release_prepared_calc_only_token(int(__otters_token))",
      { __otters_token: Number(token ?? 0) }
    );
    return 0;
  }

  function lastScriptMeta() {
    const text = runPyText(
      "import qust_wasm_bootstrap as _qb\n_qb.last_script_meta()",
      {}
    );
    return JSON.parse(text || "{}");
  }

  function buildMonitorQueryResultFromSelectionPayload({
    code,
    selectionPayloadBytes,
    theme,
  }) {
    const text = runPyText(
      [
        "import json",
        "from qust import qust_core",
        "json.dumps(",
        "    qust_core.build_monitor_query_result_from_selection_payload(",
        "        str(__otters_query_code),",
        "        selection_payload_bytes=(None if (__otters_selection_payload_bytes is None or type(__otters_selection_payload_bytes).__name__ == 'JsNull') else bytes(__otters_selection_payload_bytes)),",
        "        theme=str(__otters_theme),",
        "    ),",
        "    ensure_ascii=False,",
        ")",
      ].join("\n"),
      {
        __otters_query_code: String(code ?? ""),
        __otters_selection_payload_bytes:
          selectionPayloadBytes == null ? null : toUint8(selectionPayloadBytes),
        __otters_theme: String(theme ?? "dark"),
      }
    );
    return JSON.parse(text || "{}");
  }

  function buildMonitorQueryResultFromScatterSelectRequest({
    code,
    scatterSelectRequestBytes,
    theme,
  }) {
    const text = runPyText(
      [
        "import json",
        "from qust import qust_core",
        "json.dumps(",
        "    qust_core.build_monitor_query_result_from_scatter_select_request(",
        "        str(__otters_query_code),",
        "        scatter_select_request_bytes=(None if (__otters_scatter_select_request_bytes is None or type(__otters_scatter_select_request_bytes).__name__ == 'JsNull') else bytes(__otters_scatter_select_request_bytes)),",
        "        theme=str(__otters_theme),",
        "    ),",
        "    ensure_ascii=False,",
        ")",
      ].join("\n"),
      {
        __otters_query_code: String(code ?? ""),
        __otters_scatter_select_request_bytes:
          scatterSelectRequestBytes == null ? null : toUint8(scatterSelectRequestBytes),
        __otters_theme: String(theme ?? "dark"),
      }
    );
    return JSON.parse(text || "{}");
  }


  return {
    init,
    drainLogs: drainRuntimeLogs,
    compileExpressionBytes,
    uploadParquetDatasetBytes,
    uploadParquetNamedBytes,
    listLoadedData,
    listCodeExamples,
    renameLoadedData,
    deleteLoadedData,
    listSavedFiles,
    renameSavedFile,
    deleteSavedFile,
    getSavedFileBytes,
    evalExprMonitorLayoutBytes,
    evalExprParamsStateBytes,
    evalExprToMonitorPacketsDatasetWithParamsBytes,
    evalExprCalcSummaryDatasetWithParamsBytes,
    evalExprCalcOnlyDatasetWithParamsBytes,
    prepareExprCalcOnlyObj,
    evalPreparedCalcOnlyToken,
    releasePreparedCalcOnlyToken,
    lastScriptMeta,
    buildMonitorQueryResultFromSelectionPayload,
    buildMonitorQueryResultFromScatterSelectRequest,
  };
}
