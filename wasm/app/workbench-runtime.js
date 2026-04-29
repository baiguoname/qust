import {
  createCodeEntry,
  deleteParquetSnapshotIfName,
  loadCodeEntries,
  loadParquetSnapshots,
  renameParquetSnapshot,
  saveCodeEntries,
  saveParquetSnapshot,
  touchCodeEntry,
} from './workspace-storage.js?v=20260416_202347';
import {
  createFileItemRow,
  formatBytes,
  makeActionButton,
  promptName,
  renderItemPanel,
} from './workbench-panels.js?v=20260416_202347';

const EXPR_FONT_SIZE_KEY = 'otters_wasm_expr_font_size_v1';
const EDITOR_VIM_STORAGE_KEY = 'otters_wasm_editor_vim_v1';
const EXPR_FONT_MIN = 14;
const EXPR_FONT_MAX = 32;
const EXPR_FONT_STEP = 1;
const EDITOR_WINDOWS_STATE_KEY = 'otters_wasm_editor_windows_v1';
const EDITOR_TAB_WIDTH = 4;
const EDITOR_TAB_SPACES = '    ';
const CODE_EXAMPLES_EMPTY_LABEL = '例子';

function loadSavedEditorFontSize() {
  try {
    const raw = Number(localStorage.getItem(EXPR_FONT_SIZE_KEY));
    if (Number.isFinite(raw)) {
      return Math.min(EXPR_FONT_MAX, Math.max(EXPR_FONT_MIN, Math.round(raw)));
    }
  } catch (_) {}
  return 20;
}

function setSharedEditorFontSize(px) {
  const next = Math.min(EXPR_FONT_MAX, Math.max(EXPR_FONT_MIN, Math.round(Number(px) || 20)));
  document.documentElement.style.setProperty('--editor-font-size', `${next}px`);
  try {
    localStorage.setItem(EXPR_FONT_SIZE_KEY, String(next));
  } catch (_) {}
  return next;
}

function saveEditorVimStateLocal(enabled) {
  try {
    localStorage.setItem(EDITOR_VIM_STORAGE_KEY, enabled ? '1' : '0');
  } catch (_) {}
}

function loadEditorWindowsState() {
  try {
    const raw = localStorage.getItem(EDITOR_WINDOWS_STATE_KEY);
    if (!raw) return { openIds: [], rects: {}, settings: {} };
    const parsed = JSON.parse(raw);
    const openIds = Array.isArray(parsed?.openIds) ? parsed.openIds.map((x) => String(x)) : [];
    const rects = parsed?.rects && typeof parsed.rects === 'object' ? parsed.rects : {};
    const settings = parsed?.settings && typeof parsed.settings === 'object' ? parsed.settings : {};
    return { openIds, rects, settings };
  } catch (_) {
    return { openIds: [], rects: {}, settings: {} };
  }
}

function saveEditorWindowsState(openIds, rects, settings) {
  try {
    localStorage.setItem(EDITOR_WINDOWS_STATE_KEY, JSON.stringify({
      openIds: Array.from(openIds || []).map((x) => String(x)),
      rects: rects && typeof rects === 'object' ? rects : {},
      settings: settings && typeof settings === 'object' ? settings : {},
    }));
  } catch (_) {}
}

function htmlEscape(text) {
  return String(text ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;');
}

function highlightPython(text) {
  const src = String(text ?? '');
  if (src.length > 8000) return htmlEscape(src);
  const kw = new Set(['and', 'or', 'not', 'True', 'False', 'None', 'if', 'else', 'in', 'is']);
  const fnLike = new Set(['col', 'pl', 'params', 'pms']);
  let i = 0;
  let out = '';
  while (i < src.length) {
    const ch = src[i];
    if (ch === '#') {
      let j = i + 1;
      while (j < src.length && src[j] !== '\n') j += 1;
      out += `<span class="tok-comment">${htmlEscape(src.slice(i, j))}</span>`;
      i = j;
      continue;
    }
    if (ch === '"' || ch === "'") {
      const q = ch;
      let j = i + 1;
      while (j < src.length) {
        if (src[j] === '\\') {
          j += 2;
          continue;
        }
        if (src[j] === q) {
          j += 1;
          break;
        }
        j += 1;
      }
      out += `<span class="tok-string">${htmlEscape(src.slice(i, j))}</span>`;
      i = j;
      continue;
    }
    if (/[0-9]/.test(ch)) {
      let j = i + 1;
      while (j < src.length && /[0-9_]/.test(src[j])) j += 1;
      if (src[j] === '.') {
        j += 1;
        while (j < src.length && /[0-9_]/.test(src[j])) j += 1;
      }
      out += `<span class="tok-number">${htmlEscape(src.slice(i, j))}</span>`;
      i = j;
      continue;
    }
    if (/[A-Za-z_]/.test(ch)) {
      let j = i + 1;
      while (j < src.length && /[A-Za-z0-9_]/.test(src[j])) j += 1;
      const name = src.slice(i, j);
      if (kw.has(name)) out += `<span class="tok-keyword">${name}</span>`;
      else if (fnLike.has(name)) out += `<span class="tok-func">${name}</span>`;
      else out += htmlEscape(name);
      i = j;
      continue;
    }
    out += htmlEscape(ch);
    i += 1;
  }
  return out;
}

function indentSelection(el) {
  const value = el.value || '';
  const start = el.selectionStart ?? 0;
  const end = el.selectionEnd ?? start;
  const lineStart = value.lastIndexOf('\n', Math.max(0, start - 1)) + 1;
  const lineEnd = value.indexOf('\n', end);
  const blockEnd = lineEnd === -1 ? value.length : lineEnd;
  const selected = value.slice(lineStart, blockEnd);
  const indented = selected
    .split('\n')
    .map((line) => `${EDITOR_TAB_SPACES}${line}`)
    .join('\n');
  el.value = value.slice(0, lineStart) + indented + value.slice(blockEnd);
  const isMultiLine = start !== end || selected.includes('\n');
  if (isMultiLine) {
    el.selectionStart = start + EDITOR_TAB_WIDTH;
    el.selectionEnd = end + (selected.split('\n').length * EDITOR_TAB_WIDTH);
  } else {
    const caret = start + EDITOR_TAB_WIDTH;
    el.selectionStart = caret;
    el.selectionEnd = caret;
  }
}

function outdentSelection(el) {
  const value = el.value || '';
  const start = el.selectionStart ?? 0;
  const end = el.selectionEnd ?? start;
  const lineStart = value.lastIndexOf('\n', Math.max(0, start - 1)) + 1;
  const lineEnd = value.indexOf('\n', end);
  const blockEnd = lineEnd === -1 ? value.length : lineEnd;
  const selected = value.slice(lineStart, blockEnd);
  const lines = selected.split('\n');
  let removed = 0;
  const outdented = lines.map((line) => {
    const m = line.match(/^( {1,4}|\t)/);
    if (m) {
      removed += m[0].length;
      return line.slice(m[0].length);
    }
    return line;
  }).join('\n');
  el.value = value.slice(0, lineStart) + outdented + value.slice(blockEnd);
  const firstRemoved = (lines[0].match(/^( {1,4}|\t)/)?.[0].length) ?? 0;
  el.selectionStart = Math.max(lineStart, start - firstRemoved);
  el.selectionEnd = Math.max(el.selectionStart, end - removed);
}

export function createWorkbenchRuntime({
  state,
  evalEngine,
  ensureEngineReady,
  setStatus,
  appendOutputLines,
  summarizePyError,
  getEditorValue,
  setEditorValue,
  editorControls,
  runScriptText,
  scheduleRefresh,
}) {
  function ensurePopupState() {
    if (!state.codeDrafts || typeof state.codeDrafts !== 'object') state.codeDrafts = {};
    if (!Array.isArray(state.openCodeIds)) state.openCodeIds = [];
    if (!state.popupRects || typeof state.popupRects !== 'object') state.popupRects = {};
    if (!state.popupSettings || typeof state.popupSettings !== 'object') state.popupSettings = {};
  }

  function persistPopupState() {
    ensurePopupState();
    saveEditorWindowsState(state.openCodeIds, state.popupRects, state.popupSettings);
  }

  function getSavedPopupRect(entryId) {
    ensurePopupState();
    const rect = state.popupRects[String(entryId)];
    if (!rect || typeof rect !== 'object') return null;
    const left = Number(rect.left);
    const top = Number(rect.top);
    const width = Number(rect.width);
    const height = Number(rect.height);
    if (![left, top, width, height].every((v) => Number.isFinite(v) && v > 0)) return null;
    return { left, top, width, height };
  }

  function savePopupRect(entryId, rect) {
    ensurePopupState();
    state.popupRects[String(entryId)] = {
      left: Number(rect.left),
      top: Number(rect.top),
      width: Number(rect.width),
      height: Number(rect.height),
    };
    persistPopupState();
  }

  function getSavedPopupSettings(entryId) {
    ensurePopupState();
    const raw = state.popupSettings[String(entryId)];
    if (!raw || typeof raw !== 'object') return null;
    const fontSize = Number(raw.fontSize);
    return {
      fontSize: Number.isFinite(fontSize)
        ? Math.min(EXPR_FONT_MAX, Math.max(EXPR_FONT_MIN, Math.round(fontSize)))
        : null,
      vimEnabled: raw.vimEnabled === true,
    };
  }

  function savePopupSettings(entryId, patch) {
    ensurePopupState();
    const key = String(entryId);
    const prev = state.popupSettings[key] && typeof state.popupSettings[key] === 'object'
      ? state.popupSettings[key]
      : {};
    const next = { ...prev, ...patch };
    if (Number.isFinite(Number(next.fontSize))) {
      next.fontSize = Math.min(EXPR_FONT_MAX, Math.max(EXPR_FONT_MIN, Math.round(Number(next.fontSize))));
    } else {
      delete next.fontSize;
    }
    next.vimEnabled = next.vimEnabled === true;
    state.popupSettings[key] = next;
    persistPopupState();
  }

  function persistCodes() {
    saveCodeEntries(state.codeEntries || []);
  }

  async function loadCodeExamples() {
    if (state.codeExamples && typeof state.codeExamples === 'object') {
      return state.codeExamples;
    }
    if (state.codeExamplesPromise) {
      return state.codeExamplesPromise;
    }
    state.codeExamplesPromise = (async () => {
      await ensureEngineReady();
      const items = await evalEngine.listCodeExamples();
      state.codeExamples = items && typeof items === 'object' ? items : {};
      return state.codeExamples;
    })()
      .catch((err) => {
        state.codeExamples = {};
        setStatus(`加载例子失败: ${summarizePyError(err)}`, true);
        appendOutputLines([`[stderr] 加载例子失败: ${String(err && (err.stack || err.message) || err)}`]);
        return {};
      })
      .finally(() => {
        state.codeExamplesPromise = null;
      });
    return state.codeExamplesPromise;
  }

  function activeCode() {
    return Array.from(state.codeEntries || []).find((item) => item.id === state.activeCodeId) || null;
  }

  function markCodeActive(codeId) {
    state.activeCodeId = codeId == null ? null : String(codeId);
    renderCodePool();
  }

  function ensureCodeDraft(entry) {
    ensurePopupState();
    if (!entry) return '';
    const key = String(entry.id);
    if (!(key in state.codeDrafts)) {
      state.codeDrafts[key] = String(entry.script || '');
    }
    return state.codeDrafts[key];
  }

  function getCodeDraft(entryId) {
    const entry = Array.from(state.codeEntries || []).find((item) => item.id === String(entryId));
    if (!entry) return '';
    return ensureCodeDraft(entry);
  }

  function setCodeDraft(entryId, script) {
    ensurePopupState();
    state.codeDrafts[String(entryId)] = String(script ?? '');
  }

  function isCodeOpen(entryId) {
    ensurePopupState();
    return state.openCodeIds.includes(String(entryId));
  }

  function getEditorWindowsLayer() {
    let layer = document.getElementById('editor_windows');
    if (!layer) {
      layer = document.createElement('div');
      layer.id = 'editor_windows';
      layer.style.position = 'fixed';
      layer.style.inset = '0';
      layer.style.zIndex = '48';
      layer.style.pointerEvents = 'none';
      document.body.appendChild(layer);
    }
    return layer;
  }

  function getEditorWindow(entryId) {
    return document.querySelector(`.editor-window[data-code-window-id="${String(entryId)}"]`);
  }

  function bringWindowToFront(entryId) {
    const win = getEditorWindow(entryId);
    if (!win) return;
    const all = Array.from(document.querySelectorAll('.editor-window'));
    let maxZ = 60;
    for (const item of all) {
      const z = Number(item.style.zIndex || 60);
      if (Number.isFinite(z)) maxZ = Math.max(maxZ, z);
    }
    win.style.zIndex = String(maxZ + 1);
  }

  function focusCodeEditor(entryId) {
    const textarea = document.querySelector(`.editor-window[data-code-window-id="${String(entryId)}"] .editor-window-script`);
    if (textarea) {
      bringWindowToFront(entryId);
      textarea.focus();
    }
  }

  function syncWindowTitle(entryId, title) {
    const win = getEditorWindow(entryId);
    const titleEl = win?.querySelector('.editor-window-title-text');
    if (titleEl) titleEl.textContent = String(title || '编辑代码');
  }

  function syncWindowDraft(entryId) {
    const win = getEditorWindow(entryId);
    const textarea = win?.querySelector('.editor-window-script');
    if (textarea && document.activeElement !== textarea) {
      textarea.value = getCodeDraft(entryId);
    }
  }

  function closeCodeEditor(entryId) {
    ensurePopupState();
    const id = String(entryId);
    state.openCodeIds = state.openCodeIds.filter((item) => item !== id);
    const win = getEditorWindow(id);
    if (win) win.remove();
    if (state.activeCodeId === id) {
      state.activeCodeId = state.openCodeIds.at(-1) || null;
    }
    persistPopupState();
    renderCodePool();
  }

  function createEditorWindow(entry) {
    const entryId = String(entry.id);
    const layer = getEditorWindowsLayer();
    const win = document.createElement('div');
    win.className = 'editor-dialog editor-window';
    win.dataset.codeWindowId = entryId;
    win.style.pointerEvents = 'auto';
    const paneOffset = Math.max(460, Math.floor(window.innerWidth * 0.34));
    const savedRect = getSavedPopupRect(entryId);
    if (savedRect) {
      win.style.left = `${savedRect.left}px`;
      win.style.top = `${savedRect.top}px`;
      win.style.width = `${savedRect.width}px`;
      win.style.height = `${savedRect.height}px`;
    } else {
      win.style.left = `${paneOffset + (state.openCodeIds.length * 36)}px`;
      win.style.top = `${90 + (state.openCodeIds.length * 24)}px`;
      win.style.width = '72vw';
      win.style.height = '68vh';
    }

    const head = document.createElement('div');
    head.className = 'editor-head';

    const titleWrap = document.createElement('div');
    titleWrap.className = 'editor-title';
    const dot = document.createElement('span');
    dot.className = 'dot';
    dot.setAttribute('aria-hidden', 'true');
    const titleEl = document.createElement('span');
    titleEl.className = 'editor-window-title-text';
    titleEl.textContent = entry.name || '编辑代码';
    titleWrap.append(dot, titleEl);

    const actions = document.createElement('div');
    actions.className = 'editor-actions';
    const exampleSelect = document.createElement('select');
    exampleSelect.className = 'editor-example-select';
    exampleSelect.title = '例子';
    exampleSelect.setAttribute('aria-label', '例子');
    const examplePlaceholder = document.createElement('option');
    examplePlaceholder.value = '';
    examplePlaceholder.textContent = CODE_EXAMPLES_EMPTY_LABEL;
    exampleSelect.appendChild(examplePlaceholder);
    const fontDecBtn = document.createElement('button');
    fontDecBtn.type = 'button';
    fontDecBtn.title = '减小字体';
    fontDecBtn.textContent = '字-';
    const fontIncBtn = document.createElement('button');
    fontIncBtn.type = 'button';
    fontIncBtn.title = '增大字体';
    fontIncBtn.textContent = '字+';
    const vimBtn = document.createElement('button');
    vimBtn.type = 'button';
    vimBtn.title = '切换 Vim 模式';
    vimBtn.textContent = 'Vim 模式：关';
    const runBtn = document.createElement('button');
    runBtn.type = 'button';
    runBtn.textContent = '运行';
    const saveBtn = document.createElement('button');
    saveBtn.type = 'button';
    saveBtn.textContent = '保存';
    actions.append(exampleSelect, fontDecBtn, fontIncBtn, vimBtn, runBtn, saveBtn);
    head.append(titleWrap, actions);

    const body = document.createElement('div');
    body.className = 'editor-body';
    const editorShell = document.createElement('div');
    editorShell.className = 'editor';
    editorShell.style.position = 'relative';
    const highlightEl = document.createElement('pre');
    highlightEl.className = 'editor-window-highlight';
    highlightEl.style.margin = '0';
    highlightEl.style.boxSizing = 'border-box';
    highlightEl.style.width = '100%';
    highlightEl.style.minHeight = '220px';
    highlightEl.style.padding = '12px 12px';
    highlightEl.style.fontFamily = '"JetBrains Mono", "Fira Code", "Cascadia Code", "Consolas", monospace';
    highlightEl.style.fontSize = 'var(--editor-font-size)';
    highlightEl.style.fontWeight = 'var(--editor-font-weight)';
    highlightEl.style.lineHeight = '1.6';
    highlightEl.style.whiteSpace = 'pre';
    highlightEl.style.overflowWrap = 'normal';
    highlightEl.style.wordBreak = 'normal';
    highlightEl.style.tabSize = String(EDITOR_TAB_WIDTH);
    highlightEl.style.letterSpacing = '0.005em';
    highlightEl.style.fontVariantLigatures = 'contextual';
    highlightEl.style.textRendering = 'geometricPrecision';
    highlightEl.style.position = 'absolute';
    highlightEl.style.inset = '0';
    highlightEl.style.zIndex = '1';
    highlightEl.style.color = '#d4d4d4';
    highlightEl.style.pointerEvents = 'none';
    highlightEl.style.overflow = 'auto';
    highlightEl.style.scrollbarWidth = 'none';
    const cmHost = document.createElement('div');
    cmHost.className = 'editor-window-cm-host';
    cmHost.style.position = 'absolute';
    cmHost.style.inset = '0';
    cmHost.style.zIndex = '3';
    cmHost.style.display = 'none';
    cmHost.style.overflow = 'hidden';
    const textarea = document.createElement('textarea');
    textarea.className = 'editor-window-script';
    textarea.spellcheck = false;
    textarea.wrap = 'off';
    textarea.value = getCodeDraft(entryId);
    textarea.style.width = '100%';
    textarea.style.height = '100%';
    textarea.style.minHeight = '220px';
    textarea.style.padding = '12px 12px';
    textarea.style.boxSizing = 'border-box';
    textarea.style.border = 'none';
    textarea.style.resize = 'none';
    textarea.style.background = 'transparent';
    textarea.style.color = '#e8eeff';
    textarea.style.caretColor = '#f8f8f2';
    textarea.style.outline = 'none';
    textarea.style.overflow = 'auto';
    textarea.style.fontFamily = '"JetBrains Mono", "Fira Code", "Cascadia Code", "Consolas", monospace';
    textarea.style.fontSize = 'var(--editor-font-size)';
    textarea.style.fontWeight = 'var(--editor-font-weight)';
    textarea.style.lineHeight = '1.6';
    textarea.style.whiteSpace = 'pre';
    textarea.style.overflowWrap = 'normal';
    textarea.style.wordBreak = 'normal';
    textarea.style.tabSize = String(EDITOR_TAB_WIDTH);
    textarea.style.letterSpacing = '0.005em';
    textarea.style.fontVariantLigatures = 'contextual';
    textarea.style.textRendering = 'geometricPrecision';
    textarea.style.position = 'absolute';
    textarea.style.inset = '0';
    textarea.style.zIndex = '2';
    textarea.style.color = 'transparent';
    textarea.style.background = 'transparent';
    textarea.style.caretColor = '#f8f8f2';
    textarea.addEventListener('focus', () => {
      state.activeCodeId = entryId;
      bringWindowToFront(entryId);
      renderCodePool();
    });
    const savedSettings = getSavedPopupSettings(entryId);
    const popupState = {
      vimEnabled: savedSettings?.vimEnabled === true,
      fontSize: savedSettings?.fontSize ?? loadSavedEditorFontSize(),
      cmReady: false,
      cmView: null,
      cmApi: null,
      hlTimer: null,
      hlRafPending: false,
    };

    const applyWindowFontSize = (nextPx) => {
      popupState.fontSize = Math.min(EXPR_FONT_MAX, Math.max(EXPR_FONT_MIN, Math.round(Number(nextPx) || loadSavedEditorFontSize())));
      win.style.setProperty('--editor-font-size', `${popupState.fontSize}px`);
      info.textContent = `字体 ${popupState.fontSize}px`;
      savePopupSettings(entryId, { fontSize: popupState.fontSize, vimEnabled: popupState.vimEnabled });
      return popupState.fontSize;
    };

    const syncHighlight = () => {
      if (popupState.vimEnabled) return;
      highlightEl.innerHTML = highlightPython(textarea.value || '') + '\n';
      highlightEl.scrollTop = textarea.scrollTop;
      highlightEl.scrollLeft = textarea.scrollLeft;
    };
    const scheduleHighlight = (delayMs = 0) => {
      if (popupState.vimEnabled) return;
      if (delayMs > 0) {
        if (popupState.hlTimer) clearTimeout(popupState.hlTimer);
        popupState.hlTimer = setTimeout(() => {
          popupState.hlTimer = null;
          syncHighlight();
        }, delayMs);
        return;
      }
      if (popupState.hlRafPending) return;
      popupState.hlRafPending = true;
      requestAnimationFrame(() => {
        popupState.hlRafPending = false;
        syncHighlight();
      });
    };

    const ensureCodeMirrorVim = async () => {
      if (popupState.cmReady && popupState.cmView && popupState.cmApi) return true;
      try {
        cmHost.innerHTML = '';
        const { mountVimEditor } = await import('./codemirror-vim.bundle.js?v=20260423_170900');
        const cmEditor = mountVimEditor(cmHost, textarea.value || '', {
          lineWrapping: false,
          tabSize: EDITOR_TAB_WIDTH,
          indentUnit: EDITOR_TAB_SPACES,
          onRun: () => { void runCode(entryId); },
          onSave: () => { void saveCodeEntry(entryId).then(() => closeCodeEditor(entryId)); },
          onChange: (txt) => {
            textarea.value = txt;
            setCodeDraft(entryId, txt);
          },
        });
        popupState.cmApi = { mountVimEditor, cmEditor };
        popupState.cmView = cmEditor.view;
        popupState.cmReady = true;
        return true;
      } catch (err) {
        appendOutputLines([`[stderr] Vim 模式加载失败，已回退普通编辑：${String(err)}`]);
        return false;
      }
    };

    const setVimMode = async (enabled) => {
      const on = !!enabled;
      if (on) {
        const ok = await ensureCodeMirrorVim();
        if (!ok) {
          popupState.vimEnabled = false;
          saveEditorVimStateLocal(false);
          savePopupSettings(entryId, { fontSize: popupState.fontSize, vimEnabled: false });
          vimBtn.textContent = 'Vim 模式：关';
          vimBtn.classList.remove('active');
          cmHost.style.display = 'none';
          textarea.style.display = '';
          highlightEl.style.display = '';
          syncHighlight();
          return;
        }
        popupState.vimEnabled = true;
        saveEditorVimStateLocal(true);
        savePopupSettings(entryId, { fontSize: popupState.fontSize, vimEnabled: true });
        cmHost.style.display = 'block';
        textarea.style.display = 'none';
        highlightEl.style.display = 'none';
        vimBtn.textContent = 'Vim 模式：开';
        vimBtn.classList.add('active');
        try { popupState.cmView?.focus(); } catch (_) {}
        return;
      }
      if (popupState.cmReady && popupState.cmView) {
        const txt = popupState.cmView.state.doc.toString();
        textarea.value = txt;
        setCodeDraft(entryId, txt);
      }
      popupState.vimEnabled = false;
      saveEditorVimStateLocal(false);
      savePopupSettings(entryId, { fontSize: popupState.fontSize, vimEnabled: false });
      cmHost.style.display = 'none';
      textarea.style.display = '';
      highlightEl.style.display = '';
      vimBtn.textContent = 'Vim 模式：关';
      vimBtn.classList.remove('active');
      syncHighlight();
      setTimeout(() => textarea.focus(), 0);
    };

    const applyEditorText = (nextText) => {
      const text = String(nextText ?? '');
      textarea.value = text;
      setCodeDraft(entryId, text);
      if (popupState.cmReady && popupState.cmApi?.cmEditor?.setDoc) {
        try {
          popupState.cmApi.cmEditor.setDoc(text);
        } catch (_) {}
      }
      scheduleHighlight(0);
      renderCodePool();
    };

    textarea.addEventListener('input', () => {
      setCodeDraft(entryId, textarea.value);
      renderCodePool();
      scheduleHighlight(80);
    });
    textarea.addEventListener('scroll', () => scheduleHighlight(0));

    fontDecBtn.onclick = (ev) => {
      ev.preventDefault();
      ev.stopPropagation();
      applyWindowFontSize(popupState.fontSize - EXPR_FONT_STEP);
      scheduleHighlight(0);
    };
    fontIncBtn.onclick = (ev) => {
      ev.preventDefault();
      ev.stopPropagation();
      applyWindowFontSize(popupState.fontSize + EXPR_FONT_STEP);
      scheduleHighlight(0);
    };
    vimBtn.onclick = (ev) => {
      ev.preventDefault();
      ev.stopPropagation();
      void setVimMode(!popupState.vimEnabled);
    };
    runBtn.onclick = async (ev) => {
      ev.preventDefault();
      ev.stopPropagation();
      await runCode(entryId);
    };
    saveBtn.onclick = async (ev) => {
      ev.preventDefault();
      ev.stopPropagation();
      await saveCodeEntry(entryId);
      setStatus(`已保存代码: ${entry.name}`);
      closeCodeEditor(entryId);
    };

    exampleSelect.addEventListener('change', () => {
      const name = String(exampleSelect.value || '');
      if (!name) return;
      const code = state.codeExamples && typeof state.codeExamples === 'object'
        ? state.codeExamples[name]
        : '';
      if (typeof code !== 'string' || !code.length) return;
      applyEditorText(code);
      setStatus(`已填入例子: ${name}`);
      exampleSelect.value = '';
      if (popupState.vimEnabled && popupState.cmView) {
        try { popupState.cmView.focus(); } catch (_) {}
      } else {
        textarea.focus();
      }
    });

    textarea.addEventListener('keydown', (ev) => {
      if ((ev.ctrlKey || ev.metaKey) && (ev.key === 's' || ev.key === 'S')) {
        ev.preventDefault();
        void saveCodeEntry(entryId).then(() => {
          setStatus(`已保存代码: ${entry.name}`);
          closeCodeEditor(entryId);
        });
        return;
      }
      if ((ev.ctrlKey || ev.metaKey) && ev.key === 'Enter') {
        ev.preventDefault();
        void runCode(entryId);
        return;
      }
      if (ev.key === 'Tab') {
        ev.preventDefault();
        if (ev.shiftKey) outdentSelection(textarea);
        else indentSelection(textarea);
        setCodeDraft(entryId, textarea.value);
        scheduleHighlight(0);
      }
    });

    editorShell.append(highlightEl, textarea, cmHost);
    body.appendChild(editorShell);

    const foot = document.createElement('div');
    foot.className = 'editor-foot';
    const left = document.createElement('span');
    left.textContent = '快捷键：Ctrl+Enter 运行 · Ctrl+S 保存';
    const info = document.createElement('span');
    info.textContent = '';
    foot.append(left, info);

    win.append(head, body, foot);
    layer.appendChild(win);

    const saveCurrentRect = () => {
      const rect = win.getBoundingClientRect();
      if (!Number.isFinite(rect.width) || !Number.isFinite(rect.height) || rect.width <= 0 || rect.height <= 0) return;
      savePopupRect(entryId, {
        left: rect.left,
        top: rect.top,
        width: rect.width,
        height: rect.height,
      });
    };

    let dragging = false;
    let baseL = 0;
    let baseT = 0;
    let startX = 0;
    let startY = 0;
    head.addEventListener('pointerdown', (ev) => {
      const targetEl = ev.target && typeof ev.target.closest === 'function' ? ev.target : null;
      if (targetEl && targetEl.closest('button, input, textarea, select, label, a')) return;
      dragging = true;
      const rect = win.getBoundingClientRect();
      baseL = rect.left;
      baseT = rect.top;
      startX = ev.clientX;
      startY = ev.clientY;
      bringWindowToFront(entryId);
      head.setPointerCapture(ev.pointerId);
    });
    head.addEventListener('pointermove', (ev) => {
      if (!dragging) return;
      const l = Math.max(6, baseL + (ev.clientX - startX));
      const t = Math.max(6, baseT + (ev.clientY - startY));
      win.style.left = `${l}px`;
      win.style.top = `${t}px`;
    });
    const stopDrag = (ev) => {
      if (!dragging) return;
      dragging = false;
      try { head.releasePointerCapture(ev.pointerId); } catch (_) {}
      saveCurrentRect();
    };
    head.addEventListener('pointerup', stopDrag);
    head.addEventListener('pointercancel', stopDrag);
    if (typeof ResizeObserver !== 'undefined') {
      const ro = new ResizeObserver(() => {
        saveCurrentRect();
      });
      ro.observe(win);
    }
    win.addEventListener('pointerdown', () => bringWindowToFront(entryId));
    bringWindowToFront(entryId);
    saveCurrentRect();
    applyWindowFontSize(popupState.fontSize);
    void setVimMode(popupState.vimEnabled);
    syncHighlight();
    void loadCodeExamples().then((items) => {
      while (exampleSelect.options.length > 1) {
        exampleSelect.remove(1);
      }
      for (const name of Object.keys(items || {}).reverse()) {
        const option = document.createElement('option');
        option.value = name;
        option.textContent = name;
        exampleSelect.appendChild(option);
      }
    });
    return win;
  }

  function openCodeEditor(entryId) {
    ensurePopupState();
    const id = String(entryId);
    const entry = Array.from(state.codeEntries || []).find((item) => item.id === id);
    if (!entry) return;
    ensureCodeDraft(entry);
    if (!state.openCodeIds.includes(id)) {
      state.openCodeIds = [...state.openCodeIds, id];
      createEditorWindow(entry);
      persistPopupState();
    } else {
      syncWindowTitle(id, entry.name || '编辑代码');
      syncWindowDraft(id);
    }
    markCodeActive(id);
    queueMicrotask(() => focusCodeEditor(id));
  }

  function replaceCodeEntry(entry) {
    state.codeEntries = Array.from(state.codeEntries || []).map((item) => item.id === entry.id ? entry : item);
    persistCodes();
    syncWindowTitle(entry.id, entry.name || '编辑代码');
    syncWindowDraft(entry.id);
    renderCodePool();
  }

  function upsertCodeEntry(entry) {
    const found = Array.from(state.codeEntries || []).some((item) => item.id === entry.id);
    if (found) {
      replaceCodeEntry(entry);
      return;
    }
    state.codeEntries = [...Array.from(state.codeEntries || []), entry];
    persistCodes();
    renderCodePool();
  }

  async function saveEditorToActiveCode() {
    const current = activeCode();
    if (!current) {
      const created = createCodeEntry({
        name: `新建代码 ${Array.from(state.codeEntries || []).length + 1}`,
        script: getEditorValue(),
      });
      upsertCodeEntry(created);
      markCodeActive(created.id);
      return created;
    }
    const next = touchCodeEntry(current, { script: getEditorValue() });
    upsertCodeEntry(next);
    markCodeActive(next.id);
    return next;
  }

  function setEditorFromCode(entry, open = false) {
    ensureCodeDraft(entry);
    markCodeActive(entry.id);
    if (open) {
      openCodeEditor(entry.id);
      return;
    }
    setEditorValue(entry.script || '');
    state.currentExpr = entry.script || '';
    if (typeof editorControls.setTitle === 'function') {
      editorControls.setTitle(entry.name || '编辑代码');
    }
  }

  async function createNewCode() {
    const entry = createCodeEntry({
      name: `新建代码 ${Array.from(state.codeEntries || []).length + 1}`,
      script: '',
    });
    upsertCodeEntry(entry);
    setEditorFromCode(entry, true);
    setStatus(`已新建代码: ${entry.name}`);
  }

  async function editCode(entryId) {
    const entry = Array.from(state.codeEntries || []).find((item) => item.id === entryId);
    if (!entry) return;
    setEditorFromCode(entry, true);
  }

  async function renameCode(entryId) {
    const entry = Array.from(state.codeEntries || []).find((item) => item.id === entryId);
    if (!entry) return;
    const nextName = promptName('重命名代码', entry.name);
    if (!nextName) return;
    const next = touchCodeEntry(entry, { name: nextName });
    upsertCodeEntry(next);
    if (state.activeCodeId === next.id && typeof editorControls.setTitle === 'function') {
      editorControls.setTitle(next.name);
    }
    setStatus(`代码已重命名为: ${nextName}`);
  }

  async function deleteCode(entryId) {
    const entry = Array.from(state.codeEntries || []).find((item) => item.id === entryId);
    if (!entry) return;
    if (!globalThis.confirm(`确认删除代码「${entry.name}」吗？`)) return;
    state.codeEntries = Array.from(state.codeEntries || []).filter((item) => item.id !== entryId);
    ensurePopupState();
    delete state.codeDrafts[String(entryId)];
    closeCodeEditor(entryId);
    if (state.activeCodeId === entryId) {
      state.activeCodeId = null;
      setEditorValue('');
      state.currentExpr = '';
      if (typeof editorControls.setTitle === 'function') {
        editorControls.setTitle('编辑代码');
      }
    }
    persistCodes();
    renderCodePool();
    setStatus(`已删除代码: ${entry.name}`);
  }

  async function runCode(entryId) {
    const entry = Array.from(state.codeEntries || []).find((item) => item.id === entryId);
    if (!entry) return false;
    const script = isCodeOpen(entryId) ? getCodeDraft(entryId) : String(entry.script || '');
    const saved = await saveCodeEntry(entryId, script);
    const target = saved || entry;
    const ok = await runScriptText(target.script, { codeId: target.id, codeName: target.name });
    renderCodePool();
    return ok;
  }

  async function saveCodeEntry(entryId, scriptOverride = null) {
    const current = Array.from(state.codeEntries || []).find((item) => item.id === String(entryId));
    if (!current) return null;
    const nextScript = String(scriptOverride ?? getCodeDraft(entryId));
    const next = touchCodeEntry(current, { script: nextScript });
    setCodeDraft(entryId, nextScript);
    upsertCodeEntry(next);
    markCodeActive(next.id);
    return next;
  }

  function renderLoadedFilesPanel() {
    renderItemPanel('loaded_files_panel', state.loadedFiles, (item) => createFileItemRow({
      name: String(item?.name ?? ''),
      meta: String(item?.meta ?? ''),
      actions: [
        ...(!item?.loaded ? [
          makeActionButton('加载', async () => {
            try {
              const bytes = item?.bytes instanceof Uint8Array ? item.bytes : null;
              if (!bytes?.byteLength) {
                setStatus(`缓存数据不可加载: ${String(item?.name ?? '')}`, true);
                return;
              }
              await applyParquetBytes(bytes, String(item?.name ?? 'last.parquet'), { persist: false });
              await refreshLoadedFilesPanel();
            } catch (err) {
              setStatus(`加载缓存数据失败: ${summarizePyError(err)}`, true);
            }
          }),
        ] : []),
        makeActionButton('重命名', async () => {
          const oldName = String(item?.name ?? '');
          const nextName = promptName('重命名已上传数据', oldName);
          if (!nextName || nextName === oldName) return;
          try {
            if (item?.loaded) {
              await ensureEngineReady();
              await evalEngine.renameLoadedData(oldName, nextName);
            }
            if (item?.cached) {
              await renameParquetSnapshot(oldName, nextName);
            }
            await refreshLoadedFilesPanel();
            setStatus(`已重命名数据: ${oldName} -> ${nextName}`);
          } catch (err) {
            setStatus(`重命名数据失败: ${summarizePyError(err)}`, true);
          }
        }),
        makeActionButton('删除', async () => {
          const oldName = String(item?.name ?? '');
          if (!globalThis.confirm(`确认删除数据「${oldName}」吗？`)) return;
          try {
            if (item?.loaded) {
              await ensureEngineReady();
              await evalEngine.deleteLoadedData(oldName);
            }
            if (item?.cached) {
              await deleteParquetSnapshotIfName(oldName);
            }
            await refreshLoadedFilesPanel();
            setStatus(`已删除数据: ${oldName}`);
          } catch (err) {
            setStatus(`删除数据失败: ${summarizePyError(err)}`, true);
          }
        }, 'danger'),
      ],
    }));
  }

  function renderSavedFilesPanel() {
    renderItemPanel('saved_files_panel', state.savedFiles, (item) => createFileItemRow({
      name: String(item?.name ?? ''),
      meta: formatBytes(item?.size ?? 0),
      actions: [
        makeActionButton('下载', async () => {
          try {
            const bytes = await evalEngine.getSavedFileBytes(String(item?.name ?? ''));
            const blob = new Blob([bytes], { type: 'application/octet-stream' });
            const url = URL.createObjectURL(blob);
            try {
              const a = document.createElement('a');
              a.href = url;
              a.download = String(item?.name ?? 'output.bin');
              document.body.appendChild(a);
              a.click();
              a.remove();
            } finally {
              URL.revokeObjectURL(url);
            }
            setStatus(`下载完成: ${String(item?.name ?? '')}`);
          } catch (err) {
            setStatus(`下载失败: ${summarizePyError(err)}`, true);
          }
        }),
        makeActionButton('删除', async () => {
          const fileName = String(item?.name ?? '');
          if (!globalThis.confirm(`确认删除已保存文件「${fileName}」吗？`)) return;
          try {
            await ensureEngineReady();
            await evalEngine.deleteSavedFile(fileName);
            await refreshSavedFilesPanel();
            setStatus(`已删除已保存文件: ${fileName}`);
          } catch (err) {
            setStatus(`删除已保存文件失败: ${summarizePyError(err)}`, true);
          }
        }, 'danger'),
      ],
    }));
  }

  function renderCodePool() {
    renderItemPanel('saved_codes_panel', state.codeEntries, (item) => {
      const draft = ensureCodeDraft(item);
      const dirty = draft !== String(item.script || '');
      return createFileItemRow({
      name: String(item.name || '未命名代码'),
      meta: `${new Date(Number(item.updatedAt || 0)).toLocaleString('zh-CN', { hour12: false })}${dirty ? ' · 未保存修改' : ''}`,
      actions: [
        makeActionButton(isCodeOpen(item.id) ? '定位' : '编辑', () => editCode(item.id)),
        makeActionButton('重命名', () => renameCode(item.id)),
        makeActionButton('运行', () => runCode(item.id)),
        makeActionButton('删除', () => deleteCode(item.id), 'danger'),
      ],
      active: state.activeCodeId === item.id || isCodeOpen(item.id),
      rowClass: 'code-item',
    });});
  }

  async function refreshLoadedFilesPanel() {
    await ensureEngineReady();
    const loadedItems = Array.from(await evalEngine.listLoadedData() || []).filter(
      (item) => String(item?.source || '') !== 'inline',
    );
    const cachedItems = Array.from(await loadParquetSnapshots() || []);
    const cachedByName = new Map(cachedItems.map((item) => [String(item?.name || ''), item]));
    const loadedRows = loadedItems.map((item) => {
      const cached = cachedByName.get(String(item?.name || ''));
      return {
        ...item,
        loaded: true,
        cached: Boolean(cached),
        bytes: cached?.bytes || null,
        meta: cached
          ? `已加载 · id=${Number(item?.dataset_id ?? 0)} · ${Number(item?.row_count ?? 0)} rows · 已缓存`
          : `已加载 · id=${Number(item?.dataset_id ?? 0)} · ${Number(item?.row_count ?? 0)} rows`,
      };
    });
    const loadedNames = new Set(loadedRows.map((item) => String(item?.name || '')));
    const cachedRows = cachedItems
      .filter((item) => !loadedNames.has(String(item?.name || '')))
      .map((item) => ({
        name: String(item?.name || 'last.parquet'),
        loaded: false,
        cached: true,
        bytes: item?.bytes || null,
        meta: `缓存 parquet · ${formatBytes(item?.bytes?.byteLength ?? 0)}`,
      }));
    state.loadedFiles = [...loadedRows, ...cachedRows];
    renderLoadedFilesPanel();
  }

  async function refreshSavedFilesPanel() {
    await ensureEngineReady();
    const items = await evalEngine.listSavedFiles();
    state.savedFiles = Array.from(items || []);
    renderSavedFilesPanel();
  }

  async function applyParquetBytes(bytes, fileName, opts = {}) {
    const { persist = true } = opts;
    await ensureEngineReady();
    const meta = await evalEngine.uploadDatasetNamed(fileName, bytes);
    await refreshLoadedFilesPanel();
    if (persist) {
      await saveParquetSnapshot(fileName, bytes);
    }
    state.datasetId = meta.dataset_id;
    setStatus(`已加载 parquet: ${fileName} (dataset_id=${state.datasetId})，可在代码里 load_data("${fileName}")`);
  }

  async function restoreCachedParquetDatasets() {
    await ensureEngineReady();
    const cachedItems = Array.from(await loadParquetSnapshots() || []);
    if (!cachedItems.length) return 0;
    const loadedItems = Array.from(await evalEngine.listLoadedData() || []);
    const loadedNames = new Set(loadedItems.map((item) => String(item?.name || '')));
    let restored = 0;
    for (const item of cachedItems) {
      const name = String(item?.name || '');
      const bytes = item?.bytes instanceof Uint8Array ? item.bytes : null;
      if (!name || !bytes?.byteLength || loadedNames.has(name)) continue;
      await evalEngine.uploadDatasetNamed(name, bytes);
      loadedNames.add(name);
      restored += 1;
    }
    return restored;
  }

  function initCodePool() {
    state.codeEntries = loadCodeEntries();
    state.activeCodeId = null;
    ensurePopupState();
    setEditorValue('');
    state.currentExpr = '';
    const restored = loadEditorWindowsState();
    state.popupRects = restored.rects || {};
    state.popupSettings = restored.settings || {};
    if (typeof editorControls.setTitle === 'function') {
      editorControls.setTitle('编辑代码');
    }
    void loadCodeExamples();
    renderCodePool();
    for (const id of restored.openIds || []) {
      if (Array.from(state.codeEntries || []).some((item) => item.id === String(id))) {
        openCodeEditor(id);
      }
    }
  }

  return {
    initCodePool,
    renderCodePool,
    refreshLoadedFilesPanel,
    refreshSavedFilesPanel,
    applyParquetBytes,
    restoreCachedParquetDatasets,
    createNewCode,
    saveEditorToActiveCode,
    setEditorFromCode,
    runCode,
    activeCode,
  };
}
