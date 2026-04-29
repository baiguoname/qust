export function formatBytes(n) {
  const num = Number(n || 0);
  if (!Number.isFinite(num) || num <= 0) return '0 B';
  if (num < 1024) return `${num} B`;
  if (num < 1024 * 1024) return `${(num / 1024).toFixed(1)} KB`;
  return `${(num / (1024 * 1024)).toFixed(1)} MB`;
}

export function promptName(title, currentName) {
  const next = globalThis.prompt(title, currentName || '');
  if (next == null) return null;
  const trimmed = String(next).trim();
  if (!trimmed) return null;
  return trimmed;
}

export function makeActionButton(label, onClick, extraClass = '') {
  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = `mini-btn ${extraClass}`.trim();
  btn.textContent = label;
  btn.addEventListener('click', (ev) => {
    ev.preventDefault();
    ev.stopPropagation();
    void onClick();
  });
  return btn;
}

export function renderItemPanel(panelId, items, buildRow, emptyHtml = '<div class="params-empty">暂无</div>') {
  const panel = document.getElementById(panelId);
  if (!panel) return;
  const list = Array.from(items || []);
  if (list.length === 0) {
    panel.innerHTML = emptyHtml;
    return;
  }
  panel.innerHTML = '';
  for (const item of list) {
    const row = buildRow(item);
    if (row) panel.appendChild(row);
  }
}

export function createFileItemRow({
  name,
  meta,
  actions,
  active = false,
  rowClass = '',
}) {
  const row = document.createElement('div');
  row.className = `file-item${rowClass ? ` ${rowClass}` : ''}${active ? ' active' : ''}`;

  const left = document.createElement('div');
  left.className = 'file-main';

  const nameEl = document.createElement('div');
  nameEl.className = 'name';
  nameEl.textContent = String(name || '');

  const metaEl = document.createElement('div');
  metaEl.className = 'meta';
  metaEl.textContent = String(meta || '');

  left.append(nameEl, metaEl);

  const actionsEl = document.createElement('div');
  actionsEl.className = 'item-actions';
  for (const action of Array.from(actions || [])) {
    if (action) actionsEl.appendChild(action);
  }

  row.append(left, actionsEl);
  return row;
}
