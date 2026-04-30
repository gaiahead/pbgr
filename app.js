/* PBGR 가치평가 모니터 — app.js */

const LS_KEY = 'pbgr_settings_v2';

/* ─── LocalStorage ─── */

function loadSettings() {
  try { return JSON.parse(localStorage.getItem(LS_KEY) || '{}'); } catch { return {}; }
}

function saveSettings() {
  const s = { req_kr: parseFloat(document.getElementById('req-kr').value) || 10, roe: {} };
  document.querySelectorAll('.roe-input').forEach(inp => {
    const val = parseFloat(inp.value);
    const def = parseFloat(inp.dataset.default);
    if (!isNaN(val) && Math.abs(val - def) > 0.001) s.roe[inp.dataset.ticker] = val;
  });
  localStorage.setItem(LS_KEY, JSON.stringify(s));
  setStatus('✓ 저장됨', '#4ade80');
  const btn = document.getElementById('save-btn');
  btn.className = 'save-btn saved';
  btn.textContent = '✓ 저장됨';
  renderTable();
}

/* ─── UI Helpers ─── */

function setStatus(msg, color = '#475569') {
  const el = document.getElementById('status-msg');
  el.textContent = msg;
  el.style.color = color;
}

function markDirty() {
  document.getElementById('save-btn').className = 'save-btn unsaved';
  document.getElementById('save-btn').textContent = '● 저장';
  setStatus('수정됨', '#fbbf24');
  // ROE는 포커스 잃을 때 재계산 (입력 중 방해 안 함)
  if (document.activeElement && document.activeElement.classList.contains('roe-input')) return;
  renderTable();
}

/* ─── Config ─── */

let configData = null;

async function loadConfig() {
  configData = await fetch('config.json').then(r => r.json());
  return configData;
}

/* ─── Formatters ─── */

function fmtKR(v) {
  return v != null ? Number(v).toLocaleString('ko-KR') + '원' : '—';
}

function fmtEquity(v) {
  if (!v) return '—';
  return v >= 10000
    ? (v / 10000).toFixed(1) + '조'
    : Math.round(v).toLocaleString('ko-KR') + '억';
}

function fmtShares(v) {
  return v ? (v / 1e8).toFixed(2) + '억주' : '—';
}

function gap(pbgr) {
  if (!pbgr) return '—';
  const pct = ((1 / pbgr) - 1) * 100;
  return `<span style="color:${pct >= 0 ? '#4ade80' : '#f87171'}">${pct >= 0 ? '+' : ''}${pct.toFixed(1)}%</span>`;
}

function pbgrHtml(pbgr) {
  if (!pbgr) return '—';
  const cls = pbgr < 1 ? 'under' : 'over';
  return `<span class="pbgr-val ${cls}">${pbgr.toFixed(3)}</span>`;
}

/* ─── PBGR Calculation ─── */

function recalcKR(price, equity_100m, roe_pct, shares, base_date, req_pct) {
  if (!price || !equity_100m || !roe_pct || !shares) return null;
  const base = new Date(base_date);
  const today = new Date();
  const months = (today.getFullYear() - base.getFullYear()) * 12 + (today.getMonth() - base.getMonth());
  const daysInMonth = new Date(today.getFullYear(), today.getMonth() + 1, 0).getDate();
  const dv = months + (today.getDate() - 1) / daysInMonth;
  const roe = roe_pct / 100, r = req_pct / 100;
  const y10 = equity_100m * Math.pow(1 + roe, 10);
  const y11 = equity_100m * Math.pow(1 + roe, 11);
  const r_t = Math.pow(y11 / y10, 1 / 12) - 1;
  const trailing = y10 * Math.pow(1 + r_t, dv - 1);
  const bps = trailing / Math.pow(1 + r, 10) * 1e8 / shares;
  return bps > 0 ? { pbgr: price / bps, fair_price: Math.round(bps) } : null;
}

/* ─── Equity Estimation ─── */

function estimateEquityNow(a) {
  const roeDiff = Math.abs(a._roe - a.roe_pct) > 0.001;
  if (roeDiff && a.equity_y0_100m && a._roe && a.base_date) {
    const parts = a.base_date.split('.');
    const bY = parseInt(parts[0]), bM = parseInt(parts[1]);
    const base = new Date(bY, bM - 1, new Date(bY, bM, 0).getDate());
    const t = new Date();
    const m2 = (t.getFullYear() - bY) * 12 + (t.getMonth() - (bM - 1));
    const dv2 = m2 + (t.getDate() - 1) / new Date(t.getFullYear(), t.getMonth() + 1, 0).getDate();
    return a.equity_y0_100m * Math.pow(1 + a._roe / 100, dv2 / 12);
  }
  return a.equity_now_100m;
}

/* ─── Table Rendering ─── */

let rawData = null;

function renderTable() {
  const reqKR = parseFloat(document.getElementById('req-kr').value) || 10;
  const tbody = document.getElementById('kr-body');
  const s = loadSettings();

  const currentRoe = {};
  document.querySelectorAll('.roe-input').forEach(inp => {
    currentRoe[inp.dataset.ticker] = parseFloat(inp.value);
  });

  tbody.innerHTML = '';
  const assets = rawData.assets.filter(a => a.market === 'KR');

  assets.forEach(a => {
    const configRoe = configData?.kr?.assets?.[a.ticker]?.roe ?? a.roe_pct;
    const roe = currentRoe[a.ticker] ?? s.roe?.[a.ticker] ?? configRoe;
    const isCustom = Math.abs(roe - configRoe) > 0.001;
    const calc = recalcKR(a.price, a.equity_y0_100m, roe, a.shares, a.base_date, reqKR);

    const equityNow = estimateEquityNow(a);

    // 자본총계 시리즈
    const eqSeries = a.equity_series || {};
    const actualEqKeys = Object.keys(eqSeries).filter(k => !k.includes('(E)')).sort();
    const eqActual = actualEqKeys.length ? eqSeries[actualEqKeys[actualEqKeys.length - 1]] : null;
    const estKeys = Object.keys(eqSeries).filter(k => k.includes('(E)')).sort();
    const eqEst = estKeys.length ? eqSeries[estKeys[estKeys.length - 1]] : null;

    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td><div class="name">${a.name}</div><div class="ticker">${a.ticker}</div></td>
      <td>${fmtKR(a.price)}</td>
      <td>${calc ? fmtKR(calc.fair_price) : '—'}</td>
      <td>${gap(calc?.pbgr)}</td>
      <td style="color:#64748b;font-size:0.8rem">${fmtEquity(eqActual)}</td>
      <td style="color:#64748b;font-size:0.8rem">${fmtEquity(equityNow)}</td>
      <td style="color:#64748b;font-size:0.8rem">${fmtEquity(eqEst)}</td>
      <td>${pbgrHtml(calc?.pbgr)}</td>
    `;

    // 자본 CAGR 입력 컬럼
    const roeTd = document.createElement('td');
    const roeCell = document.createElement('div');
    roeCell.className = 'roe-cell';

    const dot = document.createElement('span');
    dot.className = 'roe-dot';
    dot.style.background = isCustom ? '#60a5fa' : '#2d3748';
    dot.title = isCustom ? '수정됨 (더블클릭 복원)' : (a.roe_note || '기본값');

    const inp = document.createElement('input');
    inp.type = 'text';
    inp.inputMode = 'decimal';
    inp.className = 'roe-input' + (isCustom ? ' dirty' : '');
    inp.value = roe.toFixed(2);
    inp.dataset.ticker = a.ticker;
    inp.dataset.default = configRoe;
    inp.addEventListener('change', markDirty);
    inp.addEventListener('input', () => {
      document.getElementById('save-btn').className = 'save-btn unsaved';
      document.getElementById('save-btn').textContent = '● 저장';
      setStatus('수정됨', '#fbbf24');
    });
    inp.addEventListener('blur', renderTable);
    inp.addEventListener('dblclick', () => {
      inp.value = configRoe.toFixed(2);
      markDirty();
    });

    const unit = document.createElement('span');
    unit.className = 'unit';
    unit.textContent = '%';

    roeCell.append(dot, inp, unit);
    roeTd.appendChild(roeCell);
    tr.appendChild(roeTd);

    // 참고 지표 컬럼
    const histTd = document.createElement('td');
    histTd.style.cssText = 'text-align:right;white-space:nowrap';
    const ref = a.roe_ref;
    const cagr = a.equity_cagr_pct;
    if (ref || cagr != null) {
      let html = '';
      if (ref?.actual_avg != null)
        html += `<div style="font-size:0.78rem;color:#64748b">실적 ROE <span style="color:#94a3b8;font-weight:600">${ref.actual_avg.toFixed(1)}%</span></div>`;
      if (cagr != null)
        html += `<div style="font-size:0.78rem;color:#334155;margin-top:2px">자본 CAGR <span style="color:#e2e8f0;font-weight:700">${cagr.toFixed(1)}%</span></div>`;
      histTd.innerHTML = html || '—';
    } else {
      histTd.textContent = '—';
    }
    tr.appendChild(histTd);

    // 주식수 컬럼
    const sharesTd = document.createElement('td');
    sharesTd.style.cssText = 'text-align:right;white-space:nowrap';
    if (a.shares || a.shares_common) {
      const total = a.shares || a.shares_common;
      const common = a.shares_common;
      const preferred = a.shares_preferred;
      let html = `<div style="font-size:0.78rem;color:#94a3b8">전체 ${fmtShares(total)}</div>`;
      html += `<div style="font-size:0.72rem;color:#475569;margin-top:1px">보통주 ${fmtShares(common)}`;
      if (preferred) html += ` / 우선주 ${fmtShares(preferred)}`;
      html += '</div>';
      sharesTd.innerHTML = html;
    } else {
      sharesTd.textContent = '—';
    }
    tr.appendChild(sharesTd);
    tbody.appendChild(tr);
  });
}

/* ─── Init ─── */

async function init() {
  const [dataRes] = await Promise.all([
    fetch('pbgr_data.json?v=20260324c').then(r => r.json()),
    loadConfig()
  ]);
  rawData = dataRes;
  document.getElementById('updated').textContent = rawData.updated;
  const s = loadSettings();
  document.getElementById('req-kr').value = s.req_kr ?? (configData.kr.required_return * 100).toFixed(1);
  document.getElementById('req-kr').addEventListener('input', markDirty);
  renderTable();
}

init();
