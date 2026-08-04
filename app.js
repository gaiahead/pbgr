/* PBGR 가치평가 모니터 — app.js */

const LS_KEY = 'pbgr_settings_v2';
let marketCagrOverrides = {};

/* ─── LocalStorage ─── */

function loadSettings() {
  try { return JSON.parse(localStorage.getItem(LS_KEY) || '{}'); } catch { return {}; }
}

function saveSettings() {
  const s = {
    req_kr: parseFloat(document.getElementById('req-kr').value) || 10,
    market_cagr_overrides: normalizeMarketCagrOverrides(marketCagrOverrides),
  };
  marketCagrOverrides = s.market_cagr_overrides;
  localStorage.setItem(LS_KEY, JSON.stringify(s));
  setStatus('✓ 저장됨', '#16a34a');
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

function markDirty(shouldRender = true) {
  updateRequiredReturnHint();
  document.getElementById('save-btn').className = 'save-btn unsaved';
  document.getElementById('save-btn').textContent = '● 저장';
  setStatus('수정됨', '#c2410c');
  if (shouldRender) renderTable();
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

function fmtKoreanEok(v) {
  if (v == null || !Number.isFinite(Number(v)) || Number(v) === 0) return '—';
  const n = Number(v);
  const abs = Math.abs(n);
  const sign = n < 0 ? '-' : '';
  if (abs >= 100000000) return sign + (abs / 100000000).toFixed(2).replace(/\.00$/, '') + '경';
  if (abs >= 10000) return sign + (abs / 10000).toFixed(1).replace(/\.0$/, '') + '조';
  return sign + Math.round(abs).toLocaleString('ko-KR') + '억';
}

function fmtEquity(v) {
  return fmtKoreanEok(v);
}

function fmtShares(v) {
  return v ? (v / 1e8).toFixed(2) + '억주' : '—';
}

function fmtMarketCapKR(price, shares) {
  if (!price || !shares) return '—';
  const won = Number(price) * Number(shares);
  if (!Number.isFinite(won) || won <= 0) return '—';
  return fmtKoreanEok(won / 1e8);
}

function updateRequiredReturnHint() {
  const input = document.getElementById('req-kr');
  const hint = document.getElementById('req-kr-discount');
  if (!input || !hint) return;

  const reqPct = parseFloat(input.value);
  if (!Number.isFinite(reqPct) || reqPct <= -100) {
    hint.textContent = '할인계수 계산 불가';
    return;
  }

  const rate = 1 + reqPct / 100;
  const discountFactor = 1 / Math.pow(rate, 10);
  hint.textContent = `1 / ${rate.toFixed(2)}^10 = ${discountFactor.toFixed(3)}배`;
}

function gap(pbgr) {
  if (!pbgr) return '—';
  const rawPct = ((1 / pbgr) - 1) * 100;
  const pct = Math.abs(rawPct) < 0.05 ? 0 : rawPct;
  const cls = pct > 0 ? 'positive' : pct < 0 ? 'negative' : 'neutral';
  const sign = pct > 0 ? '+' : '';
  return `<span class="gap-val ${cls}">${sign}${pct.toFixed(1)}%</span>`;
}

function pbgrHtml(pbgr) {
  if (!pbgr) return '—';
  const delta = pbgr - 1;
  const cls = Math.abs(delta) < 0.0005 ? 'neutral' : delta < 0 ? 'under' : 'over';
  return `<span class="pbgr-val ${cls}">${pbgr.toFixed(3)}</span>`;
}

/* ─── PBGR Calculation ─── */

function dateValueMonths(baseDate, today = new Date()) {
  const match = String(baseDate || '').match(/^(\d{4})[.-](\d{1,2})/);
  if (!match) return null;
  const baseYear = Number(match[1]);
  const baseMonth = Number(match[2]) - 1;
  if (baseMonth < 0 || baseMonth > 11) return null;
  const months = (today.getFullYear() - baseYear) * 12 + (today.getMonth() - baseMonth);
  const daysInMonth = new Date(today.getFullYear(), today.getMonth() + 1, 0).getDate();
  return months + (today.getDate() - 1) / daysInMonth;
}

function recalcKR(price, equity_100m, cagr_pct, shares, base_date, req_pct, today = new Date()) {
  const values = [price, equity_100m, cagr_pct, shares, req_pct];
  if (!values.every(v => Number.isFinite(Number(v)))) return null;
  if (price <= 0 || equity_100m <= 0 || shares <= 0 || cagr_pct <= -100 || req_pct <= -100) return null;
  const dv = dateValueMonths(base_date, today);
  if (dv == null) return null;
  const cagr = cagr_pct / 100, r = req_pct / 100;
  const y10FromBase = equity_100m * Math.pow(1 + cagr, 10);
  const y11FromBase = equity_100m * Math.pow(1 + cagr, 11);
  const monthlyGrowth = Math.pow(y11FromBase / y10FromBase, 1 / 12) - 1;
  // 자본총계 (+10년)는 최근 실적 기준 +10년이 아니라, 현 시점에서 +10년 값이다.
  // 따라서 적정가 계산에 쓰는 현재시점 보정 후 10년 자본과 화면 표시값을 일치시킨다.
  const equity10FromNow = y10FromBase * Math.pow(1 + monthlyGrowth, dv - 1);
  const bps = equity10FromNow / Math.pow(1 + r, 10) * 1e8 / shares;
  const equityNow = equity_100m * Math.pow(1 + cagr, dv / 12);
  return bps > 0 ? {
    pbgr: price / bps,
    fair_price: Math.round(bps),
    equity_now_100m: equityNow,
    equity10_100m: equity10FromNow
  } : null;
}

function impliedCagrKR(price, equity_100m, shares, base_date, req_pct, today = new Date()) {
  const values = [price, equity_100m, shares, req_pct];
  if (!values.every(v => Number.isFinite(Number(v)))) return null;
  if (price <= 0 || equity_100m <= 0 || shares <= 0 || req_pct <= -100) return null;
  const dv = dateValueMonths(base_date, today);
  if (dv == null) return null;
  const projectionYears = 10 + (dv - 1) / 12;
  if (projectionYears <= 0) return null;

  const req = req_pct / 100;
  const targetEquity100m = price * shares / 1e8 * Math.pow(1 + req, 10);
  const ratio = targetEquity100m / equity_100m;
  if (ratio <= 0) return null;
  const implied = Math.pow(ratio, 1 / projectionYears) - 1;
  return Number.isFinite(implied) ? implied * 100 : null;
}

function isValidCagrPct(value) {
  if (value == null || String(value).trim() === '') return false;
  return Number.isFinite(Number(value)) && Number(value) > -100;
}

function normalizeMarketCagrOverrides(raw) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return {};
  const clean = {};
  Object.entries(raw).forEach(([ticker, value]) => {
    if (!/^[0-9A-Z.-]+$/.test(ticker) || !isValidCagrPct(value)) return;
    clean[ticker] = Number(value);
  });
  return clean;
}

function removeMarketCagrOverride(raw, ticker) {
  const clean = normalizeMarketCagrOverrides(raw);
  delete clean[String(ticker)];
  return clean;
}

function resolveMarketCagrKR(asset, reqPct, overrides = {}, today = new Date()) {
  const saved = Object.prototype.hasOwnProperty.call(overrides, asset.ticker)
    ? overrides[asset.ticker]
    : null;
  if (isValidCagrPct(saved)) return Number(saved);
  return impliedCagrKR(
    asset.price,
    asset.equity_y0_100m,
    asset.shares,
    asset.base_date,
    reqPct,
    today,
  );
}

/* ─── Table Rendering ─── */

let rawData = null;

function updateRowValuation(tr, asset, calc) {
  const field = name => tr.querySelector(`[data-field="${name}"]`);
  field('fair-marketcap').textContent = calc ? fmtMarketCapKR(calc.fair_price, asset.shares) : '—';
  field('pbgr').innerHTML = pbgrHtml(calc?.pbgr);
  field('gap').innerHTML = gap(calc?.pbgr);
  field('fair-price').textContent = calc ? fmtKR(calc.fair_price) : '—';
  field('equity-now').textContent = fmtEquity(calc?.equity_now_100m ?? null);
  field('equity-10').textContent = fmtEquity(calc?.equity10_100m ?? null);
}

function renderTable() {
  const reqKR = parseFloat(document.getElementById('req-kr').value) || 10;
  const tbody = document.getElementById('kr-body');

  tbody.innerHTML = '';
  const assets = rawData.assets.filter(a => a.market === 'KR');

  assets.forEach(a => {
    const hasMarketOverride = Object.prototype.hasOwnProperty.call(
      marketCagrOverrides,
      a.ticker,
    );
    const marketCagr = resolveMarketCagrKR(a, reqKR, marketCagrOverrides);
    const calc = recalcKR(
      a.price, a.equity_y0_100m, marketCagr, a.shares, a.base_date, reqKR
    );

    // 자본총계 시리즈
    const eqSeries = a.equity_series || {};
    const actualEqKeys = Object.keys(eqSeries).filter(k => !k.includes('(E)')).sort();
    const eqActual = actualEqKeys.length ? eqSeries[actualEqKeys[actualEqKeys.length - 1]] : null;

    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td><div class="name">${a.name}</div><div class="ticker">${a.ticker}</div></td>
      <td class="metric-cell marketcap-cell">${fmtMarketCapKR(a.price, a.shares)}</td>
      <td class="metric-cell cagr-cell cagr-market">
        <span class="market-cagr-editor">
          <input class="market-cagr-input" type="number" step="0.01"
            value="${isValidCagrPct(marketCagr) ? Number(marketCagr).toFixed(2) : ''}"
            aria-label="${a.name} 시장 평가 CAGR"
            title="괴리율·적정가·PBGR에 적용됩니다. 값을 비우면 종가 역산값으로 복원됩니다.">
          <span class="unit">%</span>
          <button class="market-cagr-reset" type="button"
            aria-label="${a.name} 시장 평가 초기화"
            title="${a.name} 시장 평가 초기화"
            ${hasMarketOverride ? '' : 'disabled'}>↺</button>
        </span>
      </td>
      <td class="metric-cell fair-marketcap-cell" data-field="fair-marketcap">${calc ? fmtMarketCapKR(calc.fair_price, a.shares) : '—'}</td>
      <td data-field="pbgr">${pbgrHtml(calc?.pbgr)}</td>
      <td data-field="gap">${gap(calc?.pbgr)}</td>
      <td>${fmtKR(a.price)}</td>
      <td data-field="fair-price">${calc ? fmtKR(calc.fair_price) : '—'}</td>
      <td class="metric-cell equity-cell">${fmtEquity(eqActual)}</td>
      <td class="metric-cell equity-cell" data-field="equity-now">${fmtEquity(calc?.equity_now_100m ?? null)}</td>
      <td class="metric-cell equity-cell" data-field="equity-10">${fmtEquity(calc?.equity10_100m ?? null)}</td>
    `;

    const marketInput = tr.querySelector('.market-cagr-input');
    const resetButton = tr.querySelector('.market-cagr-reset');
    const restoreImpliedMarketCagr = () => {
      marketCagrOverrides = removeMarketCagrOverride(marketCagrOverrides, a.ticker);
      const currentReq = parseFloat(document.getElementById('req-kr').value) || 10;
      const fallback = resolveMarketCagrKR(a, currentReq, marketCagrOverrides);
      marketInput.value = isValidCagrPct(fallback) ? Number(fallback).toFixed(2) : '';
      resetButton.disabled = true;
      updateRowValuation(
        tr,
        a,
        recalcKR(a.price, a.equity_y0_100m, fallback, a.shares, a.base_date, currentReq),
      );
    };

    marketInput.addEventListener('input', () => {
      const raw = marketInput.value.trim();
      const value = raw === '' ? null : Number(raw);
      let editedCalc = null;
      if (isValidCagrPct(value)) {
        marketCagrOverrides[a.ticker] = value;
        resetButton.disabled = false;
        const currentReq = parseFloat(document.getElementById('req-kr').value) || 10;
        editedCalc = recalcKR(
          a.price, a.equity_y0_100m, value, a.shares, a.base_date, currentReq
        );
      } else {
        marketCagrOverrides = removeMarketCagrOverride(marketCagrOverrides, a.ticker);
        resetButton.disabled = true;
      }
      updateRowValuation(tr, a, editedCalc);
      markDirty(false);
    });
    marketInput.addEventListener('change', () => {
      if (isValidCagrPct(marketInput.value.trim())) return;
      restoreImpliedMarketCagr();
    });
    resetButton.addEventListener('click', () => {
      restoreImpliedMarketCagr();
      markDirty(false);
      marketInput.focus();
    });

    // 주식수 컬럼
    const sharesTd = document.createElement('td');
    sharesTd.className = 'shares-cell';
    if (a.shares || a.shares_common) {
      const total = a.shares || a.shares_common;
      const common = a.shares_common;
      const preferred = a.shares_preferred;
      let html = `<div class="shares-total">전체 ${fmtShares(total)}</div>`;
      html += `<div class="shares-detail">보통주 ${fmtShares(common)}`;
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
    fetch('pbgr_data.json?v=market-eval-reset-v1-20260804').then(r => r.json()),
    loadConfig()
  ]);
  rawData = dataRes;
  document.getElementById('updated').textContent = rawData.updated;
  const s = loadSettings();
  marketCagrOverrides = normalizeMarketCagrOverrides(s.market_cagr_overrides);
  document.getElementById('req-kr').value = s.req_kr ?? (configData.kr.required_return * 100).toFixed(1);
  updateRequiredReturnHint();
  document.getElementById('req-kr').addEventListener('input', markDirty);
  renderTable();
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    dateValueMonths,
    recalcKR,
    impliedCagrKR,
    normalizeMarketCagrOverrides,
    removeMarketCagrOverride,
    resolveMarketCagrKR,
  };
}
if (typeof document !== 'undefined') init();
