#!/usr/bin/env python3
"""
PBGR (Price to Book Growth Ratio) 데이터 생성기
- 전체 데이터: 네이버 파이낸스 (한국 종목 전용)
- ROE·요구수익률: config.json
- wisereport: 자본총계(지배) CAGR, ROE 실적/추정
"""
from __future__ import annotations

import calendar
import json
import re
import sys
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

# ─── Constants ────────────────────────────────────────────
KST = timezone(timedelta(hours=9))
HTTP_TIMEOUT = 15
NAVER_HEADERS = {"User-Agent": "Mozilla/5.0", "Accept-Language": "ko-KR"}

CONFIG_PATH = Path("config.json")
OUTPUT_PATH = Path("pbgr_data.json")


# ─── Config ───────────────────────────────────────────────
def load_config() -> dict[str, Any]:
    """config.json 로드"""
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


# ─── Date Helpers ─────────────────────────────────────────
def date_value(base_date_str: str, today: Optional[datetime] = None) -> float:
    """기준일(YYYY-MM-DD 또는 YYYY.MM)로부터 경과 월 (일할 포함)"""
    if today is None:
        today = datetime.now()

    if "." in base_date_str:
        y, m = (int(x) for x in base_date_str.split(".")[:2])
        last_day = calendar.monthrange(y, m)[1]
        base = datetime(y, m, last_day)
    else:
        base = datetime.strptime(base_date_str, "%Y-%m-%d")

    months = (today.year - base.year) * 12 + (today.month - base.month)
    days_in_month = calendar.monthrange(today.year, today.month)[1]
    return months + (today.day - 1) / days_in_month


# ─── HTTP Helpers ─────────────────────────────────────────
def _http_get(url: str, headers: Optional[dict[str, str]] = None,
              timeout: int = HTTP_TIMEOUT, encoding: str = "utf-8") -> str:
    """단순 HTTP GET → 문자열 반환"""
    hdrs = headers or {"User-Agent": "Mozilla/5.0"}
    req = urllib.request.Request(url, headers=hdrs)
    with urllib.request.urlopen(req, timeout=timeout) as res:
        return res.read().decode(encoding, errors="ignore")


# ─── Naver Finance ────────────────────────────────────────
def get_naver_price(code: str) -> int:
    """실시간 현재가 (polling API)"""
    url = f"https://polling.finance.naver.com/api/realtime/domestic/stock/{code}"
    data = json.loads(_http_get(url, timeout=10))
    return int(data["datas"][0]["closePrice"].replace(",", ""))


def _fetch_listed_shares(code: str) -> Optional[int]:
    """단일 종목의 상장주식수 반환"""
    url = f"https://finance.naver.com/item/coinfo.naver?code={code}"
    html = _http_get(url, headers=NAVER_HEADERS, encoding="euc-kr")
    idx = html.find("상장주식수")
    if idx < 0:
        return None
    m = re.search(r"<em[^>]*>([0-9,]+)</em>", html[idx:idx + 200])
    return int(m.group(1).replace(",", "")) if m else None


def get_naver_shares(code: str, preferred_code: Optional[str] = None) -> dict[str, Optional[int]]:
    """보통주·우선주·전체 주식수 반환"""
    common = _fetch_listed_shares(code)
    preferred = _fetch_listed_shares(preferred_code) if preferred_code else None
    total = (common or 0) + (preferred or 0)
    return {
        "total": total if total > 0 else None,
        "common": common,
        "preferred": preferred,
    }


def get_naver_financials(code: str) -> dict[str, dict[str, Any]]:
    """
    연도별 BPS, EPS, ROE 수집.
    반환: { '2024.12': {'bps':..., 'eps':..., 'roe':...}, ... }
    """
    url = f"https://finance.naver.com/item/main.naver?code={code}"
    html = _http_get(url, headers=NAVER_HEADERS, encoding="euc-kr")

    # cop_analysis 섹션에서 연간 연도 헤더 추출
    start = html.find("cop_analysis")
    end = html.find("</table>", start)
    section = html[start:end]
    ths = re.findall(r"<th[^>]*>(.*?)</th>", section, re.DOTALL)
    annual_years: list[str] = []
    for th in ths:
        clean = re.sub(r"<[^>]+>", "", th).strip()
        m = re.match(r"(\d{4}\.\d{2})", clean)
        if m:
            annual_years.append(clean)  # 'E' 표기 포함
    annual_years = annual_years[:4]

    def extract_series(kw: str) -> list[str]:
        idx = html.find(kw)
        if idx < 0:
            return []
        chunk = html[idx:idx + 800].replace("\n", "").replace("\t", "")
        return re.findall(r"<td[^>]*>\s*([0-9,.-]+)\s*</td>", chunk)[:4]

    bps_raw = extract_series("BPS()")
    eps_raw = extract_series("EPS()")
    roe_raw = extract_series("ROE")

    result: dict[str, dict[str, Any]] = {}
    for i, yr in enumerate(annual_years):
        bps = _safe_int(bps_raw[i]) if i < len(bps_raw) else None
        eps = _safe_int(eps_raw[i]) if i < len(eps_raw) else None
        roe = _safe_float(roe_raw[i]) if i < len(roe_raw) else None
        result[yr] = {"bps": bps, "eps": eps, "roe": roe}

    return result


def _safe_int(s: str) -> Optional[int]:
    try:
        return int(re.sub(r"[^0-9-]", "", s))
    except (ValueError, TypeError):
        return None


def _safe_float(s: str) -> Optional[float]:
    try:
        return float(re.sub(r"[^0-9.-]", "", s))
    except (ValueError, TypeError):
        return None


def is_estimate(yr: str) -> bool:
    """E(추정치) 여부 판별 — HTML 엔티티 포함"""
    return "E)" in yr or "&#40;E&#41;" in yr


def get_latest_actual(financials: dict[str, dict[str, Any]]) -> tuple[Optional[str], dict[str, Any]]:
    """E(추정치) 제외한 가장 최근 연도·데이터 반환"""
    actual = {yr: v for yr, v in financials.items() if not is_estimate(yr)}
    if not actual:
        return None, {}
    latest_yr = sorted(actual.keys())[-1]
    return latest_yr, actual[latest_yr]


def build_naver_roe_hist(financials: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """네이버 파이낸스 연간 ROE를 wisereport 형식으로 변환.

    wisereport가 실패하거나 미설치인 환경에서도 PBGR 계산이 끊기지 않도록
    최소한의 실적/추정 ROE 히스토리를 제공한다.
    """
    actual: list[dict[str, Any]] = []
    estimate: list[dict[str, Any]] = []

    for year in sorted(financials.keys()):
        roe = financials[year].get("roe")
        if roe is None:
            continue
        entry = {"year": year.replace("&#40;E&#41;", "").replace("(E)", ""), "roe_pct": roe}
        if is_estimate(year):
            estimate.append(entry)
        else:
            actual.append(entry)

    actual = actual[-5:]
    estimate = estimate[:3]
    act_avg = round(sum(h["roe_pct"] for h in actual) / len(actual), 2) if actual else None
    est_avg = round(sum(h["roe_pct"] for h in estimate) / len(estimate), 2) if estimate else None

    return {
        "actual": actual,
        "actual_avg": act_avg,
        "estimate": estimate,
        "estimate_avg": est_avg,
    }


def merge_roe_hist(primary: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
    """wisereport 결과가 비어 있으면 네이버 파이낸스 히스토리로 보강."""
    if primary.get("actual") or primary.get("estimate"):
        merged = {**fallback, **primary}
        if not merged.get("actual") and fallback.get("actual"):
            merged["actual"] = fallback["actual"]
            merged["actual_avg"] = fallback.get("actual_avg")
        if not merged.get("estimate") and fallback.get("estimate"):
            merged["estimate"] = fallback["estimate"]
            merged["estimate_avg"] = fallback.get("estimate_avg")
        return merged
    return fallback


# ─── Wisereport ───────────────────────────────────────────
_EMPTY_ROE_HIST: dict[str, Any] = {
    "actual": [], "actual_avg": None,
    "estimate": [], "estimate_avg": None,
}


def get_wisereport_data(code: str) -> tuple[Optional[float], dict[str, float], dict[str, Any]]:
    """wisereport에서 자본총계(지배) + ROE 동시 수집.

    - #cns_Tab21 (연간 탭) 클릭 → 2026E~2028E 3년치 표시
    - 반환: (equity_cagr_pct, equity_series, roe_hist)
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("    [WARN] playwright 미설치, wisereport 건너뜀")
        return None, {}, {**_EMPTY_ROE_HIST}

    try:
        result = _scrape_wisereport(code)
    except Exception as e:
        print(f"    [WARN] wisereport 스크래핑 실패: {e}")
        return None, {}, {**_EMPTY_ROE_HIST}

    if not result:
        return None, {}, {**_EMPTY_ROE_HIST}

    equity_cagr = _calc_equity_cagr(result)
    equity_series = _build_equity_series(result)
    roe_hist = _build_roe_hist(result)
    return equity_cagr, equity_series, roe_hist


def _scrape_wisereport(code: str) -> Optional[dict[str, dict[str, float]]]:
    """Playwright로 wisereport 연간 테이블 파싱"""
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            page = browser.new_page(viewport={"width": 1400, "height": 900})
            page.goto(
                f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={code}",
                wait_until="domcontentloaded", timeout=30000,
            )
            page.wait_for_timeout(1500)
            return page.evaluate(_WISEREPORT_JS)
        finally:
            browser.close()


# wisereport 테이블 파싱 JS (page.evaluate에 전달)
_WISEREPORT_JS = """async () => {
    const norm = s => (s || '').replace(/\s+/g, ' ').trim();
    const sleep = ms => new Promise(r => setTimeout(r, ms));

    const parseTable = () => {
        const tables = [...document.querySelectorAll('table')];
        const table = tables.find(t => {
            const text = norm(t.innerText);
            return text.includes('자본총계(지배)')
                && text.includes('ROE(%)')
                && text.includes('2026/12(E)')
                && text.includes('2028/12(E)');
        });
        if (!table) return null;

        const firstHeadRow = table.querySelector('thead tr');
        if (!firstHeadRow) return null;

        const yearCols = [...firstHeadRow.querySelectorAll('th')]
            .map(th => norm(th.innerText))
            .map(text => {
                const m = text.match(/(\d{4}\/\d{2})(\(E\))?/);
                return m ? { year: m[1], isEst: !!m[2] } : null;
            })
            .filter(Boolean);

        if (!yearCols.length) return null;

        const rows = {};
        table.querySelectorAll('tr').forEach(tr => {
            const th = tr.querySelector('th');
            if (!th) return;
            const label = norm(th.innerText);
            if (!['ROE(%)', '자본총계(지배)'].includes(label)) return;
            const tds = [...tr.querySelectorAll('td')].map(td => norm(td.innerText));
            const data = {};
            yearCols.forEach((col, i) => {
                const raw = tds[i];
                if (!raw) return;
                const v = parseFloat(raw.replace(/,/g, ''));
                if (!Number.isNaN(v)) data[col.year + (col.isEst ? '(E)' : '')] = v;
            });
            rows[label] = data;
        });

        return rows;
    };

    for (let i = 0; i < 6; i++) {
        const tab = document.querySelector('#cns_Tab21');
        if (tab) tab.click();
        await sleep(1200);
        const parsed = parseTable();
        if (parsed) return parsed;
    }

    return null;
}"""


def _calc_equity_cagr(result: dict[str, dict[str, float]]) -> Optional[float]:
    """자본총계(지배) 기반 CAGR 계산"""
    equity_all = result.get("자본총계(지배)", {})
    actual_eq = {k: v for k, v in equity_all.items() if "(E)" not in k}
    est_eq = {k: v for k, v in equity_all.items() if "(E)" in k}
    actual_keys = sorted(actual_eq.keys())
    if not actual_keys:
        return None

    base_key = actual_keys[-1]
    base_val = actual_eq[base_key]
    est_keys = sorted(est_eq.keys())
    if not est_keys or base_val <= 0:
        return None

    target_key = est_keys[-1]
    target_val = est_eq[target_key]
    n = int(target_key.replace("(E)", "")[:4]) - int(base_key[:4])
    if n <= 0:
        return None
    return round((target_val / base_val) ** (1 / n) - 1, 4) * 100


def _build_equity_series(result: dict[str, dict[str, float]]) -> dict[str, float]:
    """자본총계 실적+추정 시리즈 반환"""
    return result.get("자본총계(지배)", {})


def _build_roe_hist(result: dict[str, dict[str, float]]) -> dict[str, Any]:
    """ROE 실적/추정 분리 및 평균 계산"""
    roe_all = result.get("ROE(%)", {})
    roe_actual: list[dict[str, Any]] = []
    roe_estimate: list[dict[str, Any]] = []

    for k in sorted(roe_all.keys()):
        entry = {"year": k.replace("(E)", ""), "roe_pct": roe_all[k]}
        if "(E)" in k:
            roe_estimate.append(entry)
        else:
            roe_actual.append(entry)

    roe_actual = roe_actual[-5:]    # 최근 실적 최대 5개
    roe_estimate = roe_estimate[:3]  # 추정 최대 3개

    act_avg = round(sum(h["roe_pct"] for h in roe_actual) / len(roe_actual), 2) if roe_actual else None
    est_avg = round(sum(h["roe_pct"] for h in roe_estimate) / len(roe_estimate), 2) if roe_estimate else None

    return {
        "actual": roe_actual,
        "actual_avg": act_avg,
        "estimate": roe_estimate,
        "estimate_avg": est_avg,
    }


# ─── PBGR Calculation ────────────────────────────────────
def calc_kr(price: int, equity_100m: float, roe_pct: float,
            shares: int, dv: float, req_return: float) -> Optional[dict[str, float]]:
    """PBGR 및 적정가 계산"""
    if not all([price, equity_100m, roe_pct, shares]):
        return None
    roe = roe_pct / 100
    y10 = equity_100m * (1 + roe) ** 10
    y11 = equity_100m * (1 + roe) ** 11
    if y10 <= 0:
        return None
    r_t = (y11 / y10) ** (1 / 12) - 1
    trailing = y10 * (1 + r_t) ** (dv - 1)
    expected_bv = trailing / (1 + req_return) ** 10
    bps = expected_bv * 1e8 / shares
    if bps <= 0:
        return None
    return {"pbgr": round(price / bps, 4), "fair_price": round(bps, 0)}


# ─── ROE Resolution ──────────────────────────────────────
def resolve_roe(cfg: dict[str, Any], equity_cagr: Optional[float],
                roe_hist: dict[str, Any]) -> tuple[float, str]:
    """ROE 결정 (우선순위: config 수동 > 자본CAGR > 실적 평균)"""
    cfg_roe = cfg.get("roe")
    if cfg_roe is not None:
        return cfg_roe, "config 수동 입력"
    if equity_cagr is not None:
        return equity_cagr, "자본총계 추정 CAGR 자동"
    actual_avg = roe_hist.get("actual_avg") or 0
    return actual_avg, "실적 평균 ROE 자동"


# ─── Equity Resolution ───────────────────────────────────
def resolve_equity(equity_series: dict[str, float], bps_actual: Optional[int],
                   shares: Optional[int]) -> Optional[float]:
    """자본총계 결정 (wisereport 실적 우선, BPS 폴백)"""
    actual_keys = sorted(k for k in equity_series if "(E)" not in k)
    if actual_keys:
        return equity_series[actual_keys[-1]]
    if bps_actual and shares:
        return bps_actual * shares / 1e8
    return None


# ─── Asset Processing ────────────────────────────────────
def process_asset(ticker: str, cfg: dict[str, Any], req_kr: float,
                  today: datetime) -> dict[str, Any]:
    """단일 종목 처리 — 데이터 수집 + PBGR 계산"""
    preferred_ticker = cfg.get("preferred_ticker")
    price = get_naver_price(ticker)
    shares_data = get_naver_shares(ticker, preferred_ticker)
    shares = shares_data["total"]
    financials = get_naver_financials(ticker)
    latest_yr, latest = get_latest_actual(financials)
    equity_cagr, equity_series, roe_hist = get_wisereport_data(ticker)
    naver_roe_hist = build_naver_roe_hist(financials)
    roe_hist = merge_roe_hist(roe_hist, naver_roe_hist)

    roe_pct, roe_note = resolve_roe(cfg, equity_cagr, roe_hist)
    bps_actual = latest.get("bps")
    equity_100m = resolve_equity(equity_series, bps_actual, shares)
    dv = date_value(latest_yr, today) if latest_yr else 0

    # 현재 시점 자본 추정
    equity_now = None
    if equity_100m and roe_pct:
        equity_now = round(equity_100m * (1 + roe_pct / 100) ** (dv / 12), 1)

    calc = calc_kr(price, equity_100m, roe_pct, shares, dv, req_kr)

    return {
        "name": cfg["name"],
        "ticker": ticker,
        "market": "KR",
        "price": price,
        "base_date": latest_yr,
        "bps_actual": bps_actual,
        "equity_y0_100m": round(equity_100m, 1) if equity_100m else None,
        "equity_now_100m": equity_now,
        "shares": shares_data["total"],
        "shares_common": shares_data["common"],
        "shares_preferred": shares_data["preferred"],
        "roe_pct": roe_pct,
        "roe_note": cfg.get("note") or roe_note,
        "equity_cagr_pct": equity_cagr,
        "equity_series": equity_series,
        "roe_ref": roe_hist,
        "required_return_pct": round(req_kr * 100, 1),
        "pbgr": calc["pbgr"] if calc else None,
        "fair_price": calc["fair_price"] if calc else None,
    }


# ─── Main ─────────────────────────────────────────────────
def main() -> None:
    config = load_config()
    kr_cfg = config["kr"]
    today = datetime.now()
    updated = datetime.now(KST).strftime("%Y-%m-%d %H:%M")

    print(f"[{updated}] PBGR 데이터 생성 시작")

    result: dict[str, Any] = {
        "updated": updated,
        "kr_required_return": kr_cfg["required_return"],
        "assets": [],
    }

    req_kr = kr_cfg["required_return"]

    for ticker, cfg in kr_cfg["assets"].items():
        name = cfg["name"]
        print(f"  [KR] {name} ({ticker}) ...", end=" ", flush=True)
        try:
            asset = process_asset(ticker, cfg, req_kr, today)
            result["assets"].append(asset)
            calc = asset
            if calc["pbgr"]:
                print(
                    f"PBGR={calc['pbgr']:.3f} | 현재가={calc['price']:,} | "
                    f"적정가={calc['fair_price']:,.0f} | "
                    f"자본={calc['equity_y0_100m']:.0f}억 ({calc['base_date']})"
                )
            else:
                print("계산 실패")
        except Exception as e:
            print(f"오류: {e}")
            result["assets"].append({
                "name": name, "ticker": ticker, "market": "KR", "error": str(e),
            })

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n✅ {OUTPUT_PATH} 저장 완료 ({len(result['assets'])}개 종목)")


if __name__ == "__main__":
    main()
