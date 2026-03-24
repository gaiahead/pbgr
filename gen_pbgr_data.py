#!/usr/bin/env python3
"""
PBGR (Price to Book Growth Ratio) 데이터 생성기
- 전체 데이터: 네이버 파이낸스 (한국 종목 전용)
- ROE·요구수익률: config.json
"""
import json
import re
import urllib.request
import calendar
from datetime import datetime, timezone, timedelta

# ─── config 로드 ──────────────────────────────────────────
with open("config.json", encoding="utf-8") as f:
    CONFIG = json.load(f)

KR_CFG = CONFIG["kr"]

# ─── 날짜값 계산 ──────────────────────────────────────────
def date_value(base_date_str, today=None):
    """기준일(YYYY-MM-DD 또는 YYYY.MM)로부터 경과 월 (일할 포함)"""
    if today is None:
        today = datetime.now()
    # YYYY.MM 형식 → YYYY-MM-01로 변환
    if '.' in base_date_str:
        y, m = base_date_str.split('.')[:2]
        base = datetime(int(y), int(m), 31 if int(m) in [1,3,5,7,8,10,12] else 30)
        # 월말로
        import calendar as cal
        last_day = cal.monthrange(int(y), int(m))[1]
        base = datetime(int(y), int(m), last_day)
    else:
        base = datetime.strptime(base_date_str, "%Y-%m-%d")
    months = (today.year - base.year) * 12 + (today.month - base.month)
    days_in_month = calendar.monthrange(today.year, today.month)[1]
    frac = (today.day - 1) / days_in_month
    return months + frac

# ─── 네이버 파이낸스 수집 ─────────────────────────────────
def naver_fetch(code):
    url = f"https://finance.naver.com/item/main.naver?code={code}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept-Language": "ko-KR"})
    with urllib.request.urlopen(req, timeout=15) as res:
        return res.read().decode("euc-kr", errors="ignore")

def get_naver_price(code):
    """실시간 현재가 (polling API)"""
    url = f"https://polling.finance.naver.com/api/realtime/domestic/stock/{code}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=10) as res:
        data = json.loads(res.read())
    return int(data["datas"][0]["closePrice"].replace(",", ""))

def get_naver_shares(code, preferred_code=None):
    """보통주·우선주·전체 주식수 반환"""
    def fetch_shares(c):
        url = f"https://finance.naver.com/item/coinfo.naver?code={c}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept-Language": "ko-KR"})
        with urllib.request.urlopen(req, timeout=15) as res:
            html = res.read().decode("euc-kr", errors="ignore")
        idx = html.find("상장주식수")
        if idx < 0:
            return None
        m = re.search(r"<em[^>]*>([0-9,]+)</em>", html[idx:idx+200])
        return int(m.group(1).replace(",", "")) if m else None

    common = fetch_shares(code)
    preferred = fetch_shares(preferred_code) if preferred_code else None
    total = (common or 0) + (preferred or 0)
    return {
        "total": total if total > 0 else None,
        "common": common,
        "preferred": preferred,
    }

def get_naver_financials(code):
    """
    연도별 BPS, EPS, ROE 수집.
    반환: { '2024.12': {'bps':..., 'eps':..., 'roe':...}, ... }
    연도 순서: 오래된것→최신 (확정치만, E 제외)
    """
    html = naver_fetch(code)

    # cop_analysis 섹션에서 연간 연도 헤더 추출
    start = html.find("cop_analysis")
    end = html.find("</table>", start)
    section = html[start:end]
    ths = re.findall(r"<th[^>]*>(.*?)</th>", section, re.DOTALL)
    years = []
    for th in ths:
        clean = re.sub(r"<[^>]+>", "", th).strip()
        m = re.match(r"(\d{4}\.\d{2})", clean)
        if m:
            years.append(clean)  # 'E' 표기 포함
    annual_years = years[:4]  # 첫 4개가 연간

    def extract_series(kw):
        idx = html.find(kw)
        if idx < 0:
            return []
        chunk = html[idx:idx+800].replace("\n", "").replace("\t", "")
        return re.findall(r"<td[^>]*>\s*([0-9,.-]+)\s*</td>", chunk)[:4]

    bps_raw = extract_series("BPS()")
    eps_raw = extract_series("EPS()")
    roe_raw = extract_series("ROE")

    result = {}
    def safe_int(s):
        try: return int(re.sub(r'[^0-9-]', '', s))
        except: return None
    def safe_float(s):
        try: return float(re.sub(r'[^0-9.-]', '', s))
        except: return None

    for i, yr in enumerate(annual_years):
        bps = safe_int(bps_raw[i]) if i < len(bps_raw) else None
        eps = safe_int(eps_raw[i]) if i < len(eps_raw) else None
        roe = safe_float(roe_raw[i]) if i < len(roe_raw) else None
        result[yr] = {"bps": bps, "eps": eps, "roe": roe}

    return result

def is_estimate(yr):
    """E(추정치) 여부 판별 - HTML 엔티티 포함"""
    return "E)" in yr or "&#40;E&#41;" in yr

def get_latest_actual(financials):
    """E(추정치) 제외한 가장 최근 연도·데이터 반환"""
    actual = {yr: v for yr, v in financials.items() if not is_estimate(yr)}
    if not actual:
        return None, {}
    latest_yr = sorted(actual.keys())[-1]
    return latest_yr, actual[latest_yr]

def get_wisereport_data(code):
    """wisereport Playwright로 자본총계(지배) + ROE 동시 수집
    - #cns_Tab21 (연간 탭) 클릭 필수 → 2026E~2028E 3년치 표시
    - 반환: (equity_cagr_pct, equity_series, roe_hist)
    """
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1400, "height": 900})
            page.goto(
                f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={code}",
                wait_until="networkidle", timeout=30000
            )
            # Financial Summary 연간 탭 클릭 (2026E~2028E 3년치 표시)
            # 클릭 후 2027(E) 또는 2028(E) 컬럼이 나타날 때까지 대기
            try:
                page.click("#cns_Tab21", timeout=8000)
                # 추정 컬럼 나타날 때까지 최대 5초 대기
                page.wait_for_function(
                    "() => document.body.innerText.includes('2027') || document.body.innerText.includes('2028')",
                    timeout=5000
                )
            except:
                page.wait_for_timeout(2000)

            result = page.evaluate("""() => {
                const tables = document.querySelectorAll('table');
                for (let t of tables) {
                    if (!t.innerText.includes('자본총계(지배)') || !t.innerText.includes('ROE')) continue;
                    const ths = Array.from(t.querySelectorAll('th')).map(h=>h.innerText.trim());
                    const yearCols = [];
                    ths.forEach((h, i) => {
                        const m = h.match(/(\\d{4}\\/\\d{2})(\\(E\\))?/);
                        if (m) yearCols.push({idx: i, year: m[1], isEst: !!m[2]});
                    });
                    const rows = {};
                    t.querySelectorAll('tr').forEach(tr => {
                        const th = tr.querySelector('th');
                        if (!th) return;
                        const label = th.innerText.trim().replace(/\\s+/g, '');
                        if (!['ROE(%)', '자본총계(지배)'].includes(label)) return;
                        const tds = Array.from(tr.querySelectorAll('td')).map(d=>d.innerText.trim());
                        const data = {};
                        yearCols.forEach((col, i) => {
                            if (i < tds.length && tds[i]) {
                                const v = parseFloat(tds[i].replace(/,/g,''));
                                if (!isNaN(v)) data[col.year + (col.isEst ? '(E)' : '')] = v;
                            }
                        });
                        rows[label] = data;
                    });
                    return rows;
                }
                return null;
            }""")
            browser.close()

        if not result:
            return None, {}, {"actual": [], "actual_avg": None, "estimate": [], "estimate_avg": None}

        # ── 자본총계(지배) CAGR 계산 ──
        equity_all = result.get("자본총계(지배)", {})
        actual_eq = {k: v for k, v in equity_all.items() if "(E)" not in k}
        est_eq = {k: v for k, v in equity_all.items() if "(E)" in k}
        actual_eq_keys = sorted(actual_eq.keys())

        equity_cagr = None
        if actual_eq_keys:
            base_key = actual_eq_keys[-1]
            base_val = actual_eq[base_key]
            est_keys = sorted(est_eq.keys())
            if est_keys:
                target_key = est_keys[-1]
                target_val = est_eq[target_key]
                target_year = target_key.replace("(E)", "")
                n = int(target_year[:4]) - int(base_key[:4])
                if n > 0 and base_val > 0:
                    equity_cagr = round((target_val / base_val) ** (1 / n) - 1, 4) * 100
                    equity_cagr = round(equity_cagr, 2)

        # ── ROE 실적/추정 분리 ──
        roe_all = result.get("ROE(%)", {})
        roe_actual = []
        roe_estimate = []
        for k in sorted(roe_all.keys()):
            entry = {"year": k.replace("(E)", ""), "roe_pct": roe_all[k]}
            if "(E)" in k:
                roe_estimate.append(entry)
            else:
                roe_actual.append(entry)

        roe_actual = roe_actual[-5:]   # 최근 실적 최대 5개
        roe_estimate = roe_estimate[:3]  # 추정 최대 3개

        act_avg = round(sum(h["roe_pct"] for h in roe_actual) / len(roe_actual), 2) if roe_actual else None
        est_avg = round(sum(h["roe_pct"] for h in roe_estimate) / len(roe_estimate), 2) if roe_estimate else None

        roe_hist = {
            "actual": roe_actual,
            "actual_avg": act_avg,
            "estimate": roe_estimate,
            "estimate_avg": est_avg,
        }

        # actual_eq에 추정값도 포함해서 반환 (UI 표시용)
        equity_series_full = {**actual_eq, **est_eq}
        return equity_cagr, equity_series_full, roe_hist

    except Exception as e:
        return None, {}, {"actual": [], "actual_avg": None, "estimate": [], "estimate_avg": None}


def get_wisereport_equity_cagr(code):
    """하위호환 래퍼"""
    cagr, series, _ = get_wisereport_data(code)
    return cagr, series


def get_wisereport_roe(code, ext_y=3):
    """하위호환 래퍼"""
    _, _, roe_hist = get_wisereport_data(code)
    return roe_hist

# ─── PBGR 계산 ───────────────────────────────────────────
def calc_kr(price, equity_100m, roe_pct, shares, dv, req_return):
    if not all([price, equity_100m, roe_pct, shares]):
        return None
    roe = roe_pct / 100
    y0 = equity_100m
    y10 = y0 * (1 + roe) ** 10
    y11 = y0 * (1 + roe) ** 11
    if y10 <= 0:
        return None
    r_t = (y11 / y10) ** (1 / 12) - 1
    trailing = y10 * (1 + r_t) ** (dv - 1)
    expected_bv = trailing / (1 + req_return) ** 10
    bps = expected_bv * 1e8 / shares
    if bps <= 0:
        return None
    return {"pbgr": round(price / bps, 4), "fair_price": round(bps, 0)}

# ─── 메인 ────────────────────────────────────────────────
def main():
    KST = timezone(timedelta(hours=9))
    today = datetime.now()
    updated = datetime.now(KST).strftime("%Y-%m-%d %H:%M")

    print(f"[{updated}] PBGR 데이터 생성 시작")

    result = {
        "updated": updated,
        "kr_required_return": KR_CFG["required_return"],
        "assets": []
    }

    req_kr = KR_CFG["required_return"]

    for ticker, cfg in KR_CFG["assets"].items():
        name = cfg["name"]
        roe_pct = cfg["roe"]
        print(f"  [KR] {name} ({ticker}) ...", end=" ", flush=True)
        try:
            preferred_ticker = cfg.get("preferred_ticker")
            price = get_naver_price(ticker)
            shares_data = get_naver_shares(ticker, preferred_ticker)
            shares = shares_data["total"]
            financials = get_naver_financials(ticker)
            latest_yr, latest = get_latest_actual(financials)
            equity_cagr, equity_series, roe_hist = get_wisereport_data(ticker)

            # ROE 기본값 우선순위:
            # 1. config.json 수동 입력 (roe 필드)
            # 2. 자본총계(지배) CAGR — 기본값 (최신 실적A → 최신 추정E 복리)
            # 3. 실적 평균 ROE — 최종 폴백
            roe_pct = 0
            roe_note = "미확인"
            cfg_roe = cfg.get("roe")

            if cfg_roe is not None:
                roe_pct = cfg_roe
                roe_note = "config 수동 입력"
            elif equity_cagr is not None:
                roe_pct = equity_cagr
                roe_note = "자본총계 추정 CAGR 자동"
            else:
                roe_pct = roe_hist.get("actual_avg") or 0
                roe_note = "wisereport 실적 평균 ROE 자동"

            # BPS × 주식수 = 자본총계
            bps_actual = latest.get("bps")
            bps_equity_100m = (bps_actual * shares / 1e8) if bps_actual and shares else None
            # wisereport 자본총계(지배) 실적 최신값 우선 사용 (더 정확)
            actual_eq_keys = sorted([k for k in equity_series if "(E)" not in k])
            if actual_eq_keys:
                equity_100m = equity_series[actual_eq_keys[-1]]
            else:
                equity_100m = bps_equity_100m

            dv = date_value(latest_yr, today) if latest_yr else 0

            # 현재 시점 자본 추정 (기준일 자본 × ROE 복리 × 경과 월)
            equity_now = None
            if equity_100m and roe_pct:
                equity_now = round(equity_100m * (1 + roe_pct/100) ** (dv/12), 1)

            calc = calc_kr(price, equity_100m, roe_pct, shares, dv, req_kr)

            asset = {
                "name": name,
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
            result["assets"].append(asset)
            print(f"PBGR={calc['pbgr']:.3f} | 현재가={price:,} | 적정가={calc['fair_price']:,} | 자본={equity_100m:.0f}억 ({latest_yr})" if calc else "계산 실패")
        except Exception as e:
            print(f"오류: {e}")
            result["assets"].append({"name": name, "ticker": ticker, "market": "KR", "error": str(e)})

    with open("pbgr_data.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n✅ pbgr_data.json 저장 완료 ({len(result['assets'])}개 종목)")

if __name__ == "__main__":
    main()
