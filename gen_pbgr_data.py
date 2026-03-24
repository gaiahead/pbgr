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

def get_wisereport_equity_cagr(code):
    """wisereport 연간 자본총계로 실제 CAGR 계산 (= ROE 대리값)"""
    url = (f"https://navercomp.wisereport.co.kr/v2/company/ajax/cF1001.aspx"
           f"?cmp_cd={code}&fin_typ=0&freq_typ=A&extY=0&extQ=0"
           f"&encparam=alV0blgxYnRzZldFanllNFlqU3Ezdz09")
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0",
        "Referer": f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={code}",
    })
    with urllib.request.urlopen(req, timeout=15) as res:
        html = res.read().decode("utf-8", errors="ignore")

    # 연간 연도 파싱
    q_start = html.find(">분기<")
    after_q = html[q_start:]
    trs = re.findall(r"<tr[^>]*>(.*?)</tr>", after_q, re.DOTALL)
    if not trs:
        return None, {}
    ths = re.findall(r"<th[^>]*>(.*?)</th>", trs[0], re.DOTALL)
    annual_years = []
    for th in ths:
        c = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", th)).strip()
        m = re.match(r"(\d{4}/\d{2})(\(E\))?", c)
        if m and "(E)" not in c:
            annual_years.append(m.group(1))

    # 자본총계 값 (title 속성)
    eq_idx = html.find(">자본총계<")
    if eq_idx < 0:
        return None, {}
    chunk = html[eq_idx:eq_idx + 1000]
    vals = re.findall(r'<td[^>]*title="(-?[0-9,]+\.?[0-9]*)"', chunk)[:len(annual_years)]

    equity_series = {}
    for yr, val in zip(annual_years, vals):
        try:
            # 연간(/12)만 포함, 분기 제외
            if yr.endswith("/12"):
                equity_series[yr] = float(val.replace(",", ""))
        except:
            pass

    if len(equity_series) < 2:
        return None, equity_series

    years = sorted(equity_series.keys())
    oldest, latest = years[0], years[-1]
    n = int(latest[:4]) - int(oldest[:4])
    if n <= 0 or equity_series[oldest] <= 0:
        return None, equity_series

    cagr = (equity_series[latest] / equity_series[oldest]) ** (1 / n) - 1
    return round(cagr * 100, 2), equity_series

def get_naver_roe(code):
    """네이버 금융 주요재무정보 테이블에서 연간 실적·추정 ROE 수집"""
    try:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://finance.naver.com/",
        })
        with urllib.request.urlopen(req, timeout=15) as res:
            html = res.read().decode("utf-8", errors="ignore")

        # ROE가 포함된 테이블 추출
        tables = re.findall(r"<table[^>]*>(.*?)</table>", html, re.DOTALL)
        roe_table = None
        for t in tables:
            if "ROE" in t:
                roe_table = t
                break
        if not roe_table:
            return {"actual": [], "actual_avg": None, "estimate": [], "estimate_avg": None}

        # 헤더에서 연도 파싱 (예: 2023.12, 2024.12, 2026.12(E))
        ths = re.findall(r"<th[^>]*>(.*?)</th>", roe_table, re.DOTALL)
        ths_clean = [re.sub(r"<[^>]+>", "", h).replace("&#40;", "(").replace("&#41;", ")").strip() for h in ths]

        year_cols = []  # (col_index, year_str, is_estimate)
        for i, h in enumerate(ths_clean):
            m = re.match(r"(\d{4})\.(\d{2})(\(E\))?", h)
            if m:
                yr = f"{m.group(1)}/{m.group(2)}"
                is_est = bool(m.group(3))
                year_cols.append((i, yr, is_est))

        # ROE 행에서 값 파싱
        trs = re.findall(r"<tr[^>]*>(.*?)</tr>", roe_table, re.DOTALL)
        roe_vals = []
        for tr in trs:
            if "ROE" in tr:
                tds = re.findall(r"<td[^>]*>(.*?)</td>", tr, re.DOTALL)
                tds_clean = [re.sub(r"<[^>]+>", "", d).strip() for d in tds]
                for v in tds_clean:
                    try:
                        roe_vals.append(float(v.replace(",", "")))
                    except:
                        roe_vals.append(None)
                break

        # 연간 실적 컬럼만 (최근 연간 실적 = 처음 3개 연도 컬럼)
        # 헤더 구조: ['주요재무정보', '최근 연간 실적', '최근 분기 실적', 연도들...]
        # 연도 컬럼 인덱스는 헤더 순서대로 매핑
        actual, estimate = [], []
        for idx, (col_i, yr, is_est) in enumerate(year_cols):
            if idx >= len(roe_vals):
                break
            v = roe_vals[idx]
            if v is None:
                continue
            entry = {"year": yr, "roe_pct": v}
            if is_est:
                estimate.append(entry)
            else:
                actual.append(entry)

        # 최근 연간 실적 3개만 (분기 제외)
        actual = actual[:3]

        act_avg = round(sum(h["roe_pct"] for h in actual) / len(actual), 2) if actual else None
        est_avg = round(sum(h["roe_pct"] for h in estimate) / len(estimate), 2) if estimate else None

        return {
            "actual": actual,
            "actual_avg": act_avg,
            "estimate": estimate,
            "estimate_avg": est_avg,
        }
    except Exception as e:
        return {"actual": [], "actual_avg": None, "estimate": [], "estimate_avg": None}


def get_wisereport_roe(code, ext_y=3):
    """네이버 금융으로 ROE 수집 (wisereport 대체)"""
    return get_naver_roe(code)

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
            roe_hist = get_wisereport_roe(ticker)
            equity_cagr, equity_series = get_wisereport_equity_cagr(ticker)

            # ROE 기본값 우선순위:
            # 1. config.json에 roe가 명시된 경우 (null 아님)
            # 2. wisereport 자본총계 CAGR (가장 오래된 결산→최신 결산)
            # 3. wisereport 실적 평균 ROE (폴백)
            if cfg.get("roe") is not None:
                roe_pct = cfg["roe"]
            elif equity_cagr is not None:
                roe_pct = equity_cagr
            else:
                roe_pct = roe_hist.get("actual_avg") or 0

            # BPS × 주식수 = 자본총계
            bps_actual = latest.get("bps")
            equity_100m = (bps_actual * shares / 1e8) if bps_actual and shares else None

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
                "roe_note": cfg.get("note", ""),
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
