#!/usr/bin/env python3
"""
PBGR (Price to Book Growth Ratio) 데이터 생성기
- 한국: 네이버 파이낸스(현재가·ROE) + yfinance(자본·주식수)
- 미국: yfinance 전용
"""
import json
import urllib.request
import re
import yfinance as yf
from datetime import datetime, timezone, timedelta
from scipy.optimize import brentq
import numpy as np

# ─── 파라미터 ───────────────────────────────────────────────
KR_REQUIRED_RETURN = 0.10   # 한국 요구수익률 10%
US_REQUIRED_RETURN = 0.07   # 미국 요구수익률 7%

KR_BASE_DATE = datetime(2024, 1, 1)  # 한국 기준일
US_BASE_DATE = datetime(2021, 1, 1)  # 미국 기준일

# 한국 종목: (이름, 코드, 그룹)
# ROE는 네이버에서 자동 수집, 자본/주식수는 yfinance
KR_ASSETS = [
    ("삼성전자",  "005930", "005930.KS"),
    ("SK하이닉스","000660", "000660.KS"),
    ("리노공업",  "058630", "058630.KS"),
    ("유한양행",  "000100", "000100.KS"),
    ("NAVER",     "035420", "035420.KS"),
]

# 미국 종목: (이름, 티커, 보정계수, ROE override or None)
# ROE override: None이면 yfinance 자동, 소수(예: 0.10)면 수동
US_ASSETS = [
    ("Apple",               "AAPL",  0.8, None),
    ("Microsoft",           "MSFT",  1.0, None),
    ("Berkshire Hathaway",  "BRK-B", 1.0, 0.10),  # BRK ROE 수동: ~10%
]

# ─── 날짜값 계산 (엑셀 수식 재현) ─────────────────────────
def calc_date_value(base_date, today=None):
    """기준일로부터 경과 월 (일할 포함)"""
    if today is None:
        today = datetime.now()
    months = (today.year - base_date.year) * 12 + (today.month - base_date.month)
    # 일할: (day-1) / 해당월 일수
    import calendar
    days_in_month = calendar.monthrange(today.year, today.month)[1]
    day_fraction = (today.day - 1) / days_in_month
    return months + day_fraction

# ─── 네이버 파이낸스 스크래핑 ──────────────────────────────
def get_naver_stock_info(code):
    """BPS, EPS, ROE(%), PBR, PER 수집"""
    url = f"https://finance.naver.com/item/main.naver?code={code}"
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "ko-KR"
    })
    with urllib.request.urlopen(req, timeout=15) as res:
        html = res.read().decode("euc-kr", errors="ignore")

    def extract_after(keyword):
        idx = html.find(keyword)
        if idx < 0:
            return None
        chunk = html[idx:idx+500]
        m = re.search(r"<td[^>]*>\s*[\r\n\t ]*([0-9,.-]+)\s*[\r\n\t ]*</td>", chunk)
        if m:
            return m.group(1).replace(",", "")
        return None

    result = {}
    v = extract_after("BPS()")
    if v:
        try: result["bps"] = int(v)
        except: pass
    v = extract_after("EPS()")
    if v:
        try: result["eps"] = int(v)
        except: pass
    v = extract_after("ROE")
    if v:
        try: result["roe_pct"] = float(v)
        except: pass
    v = extract_after("PBR")
    if v:
        try: result["pbr"] = float(v)
        except: pass
    v = extract_after("PER")
    if v:
        try: result["per"] = float(v)
        except: pass
    return result

def get_naver_price(code):
    """네이버 polling API로 현재가"""
    url = f"https://polling.finance.naver.com/api/realtime/domestic/stock/{code}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=10) as res:
        data = json.loads(res.read())
    price_str = data["datas"][0]["closePrice"].replace(",", "")
    return int(price_str)

# ─── yfinance 자본·주식수 ──────────────────────────────────
def get_yf_balance(ticker_code, base_date):
    """기준일 직전 결산 자본총계·주식수 반환"""
    t = yf.Ticker(ticker_code)
    bs = t.balance_sheet

    # 기준일 이전 가장 최근 결산
    available = [dt for dt in bs.columns if dt.to_pydatetime().replace(tzinfo=None) <= base_date]
    if not available:
        available = list(bs.columns)
    col = max(available)

    equity = None
    for field in ["Common Stock Equity", "Stockholders Equity"]:
        if field in bs.index and not np.isnan(bs.loc[field, col]):
            equity = float(bs.loc[field, col])
            break

    # 주식수
    ordinary = float(bs.loc["Ordinary Shares Number", col]) if "Ordinary Shares Number" in bs.index else 0
    preferred = float(bs.loc["Preferred Shares Number", col]) if "Preferred Shares Number" in bs.index else 0

    return equity, ordinary, preferred

def get_yf_us_data(ticker_code):
    """미국 종목 전체 데이터"""
    t = yf.Ticker(ticker_code)
    info = t.info
    bs = t.balance_sheet
    inc = t.income_stmt

    price = info.get("currentPrice") or info.get("regularMarketPrice")

    # 자본 (기준일 2021-01-01 이전 가장 최근)
    equity, ordinary, preferred = get_yf_balance(ticker_code, US_BASE_DATE)

    # ROE (trailing)
    roe = info.get("returnOnEquity", 0) or 0  # 소수점 표현 (예: 0.152)

    # 순이익 Y1, Y2 (최근 2년 annual)
    ni_list = []
    if "Net Income" in inc.index:
        ni_data = inc.loc["Net Income"].dropna()
        ni_list = [float(v) for v in ni_data.values[:2]]  # 최근 2년

    # BRK-B 등 일부 종목은 Ordinary Shares Number가 부정확 → sharesOutstanding 우선
    shares_from_bs = (ordinary + preferred) if ordinary and ordinary > 1e6 else None
    shares = shares_from_bs or info.get("sharesOutstanding", 0) or info.get("impliedSharesOutstanding", 0)

    return {
        "price": price,
        "equity_m": equity / 1e6 if equity else None,  # 백만달러
        "roe": roe,
        "ni_y1_m": ni_list[0] / 1e6 if len(ni_list) > 0 else None,
        "ni_y2_m": ni_list[1] / 1e6 if len(ni_list) > 1 else None,
        "shares": shares,
    }

# ─── PBGR 계산: 한국 모델 ─────────────────────────────────
def calc_pbgr_kr(price, equity_100m, roe_pct, shares_total, date_value, req_return=KR_REQUIRED_RETURN):
    """
    한국 PBGR 계산
    equity_100m: 자본Y0 (억원)
    roe_pct: ROE (%)
    shares_total: 총 주식수
    date_value: 기준일로부터 경과 월 (일할 포함)
    """
    if not all([price, equity_100m, roe_pct, shares_total]):
        return None

    roe = roe_pct / 100
    y0 = equity_100m
    y10 = y0 * (1 + roe) ** 10
    y11 = y0 * (1 + roe) ** 11

    # RATE(12, 0, -Y10, Y11) → Y10→Y11 성장률로 trailing 추정
    # RATE(nper, pmt, pv, fv): fv = pv*(1+r)^nper → r = (fv/pv)^(1/nper) - 1
    if y10 <= 0:
        return None
    r_trailing = (y11 / y10) ** (1 / 12) - 1
    # 자본Trailing: Y10 성장률 기반, date_value-1 기간 복리
    equity_trailing = y10 * (1 + r_trailing) ** (date_value - 1)

    # 기대장부가치 = Trailing / (1+req)^10
    expected_bv = equity_trailing / (1 + req_return) ** 10

    # BPS = 기대장부가치(억) * 1억 / 주식수
    bps = expected_bv * 1e8 / shares_total
    if bps <= 0:
        return None

    pbgr = price / bps
    fair_price = bps

    # EPS (기대순이익 기반)
    ni_expected = y0 * (1 + r_trailing) ** (date_value - 1) * roe / (1 + req_return)
    eps = ni_expected * 1e8 / shares_total if shares_total > 0 else None

    return {
        "pbgr": round(pbgr, 4),
        "fair_price": round(fair_price, 0),
        "bps": round(bps, 0),
        "eps": round(eps, 0) if eps else None,
        "equity_y0_100m": round(y0, 0),
        "roe_pct": round(roe_pct, 2),
    }

# ─── PBGR 계산: 미국 모델 ─────────────────────────────────
def calc_pbgr_us(price, equity_m, roe, ni_y1_m, ni_y2_m, shares, correction, date_value, req_return=US_REQUIRED_RETURN):
    """
    미국 PBGR 계산
    equity_m: 자본Y0 (백만달러)
    roe: ROE 소수 (예: 0.152)
    ni_y1_m/ni_y2_m: 순이익 Y1/Y2 (백만달러)
    correction: 보정계수 (ROE 점진적 수렴)
    """
    if not all([price, equity_m, roe, shares]):
        return None

    y = [0] * 12  # Y0~Y11
    y[0] = equity_m

    # Y1, Y2: 실제 순이익 누적 (없으면 ROE 추정)
    ni1 = ni_y1_m if ni_y1_m else equity_m * roe
    ni2 = ni_y2_m if ni_y2_m else equity_m * roe
    y[1] = y[0] + ni1
    y[2] = y[0] + ni1 + ni2

    # Y3~Y10: ROE × 보정^(n-2)
    for n in range(3, 11):
        y[n] = y[n-1] * (1 + roe * (correction ** (n - 2)))

    # Y11
    y[11] = y[10] * (1 + roe * (correction ** 9))

    # Trailing
    if y[10] <= 0:
        return None
    r_trailing = (y[11] / y[10]) ** (1 / 12) - 1
    equity_trailing = y[10] * (1 + r_trailing) ** (date_value - 1)

    # 적정시총
    fair_mktcap_m = equity_trailing / (1 + req_return) ** 10
    fair_price = fair_mktcap_m * 1e6 / shares
    if fair_price <= 0:
        return None

    pbgr = price / fair_price

    return {
        "pbgr": round(pbgr, 4),
        "fair_price": round(fair_price, 2),
        "roe_pct": round(roe * 100, 2),
        "equity_y0_m": round(equity_m, 0),
    }

# ─── 메인 ─────────────────────────────────────────────────
def main():
    today = datetime.now()
    kst = timezone(timedelta(hours=9))
    updated = datetime.now(kst).strftime("%Y-%m-%d %H:%M")

    kr_date_value = calc_date_value(KR_BASE_DATE, today)
    us_date_value = calc_date_value(US_BASE_DATE, today)

    print(f"[{updated}] PBGR 데이터 생성 시작")
    print(f"  KR 날짜값: {kr_date_value:.2f}개월 / US 날짜값: {us_date_value:.2f}개월")

    result = {
        "updated": updated,
        "kr_required_return": KR_REQUIRED_RETURN,
        "us_required_return": US_REQUIRED_RETURN,
        "assets": []
    }

    # ── 한국 종목 ──
    for name, naver_code, yf_code in KR_ASSETS:
        print(f"  [KR] {name} ...", end=" ", flush=True)
        try:
            # 현재가·ROE: 네이버
            price = get_naver_price(naver_code)
            naver = get_naver_stock_info(naver_code)
            roe_pct = naver.get("roe_pct", 0)

            # 자본·주식수: yfinance
            equity, ordinary, preferred = get_yf_balance(yf_code, KR_BASE_DATE)
            equity_100m = equity / 1e8 if equity else None
            shares_total = (ordinary + preferred) if ordinary else None

            calc = calc_pbgr_kr(price, equity_100m, roe_pct, shares_total, kr_date_value)

            asset = {
                "name": name,
                "ticker": naver_code,
                "market": "KR",
                "price": price,
                "pbgr": calc["pbgr"] if calc else None,
                "fair_price": calc["fair_price"] if calc else None,
                "bps": calc["bps"] if calc else None,
                "eps": calc["eps"] if calc else None,
                "roe_pct": roe_pct,
                "per": naver.get("per"),
                "pbr": naver.get("pbr"),
                "equity_y0_100m": round(equity_100m, 0) if equity_100m else None,
                "shares_total": int(shares_total) if shares_total else None,
            }
            result["assets"].append(asset)
            print(f"PBGR={calc['pbgr']:.3f}" if calc else "계산 실패")
        except Exception as e:
            print(f"오류: {e}")
            result["assets"].append({"name": name, "ticker": naver_code, "market": "KR", "error": str(e)})

    # ── 미국 종목 ──
    for name, ticker, correction, roe_override in US_ASSETS:
        print(f"  [US] {name} ...", end=" ", flush=True)
        try:
            data = get_yf_us_data(ticker)
            roe_used = roe_override if roe_override is not None else data["roe"]
            calc = calc_pbgr_us(
                data["price"], data["equity_m"], roe_used,
                data["ni_y1_m"], data["ni_y2_m"], data["shares"],
                correction, us_date_value
            )

            asset = {
                "name": name,
                "ticker": ticker,
                "market": "US",
                "price": data["price"],
                "pbgr": calc["pbgr"] if calc else None,
                "fair_price": calc["fair_price"] if calc else None,
                "roe_pct": calc["roe_pct"] if calc else None,
                "correction": correction,
                "equity_y0_m": data["equity_m"],
                "shares": int(data["shares"]) if data["shares"] else None,
            }
            result["assets"].append(asset)
            print(f"PBGR={calc['pbgr']:.3f}" if calc else "계산 실패")
        except Exception as e:
            print(f"오류: {e}")
            result["assets"].append({"name": name, "ticker": ticker, "market": "US", "error": str(e)})

    with open("pbgr_data.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n✅ pbgr_data.json 저장 완료 ({len(result['assets'])}개 종목)")

if __name__ == "__main__":
    main()
