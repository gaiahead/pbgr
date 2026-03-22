#!/usr/bin/env python3
"""
PBGR (Price to Book Growth Ratio) 데이터 생성기
- 현재가/자본/주식수: yfinance (전일 종가 기준)
- ROE·요구수익률: config.json
"""
import json
import numpy as np
import yfinance as yf
from datetime import datetime, timezone, timedelta
import calendar

# ─── config 로드 ──────────────────────────────────────────
with open("config.json", encoding="utf-8") as f:
    CONFIG = json.load(f)

KR_CFG = CONFIG["kr"]
US_CFG = CONFIG["us"]

US_BASE_DATE = datetime.strptime(US_CFG["base_date"], "%Y-%m-%d")

# ─── 날짜값 계산 ──────────────────────────────────────────
def date_value(base_date, today=None):
    if today is None:
        today = datetime.now()
    months = (today.year - base_date.year) * 12 + (today.month - base_date.month)
    days_in_month = calendar.monthrange(today.year, today.month)[1]
    frac = (today.day - 1) / days_in_month
    return months + frac

# ─── yfinance: 전일 종가 + 자본 + 주식수 ─────────────────
def get_yf_data(ticker_code, base_date=None):
    t = yf.Ticker(ticker_code)
    info = t.info

    # 전일 종가
    price = info.get("previousClose") or info.get("regularMarketPreviousClose") or info.get("currentPrice")

    # Balance sheet (최신 결산 or 기준일 이전)
    bs = t.balance_sheet
    if base_date:
        available = [dt for dt in bs.columns if dt.to_pydatetime().replace(tzinfo=None) <= base_date]
        col = max(available) if available else bs.columns[0]
    else:
        col = bs.columns[0]

    equity = None
    for field in ["Common Stock Equity", "Stockholders Equity"]:
        if field in bs.index:
            v = bs.loc[field, col]
            if not np.isnan(v):
                equity = float(v)
                break

    ordinary = float(bs.loc["Ordinary Shares Number", col]) if "Ordinary Shares Number" in bs.index else 0
    preferred = float(bs.loc["Preferred Shares Number", col]) if "Preferred Shares Number" in bs.index else 0

    # 주식수: BS 값이 부정확하면 info 폴백
    shares = (ordinary + preferred) if ordinary and ordinary > 1e6 else None
    shares = shares or info.get("sharesOutstanding") or info.get("impliedSharesOutstanding")

    base_dt = col.to_pydatetime().replace(tzinfo=None)

    # 순이익 (미국 Y1/Y2용)
    ni_list = []
    inc = t.income_stmt
    if "Net Income" in inc.index:
        ni_list = [float(v) for v in inc.loc["Net Income"].dropna().values[:2]]

    roe_auto = info.get("returnOnEquity")  # 소수 표현

    return {
        "price": price,
        "equity": equity,
        "shares": shares,
        "base_dt": base_dt,
        "ni_list": ni_list,
        "roe_auto": roe_auto,
    }

# ─── PBGR 계산: 한국 모델 ────────────────────────────────
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

# ─── PBGR 계산: 미국 모델 ────────────────────────────────
def calc_us(price, equity_m, roe, ni_y1_m, ni_y2_m, shares, correction, dv, req_return):
    if not all([price, equity_m, roe, shares]):
        return None
    y = [0.0] * 12
    y[0] = equity_m
    y[1] = y[0] + (ni_y1_m if ni_y1_m else equity_m * roe)
    y[2] = y[1] + (ni_y2_m if ni_y2_m else equity_m * roe)
    for n in range(3, 11):
        y[n] = y[n-1] * (1 + roe * (correction ** (n - 2)))
    y[11] = y[10] * (1 + roe * (correction ** 9))
    if y[10] <= 0:
        return None
    r_t = (y[11] / y[10]) ** (1 / 12) - 1
    trailing = y[10] * (1 + r_t) ** (dv - 1)
    fair_mktcap_m = trailing / (1 + req_return) ** 10
    fair_price = fair_mktcap_m * 1e6 / shares
    if fair_price <= 0:
        return None
    return {"pbgr": round(price / fair_price, 4), "fair_price": round(fair_price, 2)}

# ─── 메인 ────────────────────────────────────────────────
def main():
    KST = timezone(timedelta(hours=9))
    today = datetime.now()
    updated = datetime.now(KST).strftime("%Y-%m-%d %H:%M")

    print(f"[{updated}] PBGR 데이터 생성 시작")

    result = {
        "updated": updated,
        "kr_required_return": KR_CFG["required_return"],
        "us_required_return": US_CFG["required_return"],
        "assets": []
    }

    # ── 한국 종목 ──────────────────────────────────────────
    req_kr = KR_CFG["required_return"]
    for ticker_naver, cfg in KR_CFG["assets"].items():
        ticker_yf = ticker_naver + ".KS"
        name = cfg["name"]
        roe_pct = cfg["roe"]
        print(f"  [KR] {name} ({ticker_yf}) ...", end=" ", flush=True)
        try:
            d = get_yf_data(ticker_yf)
            equity_100m = d["equity"] / 1e8 if d["equity"] else None
            dv = date_value(d["base_dt"], today)
            calc = calc_kr(d["price"], equity_100m, roe_pct, d["shares"], dv, req_kr)

            asset = {
                "name": name,
                "ticker": ticker_naver,
                "market": "KR",
                "price": d["price"],
                "base_date": d["base_dt"].strftime("%Y-%m-%d"),
                "roe_pct": roe_pct,
                "roe_note": cfg.get("note", ""),
                "required_return_pct": round(req_kr * 100, 1),
                "equity_y0_100m": round(equity_100m, 0) if equity_100m else None,
                "pbgr": calc["pbgr"] if calc else None,
                "fair_price": calc["fair_price"] if calc else None,
            }
            result["assets"].append(asset)
            print(f"PBGR={calc['pbgr']:.3f} | 현재가={d['price']:,} | 적정가={calc['fair_price']:,}" if calc else "계산 실패")
        except Exception as e:
            print(f"오류: {e}")
            result["assets"].append({"name": name, "ticker": ticker_naver, "market": "KR", "error": str(e)})

    # ── 미국 종목 ──────────────────────────────────────────
    req_us = US_CFG["required_return"]
    for ticker, cfg in US_CFG["assets"].items():
        name = cfg["name"]
        correction = cfg.get("correction", 1.0)
        roe_override = cfg.get("roe")  # None이면 yfinance 자동
        print(f"  [US] {name} ({ticker}) ...", end=" ", flush=True)
        try:
            d = get_yf_data(ticker, base_date=US_BASE_DATE)
            equity_m = d["equity"] / 1e6 if d["equity"] else None
            roe = roe_override if roe_override is not None else d["roe_auto"]
            ni_y1 = d["ni_list"][0] / 1e6 if len(d["ni_list"]) > 0 else None
            ni_y2 = d["ni_list"][1] / 1e6 if len(d["ni_list"]) > 1 else None
            dv = date_value(US_BASE_DATE, today)
            calc = calc_us(d["price"], equity_m, roe, ni_y1, ni_y2, d["shares"], correction, dv, req_us)

            asset = {
                "name": name,
                "ticker": ticker,
                "market": "US",
                "price": d["price"],
                "base_date": US_BASE_DATE.strftime("%Y-%m-%d"),
                "roe_pct": round(roe * 100, 2) if roe else None,
                "roe_note": cfg.get("note", ""),
                "correction": correction,
                "required_return_pct": round(req_us * 100, 1),
                "equity_y0_m": round(equity_m, 0) if equity_m else None,
                "pbgr": calc["pbgr"] if calc else None,
                "fair_price": calc["fair_price"] if calc else None,
            }
            result["assets"].append(asset)
            print(f"PBGR={calc['pbgr']:.3f} | 현재가=${d['price']} | 적정가=${calc['fair_price']:,.2f}" if calc else "계산 실패")
        except Exception as e:
            print(f"오류: {e}")
            result["assets"].append({"name": name, "ticker": ticker, "market": "US", "error": str(e)})

    with open("pbgr_data.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n✅ pbgr_data.json 저장 완료 ({len(result['assets'])}개 종목)")

if __name__ == "__main__":
    main()
