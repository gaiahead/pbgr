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

# ─── 날짜값 계산 ──────────────────────────────────────────
def date_value(base_date, today=None):
    if today is None:
        today = datetime.now()
    months = (today.year - base_date.year) * 12 + (today.month - base_date.month)
    days_in_month = calendar.monthrange(today.year, today.month)[1]
    frac = (today.day - 1) / days_in_month
    return months + frac

# ─── yfinance: 전일 종가 + 자본 + 주식수 ─────────────────
def get_roe_history(ticker_code, n=3):
    """최근 n년 ROE 계산 (순이익/자본)"""
    t = yf.Ticker(ticker_code)
    bs = t.balance_sheet
    inc = t.income_stmt
    history = []
    for col in list(bs.columns)[:n]:
        try:
            eq = None
            for field in ["Common Stock Equity", "Stockholders Equity"]:
                if field in bs.index:
                    v = bs.loc[field, col]
                    if not np.isnan(v) and v > 0:
                        eq = float(v)
                        break
            ni = float(inc.loc["Net Income", col]) if "Net Income" in inc.index else None
            if eq and ni is not None:
                history.append({"year": col.year, "roe_pct": round(ni / eq * 100, 2)})
        except:
            pass
    avg = round(sum(h["roe_pct"] for h in history) / len(history), 2) if history else 0.0
    return {"history": history, "avg_pct": avg}

def get_yf_data(ticker_code, base_date=None):
    t = yf.Ticker(ticker_code)
    info = t.info

    # 전일 종가
    # history로 마지막 거래일 종가 (previousClose보다 정확)
    try:
        hist = yf.Ticker(ticker_code).history(period="5d")
        price = float(hist["Close"].dropna().iloc[-1]) if not hist.empty else None
    except:
        price = None
    price = price or info.get("regularMarketPreviousClose") or info.get("previousClose") or info.get("currentPrice")

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

    # ── 한국 종목 ──────────────────────────────────────────
    req_kr = KR_CFG["required_return"]
    for ticker_naver, cfg in KR_CFG["assets"].items():
        ticker_yf = ticker_naver + ".KS"
        name = cfg["name"]
        roe_pct = cfg["roe"]
        print(f"  [KR] {name} ({ticker_yf}) ...", end=" ", flush=True)
        try:
            d = get_yf_data(ticker_yf)
            roe_hist = get_roe_history(ticker_yf)
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
                "roe_history": roe_hist,
                "required_return_pct": round(req_kr * 100, 1),
                "equity_y0_100m": round(equity_100m, 0) if equity_100m else None,
                "shares": int(d["shares"]) if d["shares"] else None,
                "pbgr": calc["pbgr"] if calc else None,
                "fair_price": calc["fair_price"] if calc else None,
            }
            result["assets"].append(asset)
            print(f"PBGR={calc['pbgr']:.3f} | 현재가={d['price']:,} | 적정가={calc['fair_price']:,}" if calc else "계산 실패")
        except Exception as e:
            print(f"오류: {e}")
            result["assets"].append({"name": name, "ticker": ticker_naver, "market": "KR", "error": str(e)})


    with open("pbgr_data.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n✅ pbgr_data.json 저장 완료 ({len(result['assets'])}개 종목)")

if __name__ == "__main__":
    main()
