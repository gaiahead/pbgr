# PBGR — Price to Book Growth Ratio 가치평가 모니터

ROE 기반 10년 성장 복리 모델로 한국·미국 주요 종목의 **적정 장부가치**를 추정하고,  
현재 주가 대비 배수(PBGR)를 시각화합니다.

- **PBGR < 1** → 저평가 (현재가 < 적정가)  
- **PBGR > 1** → 고평가 (현재가 > 적정가)

## 데이터 갱신

GitHub Actions로 **매일 KST 07:00** 자동 갱신.

| 시장 | 현재가 | ROE | 자본·주식수 |
|------|--------|-----|-------------|
| 한국 | 네이버 파이낸스 | 네이버 파이낸스 | yfinance |
| 미국 | yfinance | yfinance | yfinance |

## 종목 구성

### 한국 (요구수익률 10%, 기준일 2024-01-01)
- 삼성전자, SK하이닉스, 리노공업, 유한양행, NAVER

### 미국 (요구수익률 7%, 기준일 2021-01-01)
- Apple, Microsoft, Berkshire Hathaway

## 계산 방법

엑셀 모델 기반 Python 구현:
- **한국**: 자본Y0 × (1+ROE)^10 → Trailing 자본 → PV(요구수익률, 10년) → BPS
- **미국**: Y1~Y2 순이익 직접 반영 + ROE × 보정계수^n → Trailing → 적정시총
