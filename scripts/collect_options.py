"""
Options Data Collector
yfinance → Cloudflare D1

매일 장 마감 후 실행:
1. API에서 활성 종목 목록 조회
2. yfinance로 옵션 체인 수집
3. D1 REST API로 저장
"""

import os
import json
import time
import requests
import yfinance as yf
from datetime import date, datetime, timedelta
from math import log, sqrt, exp
from statistics import mean, stdev

# ── 환경변수 ──
CF_ACCOUNT_ID = os.environ['CF_ACCOUNT_ID']
CF_API_TOKEN  = os.environ['CF_API_TOKEN']
CF_DB_ID      = os.environ['CF_DB_ID']
ADMIN_SECRET  = os.environ['ADMIN_SECRET']
API_BASE      = os.environ.get('API_BASE', 'https://api.drbalance.xyz')

TODAY = date.today().isoformat()
MAX_DTE = 56  # 8주 이내 만기만

# ── D1 REST API 헬퍼 ──
D1_URL = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/d1/database/{CF_DB_ID}/query"
HEADERS = {
    "Authorization": f"Bearer {CF_API_TOKEN}",
    "Content-Type": "application/json"
}

def d1_query(sql, params=None):
    """D1에 SQL 실행 — 파라미터를 인라인으로 치환"""
    if params:
        def esc(v):
            if v is None: return "NULL"
            if isinstance(v, str): return "'" + v.replace("'", "''") + "'"
            return str(v)
        # ? 플레이스홀더를 순서대로 치환
        for p in params:
            sql = sql.replace("?", esc(p), 1)
    payload = {"sql": sql}
    r = requests.post(D1_URL, headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    result = r.json()
    if not result.get("success"):
        raise Exception(f"D1 error: {result.get('errors')}")
    return result["result"][0] if result.get("result") else None

def d1_batch(statements):
    """여러 INSERT를 하나의 멀티행 INSERT로 합쳐서 /query 한 번 호출"""
    # 모든 statements가 같은 SQL 구조라고 가정 (INSERT OR REPLACE INTO options_flow)
    if not statements:
        return
    # params 값들을 문자열로 이스케이프해서 단일 쿼리로 합치기
    rows = []
    for stmt in statements:
        params = stmt["params"]
        def esc(v):
            if v is None: return "NULL"
            if isinstance(v, str): return "'" + v.replace("'", "''") + "'"
            return str(v)
        rows.append("(" + ",".join(esc(p) for p in params) + ")")

    sql = """INSERT OR REPLACE INTO options_flow
      (date, symbol, expiry_date, dte,
       call_vol, put_vol, call_oi, put_oi,
       pcr_vol, pcr_oi, atm_iv, otm_call_iv, otm_put_iv)
    VALUES """ + ",
    ".join(rows)

    r = requests.post(D1_URL, headers=HEADERS, json={"sql": sql}, timeout=60)
    r.raise_for_status()
    result = r.json()
    if not result.get("success"):
        raise Exception(f"D1 error: {result.get('errors')}")
    return result

# ── 활성 종목 목록 가져오기 ──
def get_active_symbols():
    """API에서 활성 종목 목록 조회"""
    r = requests.get(
        f"{API_BASE}/api/admin/symbols",
        params={"secret": ADMIN_SECRET},
        timeout=15
    )
    r.raise_for_status()
    data = r.json()
    symbols = [s for s in data.get("symbols", []) if s["is_active"]]
    print(f"[INFO] 활성 종목 {len(symbols)}개 조회됨")
    return symbols

# ── yfinance 옵션 체인 수집 ──
def fetch_options(symbol):
    """종목별 옵션 체인 수집 (8주 이내 만기)"""
    try:
        ticker = yf.Ticker(symbol)
        expirations = ticker.options  # 가능한 만기 목록

        today = date.today()
        records = []

        for exp_str in expirations:
            exp_date = date.fromisoformat(exp_str)
            dte = (exp_date - today).days

            # 8주 초과 제외
            if dte < 0 or dte > MAX_DTE:
                continue

            try:
                chain = ticker.option_chain(exp_str)
                calls = chain.calls
                puts  = chain.puts

                # 집계
                call_vol = int(calls['volume'].fillna(0).sum())
                put_vol  = int(puts['volume'].fillna(0).sum())
                call_oi  = int(calls['openInterest'].fillna(0).sum())
                put_oi   = int(puts['openInterest'].fillna(0).sum())

                # PCR
                pcr_vol = round(put_vol / call_vol, 4) if call_vol > 0 else None
                pcr_oi  = round(put_oi  / call_oi,  4) if call_oi  > 0 else None

                # IV — ATM / OTM call / OTM put 분리
                # 현재가 기준 delta 근사: 0.5 = ATM, 0.3 = OTM
                try:
                    spot = ticker.fast_info.last_price or 0
                    if spot > 0:
                        # ATM: strike이 현재가의 ±5% 이내
                        atm_calls = calls[
                            (calls['strike'] >= spot * 0.95) &
                            (calls['strike'] <= spot * 1.05)
                        ]
                        # OTM call: strike이 현재가의 105~120%
                        otm_calls = calls[
                            (calls['strike'] > spot * 1.05) &
                            (calls['strike'] <= spot * 1.20)
                        ]
                        # OTM put: strike이 현재가의 80~95%
                        otm_puts = puts[
                            (puts['strike'] >= spot * 0.80) &
                            (puts['strike'] < spot * 0.95)
                        ]

                        atm_iv = _avg_iv(atm_calls['impliedVolatility'])
                        otm_call_iv = _avg_iv(otm_calls['impliedVolatility'])
                        otm_put_iv  = _avg_iv(otm_puts['impliedVolatility'])
                    else:
                        atm_iv = otm_call_iv = otm_put_iv = None
                except Exception:
                    atm_iv = otm_call_iv = otm_put_iv = None

                records.append({
                    "date":        TODAY,
                    "symbol":      symbol,
                    "expiry_date": exp_str,
                    "dte":         dte,
                    "call_vol":    call_vol,
                    "put_vol":     put_vol,
                    "call_oi":     call_oi,
                    "put_oi":      put_oi,
                    "pcr_vol":     pcr_vol,
                    "pcr_oi":      pcr_oi,
                    "atm_iv":      atm_iv,
                    "otm_call_iv": otm_call_iv,
                    "otm_put_iv":  otm_put_iv,
                })

            except Exception as e:
                print(f"  [WARN] {symbol} {exp_str} 체인 실패: {e}")
                continue

        return records

    except Exception as e:
        print(f"  [ERROR] {symbol} 수집 실패: {e}")
        return []

def _avg_iv(series):
    """IV 평균 계산 (0~5 범위로 필터링)"""
    vals = [v for v in series.dropna() if 0 < v < 5]
    return round(mean(vals), 4) if vals else None

# ── D1에 저장 ──
def save_to_d1(records):
    """options_flow에 배치 저장"""
    if not records:
        return 0

    statements = []
    for r in records:
        statements.append({
            "sql": """
                INSERT OR REPLACE INTO options_flow
                  (date, symbol, expiry_date, dte,
                   call_vol, put_vol, call_oi, put_oi,
                   pcr_vol, pcr_oi, atm_iv, otm_call_iv, otm_put_iv)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            "params": [
                r["date"], r["symbol"], r["expiry_date"], r["dte"],
                r["call_vol"], r["put_vol"], r["call_oi"], r["put_oi"],
                r["pcr_vol"], r["pcr_oi"], r["atm_iv"], r["otm_call_iv"], r["otm_put_iv"]
            ]
        })

    # D1 배치는 100개 제한 — 나눠서 처리
    chunk_size = 80
    total = 0
    for i in range(0, len(statements), chunk_size):
        chunk = statements[i:i+chunk_size]
        d1_batch(chunk)
        total += len(chunk)

    return total

# ── Baseline 업데이트 ──
def update_baseline(symbol):
    """20일 rolling 평균/표준편차로 baseline 갱신"""
    try:
        result = d1_query("""
            SELECT
              AVG(call_vol) as avg_cv, AVG(put_vol) as avg_pv,
              AVG(call_oi)  as avg_co, AVG(put_oi)  as avg_po,
              AVG(pcr_vol)  as avg_pcrv, AVG(pcr_oi) as avg_pcro,
              AVG(atm_iv)   as avg_atm,
              AVG(otm_call_iv) as avg_oci, AVG(otm_put_iv) as avg_opi,
              AVG(call_vol*call_vol) - AVG(call_vol)*AVG(call_vol) as var_cv,
              AVG(call_oi*call_oi)   - AVG(call_oi)*AVG(call_oi)   as var_co,
              AVG(pcr_oi*pcr_oi)     - AVG(pcr_oi)*AVG(pcr_oi)     as var_pcro,
              AVG(otm_call_iv*otm_call_iv) - AVG(otm_call_iv)*AVG(otm_call_iv) as var_oci
            FROM options_flow
            WHERE symbol = ?
              AND date >= date('now', '-20 days')
        """, [symbol])

        if not result or not result.get("results"):
            return

        row = result["results"][0]
        if not row.get("avg_cv"):
            return

        def safe_sqrt(v):
            return round((v ** 0.5) if v and v > 0 else 1, 6)

        d1_query("""
            INSERT OR REPLACE INTO options_baseline
              (symbol, updated_date,
               avg_call_vol, avg_put_vol, avg_call_oi, avg_put_oi,
               avg_pcr_vol, avg_pcr_oi,
               avg_atm_iv, avg_otm_call_iv, avg_otm_put_iv,
               std_call_vol, std_call_oi, std_pcr_oi, std_otm_call_iv)
            VALUES (?, date('now'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            symbol,
            row.get("avg_cv"), row.get("avg_pv"),
            row.get("avg_co"), row.get("avg_po"),
            row.get("avg_pcrv"), row.get("avg_pcro"),
            row.get("avg_atm"), row.get("avg_oci"), row.get("avg_opi"),
            safe_sqrt(row.get("var_cv")),
            safe_sqrt(row.get("var_co")),
            safe_sqrt(row.get("var_pcro")),
            safe_sqrt(row.get("var_oci")),
        ])

    except Exception as e:
        print(f"  [WARN] {symbol} baseline 업데이트 실패: {e}")

# ── 메인 ──
def main():
    print(f"[START] Options Collector — {TODAY}")
    print(f"[INFO] D1 DB: {CF_DB_ID}")

    # 1. 활성 종목 조회
    symbols = get_active_symbols()
    if not symbols:
        print("[WARN] 활성 종목이 없습니다.")
        return

    total_records = 0
    success_count = 0
    fail_count = 0

    # 2. 종목별 수집
    for i, sym_info in enumerate(symbols):
        symbol = sym_info["symbol"]
        print(f"\n[{i+1}/{len(symbols)}] {symbol} ({sym_info['type']}) 수집 중...")

        records = fetch_options(symbol)

        if records:
            saved = save_to_d1(records)
            update_baseline(symbol)
            total_records += saved
            success_count += 1
            print(f"  → {len(records)}개 만기, {saved}개 레코드 저장")
        else:
            fail_count += 1
            print(f"  → 데이터 없음")

        # API 과부하 방지 (yfinance rate limit)
        time.sleep(0.5)

    # 3. 결과 요약
    print(f"\n{'='*50}")
    print(f"[완료] {TODAY}")
    print(f"  성공: {success_count}개 종목")
    print(f"  실패: {fail_count}개 종목")
    print(f"  총 레코드: {total_records}개")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()
