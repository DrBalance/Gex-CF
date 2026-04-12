"""
Options Data Collector
yfinance -> Cloudflare D1 REST API
"""

import os
import time
import requests
import yfinance as yf
from datetime import date
from statistics import mean

CF_ACCOUNT_ID = os.environ["CF_ACCOUNT_ID"]
CF_API_TOKEN  = os.environ["CF_API_TOKEN"]
CF_DB_ID      = os.environ["CF_DB_ID"]
ADMIN_SECRET  = os.environ["ADMIN_SECRET"]
API_BASE      = os.environ.get("API_BASE", "https://api.drbalance.xyz")

TODAY   = date.today().isoformat()
MAX_DTE = 56

D1_URL  = ("https://api.cloudflare.com/client/v4/accounts/"
           + CF_ACCOUNT_ID + "/d1/database/" + CF_DB_ID + "/query")
HEADERS = {"Authorization": "Bearer " + CF_API_TOKEN,
           "Content-Type": "application/json"}


def esc(v):
    if v is None:
        return "NULL"
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return str(v)


def d1_exec(sql):
    r = requests.post(D1_URL, headers=HEADERS, json={"sql": sql}, timeout=60)
    r.raise_for_status()
    result = r.json()
    if not result.get("success"):
        raise Exception("D1 error: " + str(result.get("errors")))
    res = result.get("result")
    return res[0] if res else None


def get_active_symbols():
    r = requests.get(API_BASE + "/api/admin/symbols",
                     params={"secret": ADMIN_SECRET}, timeout=15)
    r.raise_for_status()
    data = r.json()
    symbols = [s for s in data.get("symbols", []) if s["is_active"]]
    print("[INFO] 활성 종목 " + str(len(symbols)) + "개 조회됨")
    return symbols


def avg_iv(series):
    vals = [v for v in series.dropna() if 0 < v < 5]
    return round(mean(vals), 4) if vals else None


def fetch_options(symbol):
    try:
        ticker = yf.Ticker(symbol)
        expirations = ticker.options
        today = date.today()
        records = []

        for exp_str in expirations:
            exp_date = date.fromisoformat(exp_str)
            dte = (exp_date - today).days
            if dte < 0 or dte > MAX_DTE:
                continue

            try:
                chain   = ticker.option_chain(exp_str)
                calls   = chain.calls
                puts    = chain.puts

                call_vol = int(calls["volume"].fillna(0).sum())
                put_vol  = int(puts["volume"].fillna(0).sum())
                call_oi  = int(calls["openInterest"].fillna(0).sum())
                put_oi   = int(puts["openInterest"].fillna(0).sum())
                pcr_vol  = round(put_vol / call_vol, 4) if call_vol > 0 else None
                pcr_oi   = round(put_oi  / call_oi,  4) if call_oi  > 0 else None

                atm_iv = otm_call_iv = otm_put_iv = None
                try:
                    spot = ticker.fast_info.last_price or 0
                    if spot > 0:
                        atm_c = calls[(calls["strike"] >= spot*0.95) & (calls["strike"] <= spot*1.05)]
                        otm_c = calls[(calls["strike"] >  spot*1.05) & (calls["strike"] <= spot*1.20)]
                        otm_p = puts[ (puts["strike"]  >= spot*0.80) & (puts["strike"]  <  spot*0.95)]
                        atm_iv      = avg_iv(atm_c["impliedVolatility"])
                        otm_call_iv = avg_iv(otm_c["impliedVolatility"])
                        otm_put_iv  = avg_iv(otm_p["impliedVolatility"])
                except Exception:
                    pass

                records.append({
                    "date": TODAY, "symbol": symbol,
                    "expiry_date": exp_str, "dte": dte,
                    "call_vol": call_vol, "put_vol": put_vol,
                    "call_oi": call_oi, "put_oi": put_oi,
                    "pcr_vol": pcr_vol, "pcr_oi": pcr_oi,
                    "atm_iv": atm_iv, "otm_call_iv": otm_call_iv,
                    "otm_put_iv": otm_put_iv,
                })
            except Exception as e:
                print("  [WARN] " + symbol + " " + exp_str + ": " + str(e))

        return records

    except Exception as e:
        print("  [ERROR] " + symbol + ": " + str(e))
        return []


def save_to_d1(records):
    if not records:
        return 0

    total = 0
    chunk_size = 50

    for i in range(0, len(records), chunk_size):
        chunk = records[i:i+chunk_size]
        rows = []
        for r in chunk:
            row = ("(" + esc(r["date"]) + "," + esc(r["symbol"]) + ","
                   + esc(r["expiry_date"]) + "," + esc(r["dte"]) + ","
                   + esc(r["call_vol"]) + "," + esc(r["put_vol"]) + ","
                   + esc(r["call_oi"]) + "," + esc(r["put_oi"]) + ","
                   + esc(r["pcr_vol"]) + "," + esc(r["pcr_oi"]) + ","
                   + esc(r["atm_iv"]) + "," + esc(r["otm_call_iv"]) + ","
                   + esc(r["otm_put_iv"]) + ")")
            rows.append(row)

        sql = ("INSERT OR REPLACE INTO options_flow "
               "(date,symbol,expiry_date,dte,call_vol,put_vol,call_oi,put_oi,"
               "pcr_vol,pcr_oi,atm_iv,otm_call_iv,otm_put_iv) VALUES "
               + ",".join(rows))
        d1_exec(sql)
        total += len(chunk)

    return total


def update_baseline(symbol):
    try:
        sql = ("SELECT AVG(call_vol) as avg_cv, AVG(put_vol) as avg_pv,"
               "AVG(call_oi) as avg_co, AVG(put_oi) as avg_po,"
               "AVG(pcr_vol) as avg_pcrv, AVG(pcr_oi) as avg_pcro,"
               "AVG(atm_iv) as avg_atm,"
               "AVG(otm_call_iv) as avg_oci, AVG(otm_put_iv) as avg_opi,"
               "AVG(call_vol*call_vol)-AVG(call_vol)*AVG(call_vol) as var_cv,"
               "AVG(call_oi*call_oi)-AVG(call_oi)*AVG(call_oi) as var_co,"
               "AVG(pcr_oi*pcr_oi)-AVG(pcr_oi)*AVG(pcr_oi) as var_pcro,"
               "AVG(otm_call_iv*otm_call_iv)-AVG(otm_call_iv)*AVG(otm_call_iv) as var_oci "
               "FROM options_flow WHERE symbol=" + esc(symbol)
               + " AND date>=date('now','-20 days')")
        result = d1_exec(sql)
        if not result or not result.get("results"):
            return
        row = result["results"][0]
        if not row.get("avg_cv"):
            return

        def sq(v):
            return round((v**0.5) if v and v > 0 else 1, 6)

        upsert = ("INSERT OR REPLACE INTO options_baseline "
                  "(symbol,updated_date,avg_call_vol,avg_put_vol,avg_call_oi,avg_put_oi,"
                  "avg_pcr_vol,avg_pcr_oi,avg_atm_iv,avg_otm_call_iv,avg_otm_put_iv,"
                  "std_call_vol,std_call_oi,std_pcr_oi,std_otm_call_iv) VALUES ("
                  + esc(symbol) + ",date('now'),"
                  + esc(row.get("avg_cv")) + "," + esc(row.get("avg_pv")) + ","
                  + esc(row.get("avg_co")) + "," + esc(row.get("avg_po")) + ","
                  + esc(row.get("avg_pcrv")) + "," + esc(row.get("avg_pcro")) + ","
                  + esc(row.get("avg_atm")) + "," + esc(row.get("avg_oci")) + ","
                  + esc(row.get("avg_opi")) + ","
                  + esc(sq(row.get("var_cv"))) + "," + esc(sq(row.get("var_co"))) + ","
                  + esc(sq(row.get("var_pcro"))) + "," + esc(sq(row.get("var_oci"))) + ")")
        d1_exec(upsert)

    except Exception as e:
        print("  [WARN] " + symbol + " baseline: " + str(e))


def main():
    print("[START] Options Collector -- " + TODAY)

    symbols = get_active_symbols()
    if not symbols:
        print("[WARN] 활성 종목 없음")
        return

    total_records = 0
    success_count = 0
    fail_count = 0

    for i, sym_info in enumerate(symbols):
        symbol = sym_info["symbol"]
        print("[" + str(i+1) + "/" + str(len(symbols)) + "] " + symbol + " 수집 중...")

        records = fetch_options(symbol)
        if records:
            saved = save_to_d1(records)
            update_baseline(symbol)
            total_records += saved
            success_count += 1
            print("  -> " + str(len(records)) + "개 만기, " + str(saved) + "개 저장")
        else:
            fail_count += 1
            print("  -> 데이터 없음")

        time.sleep(0.5)

    print("=" * 50)
    print("[완료] " + TODAY)
    print("  성공: " + str(success_count) + "개")
    print("  실패: " + str(fail_count) + "개")
    print("  총: " + str(total_records) + "개")
    print("=" * 50)


if __name__ == "__main__":
    main()
