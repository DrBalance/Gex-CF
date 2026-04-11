// ============================================
// Options Screener - 데이터 수집 & D1 저장
// Barchart → Cloudflare D1
// 매일 장 마감 후 21:00 UTC 자동 실행
// ============================================

const BARCHART_BASE = 'https://www.barchart.com';

// 헤더 (Barchart 웹 스크래핑용)
const HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
  'Accept': 'application/json',
  'Referer': 'https://www.barchart.com/options/unusual-activity',
  'X-Requested-With': 'XMLHttpRequest'
};

// ============================================
// 1. 활성 종목 목록 가져오기
// ============================================
export async function getActiveSymbols(db) {
  const result = await db.prepare(
    `SELECT symbol, name, type, sector, sector_etf
     FROM symbols
     WHERE is_active = 1
     ORDER BY type DESC, sector, symbol`
  ).all();
  return result.results;
}

// ============================================
// 2. Barchart에서 종목별 옵션 데이터 수집
// ============================================
export async function fetchOptionsData(symbol) {
  try {
    // Barchart 옵션 개요 API (무료, 15분 지연)
    const url = `${BARCHART_BASE}/proxies/core-api/v1/options/chain` +
      `?symbol=${symbol}` +
      `&startDate=${getToday()}` +
      `&endDate=${getDateAfterDays(56)}` + // 8주 이내만
      `&fields=symbol,expiration,callOpenInterest,putOpenInterest,` +
      `callVolume,putVolume,impliedVolatility,delta` +
      `&raw=1`;

    const res = await fetch(url, { headers: HEADERS });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);

    const data = await res.json();
    return parseOptionsChain(symbol, data);

  } catch (err) {
    console.error(`[${symbol}] 데이터 수집 실패:`, err.message);
    return null;
  }
}

// ============================================
// 3. 옵션 체인 파싱
// ============================================
function parseOptionsChain(symbol, data) {
  if (!data?.data) return null;

  const today = getToday();
  const results = [];

  // 만기일별로 그룹핑
  const byExpiry = {};
  for (const row of data.data) {
    const expiry = row.expiration;
    if (!byExpiry[expiry]) {
      byExpiry[expiry] = {
        call_vol: 0, put_vol: 0,
        call_oi: 0,  put_oi: 0,
        ivs: []
      };
    }
    byExpiry[expiry].call_vol += row.callVolume || 0;
    byExpiry[expiry].put_vol  += row.putVolume  || 0;
    byExpiry[expiry].call_oi  += row.callOpenInterest || 0;
    byExpiry[expiry].put_oi   += row.putOpenInterest  || 0;
    if (row.impliedVolatility) {
      byExpiry[expiry].ivs.push({
        iv: row.impliedVolatility,
        delta: Math.abs(row.delta || 0.5)
      });
    }
  }

  // 만기별 레코드 생성
  for (const [expiry, d] of Object.entries(byExpiry)) {
    const dte = daysBetween(today, expiry);
    if (dte < 0 || dte > 56) continue; // 8주 초과 제외

    // IV 분류 (delta 기준: ATM ~0.5, OTM call ~0.3, OTM put ~0.3)
    const atmIVs  = d.ivs.filter(x => x.delta >= 0.4 && x.delta <= 0.6);
    const otmCallIVs = d.ivs.filter(x => x.delta >= 0.2 && x.delta < 0.4);
    const otmPutIVs  = d.ivs.filter(x => x.delta >= 0.2 && x.delta < 0.4);

    results.push({
      date:        today,
      symbol,
      expiry_date: expiry,
      dte,
      call_vol:    d.call_vol,
      put_vol:     d.put_vol,
      call_oi:     d.call_oi,
      put_oi:      d.put_oi,
      pcr_vol:     d.put_vol  / (d.call_vol  || 1),
      pcr_oi:      d.put_oi   / (d.call_oi   || 1),
      atm_iv:      avg(atmIVs.map(x => x.iv)),
      otm_call_iv: avg(otmCallIVs.map(x => x.iv)),
      otm_put_iv:  avg(otmPutIVs.map(x => x.iv)),
    });
  }

  return results;
}

// ============================================
// 4. D1에 저장
// ============================================
export async function saveOptionsFlow(db, records) {
  if (!records?.length) return 0;

  const stmt = db.prepare(`
    INSERT OR REPLACE INTO options_flow
      (date, symbol, expiry_date, dte,
       call_vol, put_vol, call_oi, put_oi,
       pcr_vol, pcr_oi, atm_iv, otm_call_iv, otm_put_iv)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  // D1 batch 처리 (효율적)
  const batch = records.map(r => stmt.bind(
    r.date, r.symbol, r.expiry_date, r.dte,
    r.call_vol, r.put_vol, r.call_oi, r.put_oi,
    r.pcr_vol, r.pcr_oi, r.atm_iv, r.otm_call_iv, r.otm_put_iv
  ));

  await db.batch(batch);
  return records.length;
}

// ============================================
// 5. Baseline 업데이트 (20일 rolling 평균)
// ============================================
export async function updateBaseline(db, symbol) {
  // 최근 20일 데이터 집계
  const result = await db.prepare(`
    SELECT
      AVG(call_vol)     as avg_call_vol,
      AVG(put_vol)      as avg_put_vol,
      AVG(call_oi)      as avg_call_oi,
      AVG(put_oi)       as avg_put_oi,
      AVG(pcr_vol)      as avg_pcr_vol,
      AVG(pcr_oi)       as avg_pcr_oi,
      AVG(atm_iv)       as avg_atm_iv,
      AVG(otm_call_iv)  as avg_otm_call_iv,
      AVG(otm_put_iv)   as avg_otm_put_iv,

      -- 표준편차 (SQLite: 분산 직접 계산)
      AVG(call_vol * call_vol) - AVG(call_vol) * AVG(call_vol) as var_call_vol,
      AVG(call_oi  * call_oi)  - AVG(call_oi)  * AVG(call_oi)  as var_call_oi,
      AVG(pcr_oi   * pcr_oi)   - AVG(pcr_oi)   * AVG(pcr_oi)   as var_pcr_oi,
      AVG(otm_call_iv * otm_call_iv) - AVG(otm_call_iv) * AVG(otm_call_iv) as var_otm_call_iv

    FROM options_flow
    WHERE symbol = ?
      AND date >= date('now', '-20 days')
  `).bind(symbol).first();

  if (!result) return;

  await db.prepare(`
    INSERT OR REPLACE INTO options_baseline
      (symbol, updated_date,
       avg_call_vol, avg_put_vol, avg_call_oi, avg_put_oi,
       avg_pcr_vol, avg_pcr_oi,
       avg_atm_iv, avg_otm_call_iv, avg_otm_put_iv,
       std_call_vol, std_call_oi, std_pcr_oi, std_otm_call_iv)
    VALUES (?, date('now'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).bind(
    symbol,
    result.avg_call_vol,  result.avg_put_vol,
    result.avg_call_oi,   result.avg_put_oi,
    result.avg_pcr_vol,   result.avg_pcr_oi,
    result.avg_atm_iv,    result.avg_otm_call_iv, result.avg_otm_put_iv,
    Math.sqrt(Math.max(0, result.var_call_vol)),
    Math.sqrt(Math.max(0, result.var_call_oi)),
    Math.sqrt(Math.max(0, result.var_pcr_oi)),
    Math.sqrt(Math.max(0, result.var_otm_call_iv))
  ).run();
}

// ============================================
// 6. 스크리너 쿼리 - 이상 신호 종목
// ============================================
export async function getScreenerResults(db, date = null) {
  const targetDate = date || getToday();

  // 3일 연속 call OI 누적 + z-score 2 이상
  const signals = await db.prepare(`
    SELECT
      f.symbol,
      s.name,
      s.type,
      s.sector,
      s.sector_etf,
      f.expiry_date,
      f.dte,
      f.call_oi,
      f.put_oi,
      f.pcr_oi,
      f.otm_call_iv,
      f.otm_put_iv,
      b.avg_call_oi,
      b.avg_pcr_oi,
      -- z-score 계산
      CASE WHEN b.std_call_oi > 0
        THEN (f.call_oi - b.avg_call_oi) / b.std_call_oi
        ELSE 0
      END as oi_zscore,
      -- skew 방향 (양수 = call skew, 음수 = put skew)
      (f.otm_call_iv - f.otm_put_iv) as iv_skew
    FROM options_flow f
    JOIN symbols s USING (symbol)
    JOIN options_baseline b USING (symbol)
    WHERE f.date = ?
      AND s.is_active = 1
      AND f.dte BETWEEN 7 AND 56
    ORDER BY s.type DESC, s.sector, oi_zscore DESC
  `).bind(targetDate).all();

  return signals.results;
}

// ============================================
// 유틸리티
// ============================================
function getToday() {
  return new Date().toISOString().split('T')[0];
}

function getDateAfterDays(days) {
  const d = new Date();
  d.setDate(d.getDate() + days);
  return d.toISOString().split('T')[0];
}

function daysBetween(from, to) {
  const d1 = new Date(from);
  const d2 = new Date(to);
  return Math.round((d2 - d1) / (1000 * 60 * 60 * 24));
}

function avg(arr) {
  if (!arr.length) return null;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}
