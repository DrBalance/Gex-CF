// GEX Dashboard - Cloudflare Workers (v7.1 - MD App VIX 실시간 + RSI 다이버전스)
// 엔드포인트:
//   /api/options?symbol=SPY              → CBOE (초기 로드, 무료)
//   /api/options?symbol=SPY&mode=cached  → Market Data App (유료)
//   /api/price                           → Yahoo Finance 현재가
//   /api/vix                             → Market Data App 실시간 VIX/VVIX (Yahoo 폴백)
//   /api/vannacharm                      → VannaCharm API
//   /api/gex0dte                         → Cron 계산 결과 (KV 읽기 전용)
//
// MD App indices/quotes 응답:
//   { "s":"ok", "symbol":["VIX"], "last":[29.92], "updated":[unix] }
//   last[0] = 현재가 (숫자 단위, VIX=29.92 형태 — Yahoo와 동일)
//   prevClose 없음 → KV에 전일 종가 별도 저장

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', ...CORS },
  });
}

// ── KV 캐시 헬퍼 ──
async function withCache(env, key, ttl, fetcher) {
  try {
    const cached = await env.CACHE.get(key);
    if (cached) {
      const parsed = JSON.parse(cached);
      // 캐시 출처 표시 (디버깅용)
      if (typeof parsed === 'object' && parsed !== null && !Array.isArray(parsed)) {
        parsed._cached = true;
        parsed._cacheKey = key;
      }
      return parsed;
    }
  } catch (_) {}

  const data = await fetcher();

  try {
    await env.CACHE.put(key, JSON.stringify(data), { expirationTtl: ttl });
  } catch (e) {
    console.error('KV put failed:', e.message);
  }

  return data;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// /api/options 메인 핸들러
// mode=initial(기본): CBOE 무료 데이터 (초기 로드)
// mode=cached:        Market Data App (유료 전환 후 자동갱신)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function handleOptions(url, env) {
  const symbol = (url.searchParams.get('symbol') || 'SPY').toUpperCase();
  if (!/^[\^_A-Z0-9]{1,10}$/.test(symbol)) {
    return json({ error: 'Invalid symbol' }, 400);
  }

  const mode = url.searchParams.get('mode') || 'initial';

  if (mode === 'cached') {
    return handleOptionsMDCached(url, env, symbol);
  }

  // 기본: CBOE 무료 데이터
  return handleOptionsCBOE(url, env, symbol);
}

// ── CBOE 무료 옵션 체인 (초기 로드용) ──
async function handleOptionsCBOE(url, env, symbol) {
  const nowEST = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const estHour = nowEST.getHours() + nowEST.getMinutes() / 60;
  const isMarket = estHour >= 9.5 && estHour < 16;
  const ttl = isMarket ? 300 : 3600;
  const todayStr = nowEST.toLocaleDateString('en-CA');
  const cacheKey = `cboe:${symbol}:${todayStr}`;

  try {
    const data = await withCache(env, cacheKey, ttl, async () => {
      const r = await fetch(
        `https://cdn.cboe.com/api/global/delayed_quotes/options/${symbol}.json`,
        {
          headers: {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Referer': 'https://www.cboe.com/',
          },
          signal: AbortSignal.timeout(10000),
        }
      );
      if (!r.ok) throw new Error(`CBOE ${r.status}`);
      const cboeJson = await r.json();
      return {
        ...cboeJson,
        source: 'cboe',
        timestamp: new Date().toISOString(),
      };
    });
    return json(data);
  } catch (err) {
    return json({ error: err.message, symbol, source: 'error' }, 500);
  }
}

// ── Market Data App mode=cached (유료 전환 후 자동갱신용) ──
async function handleOptionsMDCached(url, env, symbol) {
  const mdSymbol = symbol.replace('_', '');
  const nowEST = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const estHour = nowEST.getHours() + nowEST.getMinutes() / 60;
  const isMarket = estHour >= 9.5 && estHour < 16;
  const ttl = isMarket ? 300 : 3600;

  const todayStr = nowEST.toLocaleDateString('en-CA');
  const cacheKey = `md_cached:${mdSymbol}:${todayStr}`;

  try {
    const data = await withCache(env, cacheKey, ttl, async () => {

      // 현재가 조회
      const priceR = await fetch(
        `https://api.marketdata.app/v1/stocks/quotes/${mdSymbol}/`,
        {
          headers: {
            'Authorization': `Bearer ${env.MARKETDATA_TOKEN}`,
            'Accept': 'application/json',
          },
          signal: AbortSignal.timeout(8000),
        }
      );
      let currentPrice = 0;
      if (priceR.ok) {
        const priceJson = await priceR.json();
        currentPrice = priceJson.last?.[0] ?? priceJson.mid?.[0] ?? 0;
      }

      // 옵션 체인: mode=cached → 1크레딧으로 전체 체인
      const chainUrl = `https://api.marketdata.app/v1/options/chain/${mdSymbol}/` +
        `?expiration=all&mode=cached`;

      const chainR = await fetch(chainUrl, {
        headers: {
          'Authorization': `Bearer ${env.MARKETDATA_TOKEN}`,
          'Accept': 'application/json',
        },
        signal: AbortSignal.timeout(20000),
      });

      if (!chainR.ok) {
        const errText = await chainR.text();
        throw new Error(`MarketData ${chainR.status}: ${errText}`);
      }

      const mdJson = await chainR.json();
      if (mdJson.s !== 'ok') {
        throw new Error(`MarketData error: ${mdJson.errmsg || 'unknown'}`);
      }

      const {
        optionSymbol, bid, ask, last, volume, openInterest,
        iv, delta, gamma, theta, vega
      } = mdJson;

      const count = optionSymbol?.length || 0;
      if (count === 0) throw new Error('EMPTY');

      const options = [];
      for (let i = 0; i < count; i++) {
        options.push({
          option:        optionSymbol?.[i] || '',
          iv:            iv?.[i]           || 0,
          gamma:         gamma?.[i]        || 0,
          delta:         delta?.[i]        || 0,
          theta:         theta?.[i]        || 0,
          vega:          vega?.[i]         || 0,
          open_interest: openInterest?.[i] || 0,
          volume:        volume?.[i]       || 0,
          bid:           bid?.[i]          || 0,
          ask:           ask?.[i]          || 0,
          last:          last?.[i]         || 0,
        });
      }

      return {
        data: { current_price: currentPrice, options },
        timestamp: new Date().toISOString(),
        source: 'marketdata.app_cached',
      };
    });

    return json(data);
  } catch (err) {
    // Market Data App 실패 시 CBOE 폴백
    console.error(`MD cached failed (${err.message}), falling back to CBOE`);
    return handleOptionsCBOE(url, env, symbol);
  }
}

// ── /api/price ──
async function handlePrice(url, env) {
  const symbol = (url.searchParams.get('symbol') || 'SPY').toUpperCase();
  if (!/^[A-Z]{1,10}$/.test(symbol)) {
    return json({ error: 'Invalid symbol' }, 400);
  }

  try {
    const data = await withCache(env, `price:${symbol}`, 60, async () => {
      const r = await fetch(
        `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}?interval=1m&range=1d`,
        { headers: { 'User-Agent': 'Mozilla/5.0' }, signal: AbortSignal.timeout(8000) }
      );
      const j = await r.json();
      const meta = j?.chart?.result?.[0]?.meta;
      if (!meta) throw new Error('No meta');

      const marketState = meta.marketState;
      let price, priceLabel;
      if (marketState === 'PRE' && meta.preMarketPrice) {
        price = meta.preMarketPrice; priceLabel = 'preMarket';
      } else if (marketState === 'POST' && meta.postMarketPrice) {
        price = meta.postMarketPrice; priceLabel = 'postMarket';
      } else {
        price = meta.regularMarketPrice; priceLabel = 'regular';
      }

      return {
        symbol, price, priceLabel, marketState,
        regularPrice:            meta.regularMarketPrice,
        prevClose:               meta.chartPreviousClose,
        preMarketPrice:          meta.preMarketPrice          ?? null,
        postMarketPrice:         meta.postMarketPrice         ?? null,
        preMarketChangePercent:  meta.preMarketChangePercent  ?? null,
        postMarketChangePercent: meta.postMarketChangePercent ?? null,
        preMarketTime:           meta.preMarketTime  ? new Date(meta.preMarketTime  * 1000).toISOString() : null,
        postMarketTime:          meta.postMarketTime ? new Date(meta.postMarketTime * 1000).toISOString() : null,
      };
    });
    return json(data);
  } catch (err) {
    return json({ error: err.message }, 500);
  }
}

// ── /api/vix — Market Data App 실시간 (Yahoo 폴백) ──
// MD App 응답: { "s":"ok", "symbol":["VIX"], "last":[29.92], "updated":[unix] }
// last[0] = 현재가 숫자 (VIX 29.92, VVIX 110.5 형태 — Yahoo와 동일 단위)
// prevClose: MD App 미제공 → KV 날짜별 저장으로 보완
async function handleVix(url, env) {
  const symbols = (url.searchParams.get('symbols') || 'VIX,VVIX')
    .split(',').slice(0, 5).map(s => s.trim().toUpperCase());

  const nowEST = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const todayStr = nowEST.toLocaleDateString('en-CA');
  const results = {};

  await Promise.all(symbols.map(async (sym) => {
    try {
      // 1. Market Data App 실시간 (캐시 없이 직접 호출)
      let price = null, updatedAt = null;
      try {
        const mdR = await fetch(
          `https://api.marketdata.app/v1/indices/quotes/${sym}/`,
          {
            headers: {
              'Authorization': `Bearer ${env.MARKETDATA_TOKEN}`,
              'Accept': 'application/json',
            },
            signal: AbortSignal.timeout(6000),
          }
        );
        if (mdR.ok) {
          const mdJ = await mdR.json();
          // last[0] 이 현재가. 단위: VIX=29.92, VVIX=110.5 (% 아님, 그대로 사용)
          if (mdJ.s === 'ok' && Array.isArray(mdJ.last) && mdJ.last[0] != null) {
            price = mdJ.last[0];
            updatedAt = mdJ.updated?.[0] ?? null;
          }
        }
      } catch (mdErr) {
        console.warn(`MD App ${sym} failed: ${mdErr.message}`);
      }

      // 2. MD App 실패 시 Yahoo 폴백
      if (price == null) {
        const yR = await fetch(
          `https://query1.finance.yahoo.com/v8/finance/chart/%5E${sym}?interval=1m&range=1d`,
          { headers: { 'User-Agent': 'Mozilla/5.0' }, signal: AbortSignal.timeout(8000) }
        );
        const yJ = await yR.json();
        const meta = yJ?.chart?.result?.[0]?.meta;
        if (!meta) throw new Error('No data');
        price = meta.regularMarketPrice;
      }

      // 3. prevClose: KV에서 전일 종가 조회
      const prevCloseKey = `vix_prev:${sym}`;
      let prevClose = null;
      try {
        const stored = await env.CACHE.get(prevCloseKey);
        if (stored) {
          const obj = JSON.parse(stored);
          if (obj.date !== todayStr) prevClose = obj.price; // 어제 데이터면 사용
        }
      } catch (_) {}

      // 장 종료(16:00 EST) 이후 오늘 종가 저장 → 내일의 prevClose
      const estHour = nowEST.getHours() + nowEST.getMinutes() / 60;
      if (estHour >= 16 && price != null) {
        try {
          await env.CACHE.put(prevCloseKey,
            JSON.stringify({ date: todayStr, price }),
            { expirationTtl: 86400 * 3 }
          );
        } catch (_) {}
      }

      const pctChange = (price != null && prevClose != null)
        ? +((price - prevClose) / prevClose * 100).toFixed(2)
        : null;

      results[sym] = { price, prevClose, pctChange, updatedAt, source: 'marketdata' };
    } catch (e) {
      results[sym] = { error: e.message };
    }
  }));

  return json(results);
}

// ── /api/vannacharm ──
async function handleVannaCharm(url, env) {
  const symbol = (url.searchParams.get('symbol') || 'SPY').toUpperCase();
  const nowEST = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const yyyy = nowEST.getFullYear();
  const mm   = String(nowEST.getMonth() + 1).padStart(2, '0');
  const dd   = String(nowEST.getDate()).padStart(2, '0');
  const tradeDate = url.searchParams.get('date') || `${yyyy}-${mm}-${dd}`;

  const estHour = nowEST.getHours() + nowEST.getMinutes() / 60;
  const marketSession =
    estHour >= 9.5 && estHour < 16  ? 'REGULAR' :
    estHour >= 4   && estHour < 9.5 ? 'PRE'     :
    estHour >= 16  && estHour < 20  ? 'POST'    : 'CLOSED';

  const ttl = marketSession === 'REGULAR' ? 60 : 300;

  try {
    const data = await withCache(env, `vc:${symbol}:${tradeDate}`, ttl, async () => {
      const r = await fetch(
        `https://vannacharm.com/api/getMinuteSurfaces?symbol=${symbol}&trade_date=${tradeDate}`,
        { headers: { 'X-API-Key': env.VANNACHARM_KEY, 'Accept': 'application/json' }, signal: AbortSignal.timeout(15000) }
      );
      if (!r.ok) throw new Error(`VannaCharm ${r.status}`);
      const j = await r.json();
      if (!j.data || j.data.length === 0) throw new Error('EMPTY');
      return j;
    });
    return json({ ...data, _meta: { symbol, tradeDate, marketSession, cachedAt: new Date().toISOString() } });
  } catch (err) {
    if (err.message === 'EMPTY') {
      return json({ data: [], success: true, _meta: { symbol, tradeDate, marketSession } });
    }
    return json({ error: err.message, symbol }, 500);
  }
}

// ── /api/greeks — 서버에서 Vanna/Charm/GEX 계산 후 KV 캐시 ──
// 모든 클라이언트가 동일한 계산 결과를 받도록 보장
async function handleGreeks(url, env) {
  const symbol = (url.searchParams.get('symbol') || 'SPY').toUpperCase();
  const exp    = url.searchParams.get('exp') || '';
  if (!exp) return json({ error: 'exp required' }, 400);

  const nowEST = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const estHour = nowEST.getHours() + nowEST.getMinutes() / 60;
  const isMarket = estHour >= 9.5 && estHour < 16;
  const ttl = isMarket ? 60 : 300;  // 장중 60초 캐시 — 모든 유저 동일값 보장
  const todayStr = nowEST.toLocaleDateString('en-CA');
  const cacheKey = `greeks:${symbol}:${exp}:${todayStr}`;

  try {
    const data = await withCache(env, cacheKey, ttl, async () => {
      // 1. 옵션 체인 로드
      const optR = await fetch(
        `https://cdn.cboe.com/api/global/delayed_quotes/options/${symbol}.json`,
        {
          headers: {
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'application/json',
            'Referer': 'https://www.cboe.com/',
          },
          signal: AbortSignal.timeout(10000),
        }
      );
      if (!optR.ok) throw new Error(`CBOE ${optR.status}`);
      const cboeJson = await optR.ok ? await optR.json() : null;
      const spotPrice = cboeJson.data.current_price;
      const allOptions = cboeJson.data.options;

      // 2. 선택된 만기 필터링
      const expKey = exp.replace(/-/g,'').slice(2);
      const parsed = allOptions.filter(o => {
        const m = o.option.trim().match(/(\d{6})[CP]/);
        return m && m[1] === expKey;
      }).map(o => {
        const m = o.option.trim().match(/(\d{6})([CP])(\d+)/);
        if (!m) return null;
        return { strike: parseInt(m[3])/1000, type: m[2], iv: o.iv, gamma: o.gamma, oi: o.open_interest };
      }).filter(Boolean);

      // 3. 스트라이크별 집계
      const map = {};
      parsed.forEach(o => {
        if (!map[o.strike]) map[o.strike] = { strike: o.strike, callOI: 0, putOI: 0, callGamma: 0, putGamma: 0, ivSum: 0, ivN: 0 };
        const s = map[o.strike];
        if (o.type === 'C') { s.callOI += o.oi; s.callGamma = o.gamma; }
        else { s.putOI += o.oi; s.putGamma = o.gamma; }
        if (o.iv > 0) { s.ivSum += o.iv; s.ivN++; }
      });
      const strikes = Object.values(map).sort((a, b) => a.strike - b.strike);

      // 4. GEX + Vanna + Charm — BS Gamma로 통합 계산
      // CBOE Gamma는 딥OTM에서 신뢰 불가, BS로 직접 산출
      const msToExp = new Date(exp) - new Date();
      const T = Math.max(msToExp / (1000*60*60*24*365), 1/365);
      const safeT = Math.max(T, 0.5/365);
      const r = 0.045;
      let totalVanna = 0, totalCharm = 0;
      strikes.forEach(s => {
        s.iv = s.ivN > 0 ? s.ivSum / s.ivN : 0;
        const K = s.strike;
        const sigma = s.iv > 0 ? s.iv : 0.20;
        const sqrtT = Math.sqrt(T);
        const safeSqrtT = Math.sqrt(safeT);
        const lnSK = Math.log(spotPrice / K);
        const d1 = (lnSK + (r + sigma*sigma/2)*T) / (sigma * sqrtT);
        const d2 = d1 - sigma * sqrtT;
        const nd1 = Math.exp(-d1*d1/2) / Math.sqrt(2*Math.PI);
        // BS Gamma
        const bsGamma = isFinite(nd1) ? nd1 / (spotPrice * sigma * sqrtT) : 0;
        // GEX (표준 공식: callOI - putOI 기준)
        s.gex = isFinite(bsGamma) ? (s.callOI - s.putOI) * bsGamma * 100 * spotPrice : 0;
        // Vanna / Charm
        const netOI = s.callOI - s.putOI;
        const vanna = nd1 * (d2 / sigma) * netOI * 100 * spotPrice;
        totalVanna += isFinite(vanna) ? vanna : 0;
        const charm = -nd1 * (r/(sigma*safeSqrtT) - d2/(2*safeT)) * netOI * 100;
        totalCharm += isFinite(charm) ? charm : 0;
      });

      // 5. GEX 집계
      let cum = 0, flipZone = null;
      strikes.forEach(s => {
        const p = cum; cum += s.gex; s.cumGex = cum;
        if (!flipZone && ((p < 0 && cum >= 0) || (p > 0 && cum <= 0))) flipZone = s.strike;
      });
      const near = strikes.filter(s => Math.abs(s.strike - spotPrice) / spotPrice < 0.10);
      const putWall  = near.reduce((b, s) => s.putOI  > b.putOI  ? s : b, near[0])?.strike;
      const callWall = near.reduce((b, s) => s.callOI > b.callOI ? s : b, near[0])?.strike;
      const localGEX = strikes.filter(s => Math.abs(s.strike - spotPrice) / spotPrice < 0.02).reduce((a, s) => a + s.gex, 0);
      const totalCallOI = strikes.reduce((a, s) => a + s.callOI, 0);
      const totalPutOI  = strikes.reduce((a, s) => a + s.putOI,  0);
      const pcr = totalPutOI / Math.max(totalCallOI, 1);

      return {
        symbol, exp, spotPrice,
        vanna: parseFloat((totalVanna/1e6).toFixed(2)),
        charm: parseFloat((totalCharm/1e6).toFixed(2)),
        localGEX: parseFloat((localGEX/1e6).toFixed(2)),
        totalGEX: parseFloat((cum/1e6).toFixed(2)),
        flipZone, putWall, callWall, pcr: parseFloat(pcr.toFixed(3)),
        timestamp: new Date().toISOString(),
        source: 'server_computed',
      };
    });
    return json(data);
  } catch (err) {
    return json({ error: err.message, symbol, exp }, 500);
  }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 0DTE Greek 서버 계산 (Cron에서 호출)
// KV["gex0dte:{symbol}"] 에 저장 → 클라이언트는 읽기만
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function compute0DTE(env, symbol) {
  // 1. CBOE 옵션 체인 fetch
  const r = await fetch(
    `https://cdn.cboe.com/api/global/delayed_quotes/options/${symbol}.json`,
    {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Referer': 'https://www.cboe.com/',
      },
      signal: AbortSignal.timeout(12000),
    }
  );
  if (!r.ok) throw new Error(`CBOE ${r.status}`);
  const cboeJson = await r.json();
  const spotPrice = cboeJson.data.current_price;
  const allOptions = cboeJson.data.options;

  // 2. 오늘 만기(0DTE) 추출
  const nowEST = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const todayKey = `${String(nowEST.getFullYear()).slice(2)}${String(nowEST.getMonth()+1).padStart(2,'0')}${String(nowEST.getDate()).padStart(2,'0')}`;
  const todayISO = nowEST.toLocaleDateString('en-CA');  // "2026-04-11"

  const parsed = allOptions.filter(o => {
    const m = o.option.trim().match(/(\d{6})[CP]/);
    return m && m[1] === todayKey;
  }).map(o => {
    const m = o.option.trim().match(/(\d{6})([CP])(\d+)/);
    if (!m) return null;
    return { strike: parseInt(m[3])/1000, type: m[2], iv: o.iv, gamma: o.gamma, oi: o.open_interest, volume: o.volume };
  }).filter(Boolean);

  if (parsed.length === 0) throw new Error('NO_0DTE_DATA');

  // 3. 스트라이크별 집계
  const map = {};
  parsed.forEach(o => {
    if (!map[o.strike]) map[o.strike] = { strike: o.strike, callOI: 0, putOI: 0, callGamma: 0, putGamma: 0, callVol: 0, putVol: 0, ivSum: 0, ivN: 0 };
    const s = map[o.strike];
    if (o.type === 'C') { s.callOI += o.oi; s.callGamma = o.gamma; s.callVol += o.volume; }
    else                { s.putOI  += o.oi; s.putGamma  = o.gamma; s.putVol  += o.volume; }
    if (o.iv > 0) { s.ivSum += o.iv; s.ivN++; }
  });
  const strikes = Object.values(map).sort((a, b) => a.strike - b.strike);

  // 4. BS Gamma + GEX + Vanna + Charm 계산
  const msToExp = new Date(todayISO) - new Date();
  const T = Math.max(msToExp / (1000*60*60*24*365), 1/365);
  const safeT = Math.max(T, 0.5/365);
  const r_rate = 0.045;
  let totalVanna = 0, totalCharm = 0;

  strikes.forEach(s => {
    s.iv = s.ivN > 0 ? s.ivSum / s.ivN : 0;
    const K = s.strike;
    const sigma = s.iv > 0 ? s.iv : 0.20;
    const sqrtT = Math.sqrt(T);
    const safeSqrtT = Math.sqrt(safeT);
    const d1 = (Math.log(spotPrice/K) + (r_rate + sigma*sigma/2)*T) / (sigma*sqrtT);
    const d2 = d1 - sigma*sqrtT;
    const nd1 = Math.exp(-d1*d1/2) / Math.sqrt(2*Math.PI);
    const bsGamma = isFinite(nd1) ? nd1 / (spotPrice * sigma * sqrtT) : 0;

    // GEX: (callOI - putOI) × gamma × 100 × spot
    s.gex = isFinite(bsGamma) ? (s.callOI - s.putOI) * bsGamma * 100 * spotPrice : 0;
    s.callHedge = bsGamma * s.callOI * 100 * spotPrice;
    s.putHedge  = bsGamma * s.putOI  * 100 * spotPrice;

    const netOI = s.callOI - s.putOI;
    const vanna = nd1 * (d2 / sigma) * netOI * 100 * spotPrice;
    totalVanna += isFinite(vanna) ? vanna : 0;
    const charm = -nd1 * (r_rate/(sigma*safeSqrtT) - d2/(2*safeT)) * netOI * 100;
    totalCharm += isFinite(charm) ? charm : 0;
  });

  // 5. 집계 지표
  let cum = 0, flipZone = null;
  strikes.forEach(s => {
    const p = cum; cum += s.gex; s.cumGex = cum;
    if (!flipZone && ((p < 0 && cum >= 0) || (p > 0 && cum <= 0))) flipZone = s.strike;
  });
  const near = strikes.filter(s => Math.abs(s.strike - spotPrice) / spotPrice < 0.10);
  const putWall  = near.reduce((b, s) => s.putOI  > b.putOI  ? s : b, near[0])?.strike;
  const callWall = near.reduce((b, s) => s.callOI > b.callOI ? s : b, near[0])?.strike;
  const localGEX = strikes.filter(s => Math.abs(s.strike - spotPrice) / spotPrice < 0.02).reduce((a, s) => a + s.gex, 0);
  const totalCallOI = strikes.reduce((a, s) => a + s.callOI, 0);
  const totalPutOI  = strikes.reduce((a, s) => a + s.putOI,  0);
  const pcr = totalPutOI / Math.max(totalCallOI, 1);
  const upStrikes = strikes.filter(s => s.strike > spotPrice && s.strike <= spotPrice*1.05).sort((a,b) => b.callHedge - a.callHedge).slice(0,4);
  const dnStrikes = strikes.filter(s => s.strike < spotPrice && s.strike >= spotPrice*0.95).sort((a,b) => b.putHedge  - a.putHedge).slice(0,4);

  return {
    symbol,
    exp: todayISO,
    spotPrice,
    strikes,          // 전체 스트라이크 (차트용)
    upStrikes,
    dnStrikes,
    flipZone,
    putWall,
    callWall,
    localGEX: parseFloat((localGEX/1e6).toFixed(2)),
    totalGEX:  parseFloat((cum/1e6).toFixed(2)),
    vanna:     parseFloat((totalVanna/1e6).toFixed(2)),
    charm:     parseFloat((totalCharm/1e6).toFixed(2)),
    pcr:       parseFloat(pcr.toFixed(3)),
    computedAt: new Date().toISOString(),
    source: 'cron_computed',
  };
}

// ── /api/gex0dte — KV 읽기 전용 (클라이언트용) ──
async function handleGex0DTE(url, env) {
  const symbol = (url.searchParams.get('symbol') || 'SPY').toUpperCase();
  if (!/^[\^_A-Z0-9]{1,10}$/.test(symbol)) return json({ error: 'Invalid symbol' }, 400);

  try {
    const cached = await env.CACHE.get(`gex0dte:${symbol}`);
    if (cached) {
      return json(JSON.parse(cached));
    }
    // KV에 없으면 즉석 계산 (Cron 아직 미실행 상태)
    const data = await compute0DTE(env, symbol);
    await env.CACHE.put(`gex0dte:${symbol}`, JSON.stringify(data), { expirationTtl: 600 });
    return json({ ...data, source: 'on_demand_computed' });
  } catch (err) {
    return json({ error: err.message, symbol }, 500);
  }
}

// 25002500 DB Cd08Ae30D654 25002500
import { handleInitDb } from "./init-db.js";

// 25002500 Screener API D578B4e4B7ec 25002500
import {
  getActiveSymbols,
  fetchOptionsData,
  saveOptionsFlow,
  updateBaseline,
  getScreenerResults
} from './screener.js';

async function handleScreener(url, env) {
  const date = url.searchParams.get('date') || null;
  const type = url.searchParams.get('type') || 'all'; // 'etf' | 'stock' | 'all'

  try {
    let results = await getScreenerResults(env.DB, date);

    // 타입 필터
    if (type !== 'all') {
      results = results.filter(r => r.type === type);
    }

    // 섹터별 그룹핑
    const sectors = {};
    for (const row of results) {
      if (!sectors[row.sector]) sectors[row.sector] = { etf: [], stocks: [] };
      if (row.type === 'etf') sectors[row.sector].etf.push(row);
      else sectors[row.sector].stocks.push(row);
    }

    return json({ date: date || 'today', sectors, total: results.length });
  } catch (err) {
    return json({ error: err.message }, 500);
  }
}

// ── 메인 라우터 ──
export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: CORS });
    }

    if (url.pathname === '/api/options')    return handleOptions(url, env);
    if (url.pathname === '/api/price')      return handlePrice(url, env);
    if (url.pathname === '/api/vix')        return handleVix(url, env);
    if (url.pathname === '/api/vannacharm') return handleVannaCharm(url, env);
    if (url.pathname === '/api/greeks')     return handleGreeks(url, env);
    if (url.pathname === '/api/gex0dte')    return handleGex0DTE(url, env);
    if (url.pathname === '/api/screener')   return handleScreener(url, env);
    if (url.pathname === '/api/init-db')    return handleInitDb(url, env);

    return json({ error: 'Not found' }, 404);
  },

  // ── Cron 핸들러 ──
  async scheduled(event, env, ctx) {
    const cron = event.cron;

    // 1) 장 마감 후 스크리너 데이터 수집 (21:00 UTC = 17:00 EST)
    if (cron === '0 21 * * 1-5') {
      console.log('[Cron] 스크리너 데이터 수집 시작');
      try {
        const symbols = await getActiveSymbols(env.DB);
        let total = 0;

        for (const sym of symbols) {
          try {
            const records = await fetchOptionsData(sym.symbol);
            if (records?.length) {
              await saveOptionsFlow(env.DB, records);
              await updateBaseline(env.DB, sym.symbol);
              total += records.length;
              console.log(`[Screener] ${sym.symbol}: ${records.length}개 만기 저장`);
            }
            // API 과부하 방지
            await new Promise(r => setTimeout(r, 300));
          } catch (err) {
            console.error(`[Screener] ${sym.symbol} 실패:`, err.message);
          }
        }
        console.log(`[Cron] 스크리너 완료: 총 ${total}개 레코드`);
      } catch (err) {
        console.error('[Cron] 스크리너 전체 실패:', err.message);
      }
      return;
    }

    // 2) 장 중 5분마다 0DTE 계산 (기존 로직)
    const symbols = ['SPY', 'QQQ'];
    for (const sym of symbols) {
      try {
        const data = await compute0DTE(env, sym);
        await env.CACHE.put(`gex0dte:${sym}`, JSON.stringify(data), { expirationTtl: 600 });
        console.log(`[Cron] ${sym} 0DTE computed: flipZone=${data.flipZone} localGEX=${data.localGEX}M`);
      } catch (err) {
        console.error(`[Cron] ${sym} failed: ${err.message}`);
      }
    }
  },
};
