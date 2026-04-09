// GEX Dashboard - Cloudflare Workers v2
// 엔드포인트:
//   /api/options   → Market Data App 실시간 옵션 체인 (CBOE 폴백)
//   /api/price     → Yahoo Finance 현재가
//   /api/vix       → Yahoo Finance VIX/VVIX
//   /api/vannacharm → VannaCharm API (기존 유지)

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};

function json(data, status = 200, extra = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json', ...CORS, ...extra },
  });
}

// ── KV 캐시 헬퍼 ──
async function withCache(env, key, ttl, fetcher) {
  try {
    const cached = await env.CACHE.get(key);
    if (cached) return JSON.parse(cached);
  } catch (_) {}

  const data = await fetcher();

  try {
    await env.CACHE.put(key, JSON.stringify(data), { expirationTtl: ttl });
  } catch (_) {}

  return data;
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// /api/options — Market Data App 실시간 옵션 체인
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function handleOptions(url, env) {
  const symbol = (url.searchParams.get('symbol') || 'SPY').toUpperCase();
  if (!/^[\^_A-Z0-9]{1,10}$/.test(symbol)) {
    return json({ error: 'Invalid symbol' }, 400);
  }

  // CBOE 심볼 처리 (_SPX → SPX)
  const mdSymbol = symbol.replace('_', '');

  // EST 기준 현재 시각
  const nowEST = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const estHour = nowEST.getHours() + nowEST.getMinutes() / 60;
  const isMarket = estHour >= 9.5 && estHour < 16;

  // 장중 30초 캐시, 그 외 5분 캐시
  const ttl = isMarket ? 30 : 300;
  const todayStr = nowEST.toLocaleDateString('en-CA');
  const cacheKey = `md2:options:${mdSymbol}:${todayStr}`;

  try {
    const data = await withCache(env, cacheKey, ttl, async () => {

      // ── Step 1: 현재가 조회 ──
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

      // ── Step 2: 옵션 체인 조회 ──
      // expiration=all: 전체 만기, 전체 스트라이크
      // strikeLimit 없음 → Flip Zone, Put/Call Wall 정확하게 계산
      // 자동갱신은 현재가+VIX만 조회하므로 크레딧 추가 소모 없음
      // 유료 전환 후 mode=cached 추가 시 크레딧 대폭 절약 가능
      const chainUrl = `https://api.marketdata.app/v1/options/chain/${mdSymbol}/` +
        `?expiration=all`;

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
        throw new Error(`MarketData API error: ${mdJson.errmsg || JSON.stringify(mdJson)}`);
      }

      // ── columnar 응답 → 옵션 배열 변환 ──
      const {
        optionSymbol, expiration, strike, side,
        bid, ask, last, volume, openInterest,
        iv, delta, gamma, theta, vega
      } = mdJson;

      const count = optionSymbol?.length || 0;
      if (count === 0) throw new Error('EMPTY');

      const options = [];
      for (let i = 0; i < count; i++) {
        // Market Data App은 OCC 심볼을 그대로 반환
        // 예: SPY260409C00675000
        options.push({
          option:        optionSymbol?.[i]   || '',
          iv:            iv?.[i]             || 0,
          gamma:         gamma?.[i]          || 0,
          delta:         delta?.[i]          || 0,
          theta:         theta?.[i]          || 0,
          vega:          vega?.[i]           || 0,
          open_interest: openInterest?.[i]   || 0,
          volume:        volume?.[i]         || 0,
          bid:           bid?.[i]            || 0,
          ask:           ask?.[i]            || 0,
          last:          last?.[i]           || 0,
        });
      }

      return {
        data: {
          current_price: currentPrice,
          options,
        },
        timestamp: new Date().toISOString(),
        source: 'marketdata.app',
      };
    });

    return json(data);

  } catch (err) {
    // Market Data App 실패 시 CBOE 폴백
    console.error(`MarketData failed (${err.message}), falling back to CBOE`);
    return handleOptionsCBOE(url, env, symbol);
  }
}

// ── CBOE 폴백 ──
async function handleOptionsCBOE(url, env, symbol) {
  const cacheKey = `cboe:${symbol}`;
  try {
    const data = await withCache(env, cacheKey, 60, async () => {
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
        source: 'cboe_fallback',
        timestamp: new Date().toISOString(),
      };
    });
    return json(data);
  } catch (err) {
    return json({ error: err.message, symbol, source: 'error' }, 500);
  }
}

// ── /api/price ──
async function handlePrice(url, env) {
  const symbol = (url.searchParams.get('symbol') || 'SPY').toUpperCase();
  if (!/^[A-Z]{1,10}$/.test(symbol)) {
    return json({ error: 'Invalid symbol' }, 400);
  }

  const cacheKey = `price:${symbol}`;

  try {
    const data = await withCache(env, cacheKey, 15, async () => {
      const r = await fetch(
        `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}?interval=1m&range=1d`,
        {
          headers: { 'User-Agent': 'Mozilla/5.0' },
          signal: AbortSignal.timeout(8000),
        }
      );
      const j = await r.json();
      const meta = j?.chart?.result?.[0]?.meta;
      if (!meta) throw new Error('No meta in response');

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

// ── /api/vix ──
async function handleVix(url, env) {
  const symbols = (url.searchParams.get('symbols') || 'VIX,VVIX')
    .split(',').slice(0, 5).map(s => s.trim().toUpperCase());

  const cacheKey = `vix:${symbols.join(',')}`;

  try {
    const data = await withCache(env, cacheKey, 15, async () => {
      const results = {};
      await Promise.all(symbols.map(async (sym) => {
        try {
          const r = await fetch(
            `https://query1.finance.yahoo.com/v8/finance/chart/%5E${sym}?interval=1m&range=1d`,
            {
              headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json' },
              signal: AbortSignal.timeout(8000),
            }
          );
          const j = await r.json();
          const meta = j?.chart?.result?.[0]?.meta;
          if (!meta) throw new Error('No meta');
          results[sym] = {
            price: meta.regularMarketPrice,
            prevClose: meta.chartPreviousClose,
            pctChange: meta.regularMarketPrice && meta.chartPreviousClose
              ? +((meta.regularMarketPrice - meta.chartPreviousClose) / meta.chartPreviousClose * 100).toFixed(2)
              : null,
          };
        } catch (e) {
          results[sym] = { error: e.message };
        }
      }));
      return results;
    });

    return json(data);
  } catch (err) {
    return json({ error: err.message }, 500);
  }
}

// ── /api/vannacharm (기존 유지) ──
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
  const cacheKey = `vc:${symbol}:${tradeDate}`;

  try {
    const data = await withCache(env, cacheKey, ttl, async () => {
      const vcUrl = `https://vannacharm.com/api/getMinuteSurfaces?symbol=${symbol}&trade_date=${tradeDate}`;
      const r = await fetch(vcUrl, {
        headers: { 'X-API-Key': env.VANNACHARM_KEY, 'Accept': 'application/json' },
        signal: AbortSignal.timeout(15000),
      });
      if (!r.ok) throw new Error(`VannaCharm ${r.status}: ${await r.text()}`);
      const j = await r.json();
      if (!j.data || j.data.length === 0) throw new Error('EMPTY');
      return j;
    });

    return json({ ...data, _meta: { symbol, tradeDate, marketSession, cachedAt: new Date().toISOString() } });
  } catch (err) {
    if (err.message === 'EMPTY') {
      return json({ data: [], success: true, _meta: { symbol, tradeDate, marketSession, cachedAt: new Date().toISOString() } });
    }
    return json({ error: err.message, symbol }, 500);
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

    return json({ error: 'Not found' }, 404);
  },
};
