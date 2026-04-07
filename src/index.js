// GEX Dashboard - Cloudflare Workers
// 엔드포인트: /api/options, /api/price, /api/vix
// KV 캐싱으로 3명 동시 접속 시에도 외부 API 중복 호출 없음

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
  // KV에서 캐시 확인
  try {
    const cached = await env.CACHE.get(key);
    if (cached) return JSON.parse(cached);
  } catch (_) {}

  // 캐시 없으면 실제 fetch
  const data = await fetcher();

  // 결과 저장 (ttl 초)
  try {
    await env.CACHE.put(key, JSON.stringify(data), { expirationTtl: ttl });
  } catch (_) {}

  return data;
}

// ── /api/options ──
async function handleOptions(url, env) {
  const symbol = (url.searchParams.get('symbol') || 'SPY').toUpperCase();
  if (!/^[\^_A-Z0-9]{1,10}$/.test(symbol)) {
    return json({ error: 'Invalid symbol' }, 400);
  }

  const cacheKey = `options:${symbol}`;

  try {
    const data = await withCache(env, cacheKey, 30, async () => {
      const upstream = await fetch(
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
      if (!upstream.ok) throw new Error(`CBOE ${upstream.status}`);
      return upstream.json();
    });

    return json(data);
  } catch (err) {
    return json({ error: err.message, symbol }, 500);
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
      const json_ = await r.json();
      const meta = json_?.chart?.result?.[0]?.meta;
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
        symbol,
        price,
        priceLabel,
        marketState,
        regularPrice:  meta.regularMarketPrice,
        prevClose:     meta.chartPreviousClose,
        preMarketPrice:  meta.preMarketPrice  ?? null,
        postMarketPrice: meta.postMarketPrice ?? null,
        preMarketChangePercent:  meta.preMarketChangePercent  ?? null,
        postMarketChangePercent: meta.postMarketChangePercent ?? null,
        preMarketTime:  meta.preMarketTime  ? new Date(meta.preMarketTime  * 1000).toISOString() : null,
        postMarketTime: meta.postMarketTime ? new Date(meta.postMarketTime * 1000).toISOString() : null,
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
    const data = await withCache(env, cacheKey, 60, async () => {
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
          const json_ = await r.json();
          const meta = json_?.chart?.result?.[0]?.meta;
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

// ── /api/vannacharm ──
// VannaCharm Premium API 프록시 + KV 캐싱
// ?symbol=SPY&type=summary|gex|vex|chex|levels
// 3명이 동시 접속해도 KV 캐시로 1회만 호출
async function handleVannaCharm(url, env) {
  const symbol = (url.searchParams.get('symbol') || 'SPY').toUpperCase();
  const type   = url.searchParams.get('type') || 'summary'; // summary | gex | vex | chex | levels

  // 엔드포인트 매핑
  const endpointMap = {
    summary: `exposure/summary/${symbol}`,  // GEX+DEX+VEX+CHEX 전체 요약
    gex:     `exposure/gex/${symbol}`,       // 스트라이크별 GEX
    vex:     `exposure/vex/${symbol}`,       // Vanna Exposure
    chex:    `exposure/chex/${symbol}`,      // Charm Exposure
    levels:  `exposure/levels/${symbol}`,    // Flip Zone, Call/Put Wall, 0DTE magnet
  };

  const endpoint = endpointMap[type];
  if (!endpoint) return json({ error: 'Invalid type' }, 400);

  // 캐시 TTL: summary/levels는 5분, gex/vex/chex는 3분
  const ttl = (type === 'summary' || type === 'levels') ? 300 : 180;
  const cacheKey = `vc:${type}:${symbol}`;

  try {
    const data = await withCache(env, cacheKey, ttl, async () => {
      const r = await fetch(
        `https://lab.flashalpha.com/v1/${endpoint}`,
        {
          headers: {
            'X-Api-Key': env.VANNACHARM_KEY,
            'Accept': 'application/json',
          },
          signal: AbortSignal.timeout(10000),
        }
      );
      if (!r.ok) throw new Error(`VannaCharm ${r.status}: ${await r.text()}`);
      return r.json();
    });

    // 마켓 상태 병기 (프리마켓 여부 판단용)
    const now = new Date();
    const estOffset = -5; // EST (서머타임 -4, 겨울 -5 — 단순화)
    const estHour = (now.getUTCHours() + 24 + estOffset) % 24;
    const estMin  = now.getUTCMinutes();
    const estTime = estHour + estMin / 60;

    let marketSession;
    if      (estTime >= 4   && estTime < 9.5)  marketSession = 'PRE';
    else if (estTime >= 9.5 && estTime < 16)   marketSession = 'REGULAR';
    else if (estTime >= 16  && estTime < 20)   marketSession = 'POST';
    else                                        marketSession = 'CLOSED';

    return json({ ...data, _meta: { symbol, type, marketSession, cachedAt: new Date().toISOString() } });
  } catch (err) {
    return json({ error: err.message, symbol, type }, 500);
  }
}

// ── 메인 라우터 ──
export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // CORS preflight
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
