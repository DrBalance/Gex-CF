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

// ── 메인 라우터 ──
export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: CORS });
    }

    if (url.pathname === '/api/options') return handleOptions(url, env);
    if (url.pathname === '/api/price')   return handlePrice(url, env);
    if (url.pathname === '/api/vix')     return handleVix(url, env);

    return json({ error: 'Not found' }, 404);
  },
};
