// ============================================
// DB 초기화 엔드포인트
// GET /api/init-db?secret=YOUR_SECRET
// 한 번만 실행하면 됨 — 이후 삭제 가능
// ============================================

export async function handleInitDb(url, env) {
  // 간단한 보안 체크 (환경변수 INIT_SECRET 설정 권장)
  const secret = url.searchParams.get('secret');
  const expected = env.INIT_SECRET || 'drbalance-init-2026';
  if (secret !== expected) {
    return new Response(JSON.stringify({ error: 'Unauthorized' }), {
      status: 401,
      headers: { 'Content-Type': 'application/json' }
    });
  }

  const results = [];

  // SQL 문을 개별로 실행
  const statements = [

    // 1. symbols 테이블
    `CREATE TABLE IF NOT EXISTS symbols (
      symbol      TEXT PRIMARY KEY,
      name        TEXT NOT NULL,
      type        TEXT NOT NULL,
      sector      TEXT NOT NULL,
      sector_etf  TEXT,
      is_active   INTEGER DEFAULT 1,
      added_date  TEXT DEFAULT (date('now'))
    )`,

    // 2. options_flow 테이블
    `CREATE TABLE IF NOT EXISTS options_flow (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      date        TEXT NOT NULL,
      symbol      TEXT NOT NULL,
      expiry_date TEXT NOT NULL,
      dte         INTEGER NOT NULL,
      call_vol    INTEGER DEFAULT 0,
      put_vol     INTEGER DEFAULT 0,
      call_oi     INTEGER DEFAULT 0,
      put_oi      INTEGER DEFAULT 0,
      pcr_vol     REAL,
      pcr_oi      REAL,
      atm_iv      REAL,
      otm_call_iv REAL,
      otm_put_iv  REAL,
      stock_vol   INTEGER DEFAULT 0,
      UNIQUE(date, symbol, expiry_date)
    )`,

    // 3. options_baseline 테이블
    `CREATE TABLE IF NOT EXISTS options_baseline (
      symbol          TEXT PRIMARY KEY,
      updated_date    TEXT NOT NULL,
      avg_call_vol    REAL DEFAULT 0,
      avg_put_vol     REAL DEFAULT 0,
      avg_stock_vol   REAL DEFAULT 0,
      avg_call_oi     REAL DEFAULT 0,
      avg_put_oi      REAL DEFAULT 0,
      avg_pcr_vol     REAL DEFAULT 1.0,
      avg_pcr_oi      REAL DEFAULT 1.0,
      avg_atm_iv      REAL DEFAULT 0,
      avg_otm_call_iv REAL DEFAULT 0,
      avg_otm_put_iv  REAL DEFAULT 0,
      std_call_vol    REAL DEFAULT 1,
      std_call_oi     REAL DEFAULT 1,
      std_pcr_oi      REAL DEFAULT 0.1,
      std_otm_call_iv REAL DEFAULT 0.01
    )`,

    // 4. 인덱스
    `CREATE INDEX IF NOT EXISTS idx_flow_date_symbol
      ON options_flow(date, symbol)`,

    `CREATE INDEX IF NOT EXISTS idx_flow_symbol_expiry
      ON options_flow(symbol, expiry_date)`,

    `CREATE INDEX IF NOT EXISTS idx_flow_date_dte
      ON options_flow(date, dte)`,

    // 5. 초기 종목 — ETF
    `INSERT OR IGNORE INTO symbols VALUES
      ('SPY',  'SPDR S&P 500 ETF',          'etf', 'broad_market',  NULL,  1, date('now')),
      ('QQQ',  'Invesco QQQ Trust',          'etf', 'technology',    NULL,  1, date('now')),
      ('XLK',  'Technology Select Sector',   'etf', 'technology',    NULL,  1, date('now')),
      ('XLE',  'Energy Select Sector',       'etf', 'energy',        NULL,  1, date('now')),
      ('XLF',  'Financial Select Sector',    'etf', 'financial',     NULL,  1, date('now')),
      ('XLV',  'Health Care Select Sector',  'etf', 'healthcare',    NULL,  1, date('now')),
      ('XLI',  'Industrial Select Sector',   'etf', 'industrial',    NULL,  1, date('now')),
      ('XLU',  'Utilities Select Sector',    'etf', 'utilities',     NULL,  1, date('now')),
      ('XLP',  'Consumer Staples Sector',    'etf', 'staples',       NULL,  1, date('now')),
      ('XLY',  'Consumer Discretionary',     'etf', 'discretionary', NULL,  1, date('now'))`,

    // 6. 초기 종목 — 개별주 (기술)
    `INSERT OR IGNORE INTO symbols VALUES
      ('AAPL', 'Apple Inc.',                 'stock', 'technology',    'XLK', 1, date('now')),
      ('MSFT', 'Microsoft Corp.',            'stock', 'technology',    'XLK', 1, date('now')),
      ('NVDA', 'NVIDIA Corp.',               'stock', 'technology',    'XLK', 1, date('now')),
      ('GOOGL','Alphabet Inc.',              'stock', 'technology',    'XLK', 1, date('now')),
      ('META', 'Meta Platforms Inc.',        'stock', 'technology',    'XLK', 1, date('now')),
      ('AMD',  'Advanced Micro Devices',     'stock', 'technology',    'XLK', 1, date('now')),
      ('AVGO', 'Broadcom Inc.',              'stock', 'technology',    'XLK', 1, date('now'))`,

    // 7. 초기 종목 — 에너지/금융/헬스/산업/소비
    `INSERT OR IGNORE INTO symbols VALUES
      ('XOM',  'Exxon Mobil Corp.',          'stock', 'energy',        'XLE', 1, date('now')),
      ('OXY',  'Occidental Petroleum',       'stock', 'energy',        'XLE', 1, date('now')),
      ('CVX',  'Chevron Corp.',              'stock', 'energy',        'XLE', 1, date('now')),
      ('JPM',  'JPMorgan Chase & Co.',       'stock', 'financial',     'XLF', 1, date('now')),
      ('BAC',  'Bank of America Corp.',      'stock', 'financial',     'XLF', 1, date('now')),
      ('GS',   'Goldman Sachs Group',        'stock', 'financial',     'XLF', 1, date('now')),
      ('UNH',  'UnitedHealth Group',         'stock', 'healthcare',    'XLV', 1, date('now')),
      ('LLY',  'Eli Lilly and Company',      'stock', 'healthcare',    'XLV', 1, date('now')),
      ('CAT',  'Caterpillar Inc.',           'stock', 'industrial',    'XLI', 1, date('now')),
      ('GE',   'GE Aerospace',              'stock', 'industrial',    'XLI', 1, date('now')),
      ('WMT',  'Walmart Inc.',              'stock', 'staples',       'XLP', 1, date('now')),
      ('KO',   'Coca-Cola Company',         'stock', 'staples',       'XLP', 1, date('now')),
      ('TSLA', 'Tesla Inc.',                'stock', 'discretionary', 'XLY', 1, date('now'))`,

  ];

  // 순서대로 실행
  for (const sql of statements) {
    try {
      await env.DB.prepare(sql).run();
      results.push({ ok: true, sql: sql.trim().slice(0, 60) + '...' });
    } catch (err) {
      results.push({ ok: false, sql: sql.trim().slice(0, 60) + '...', error: err.message });
    }
  }

  // 결과 확인
  const tables = await env.DB.prepare(
    `SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`
  ).all();

  const symbolCount = await env.DB.prepare(
    `SELECT COUNT(*) as cnt FROM symbols`
  ).first();

  return new Response(JSON.stringify({
    success: true,
    tables: tables.results.map(r => r.name),
    symbol_count: symbolCount.cnt,
    steps: results
  }, null, 2), {
    headers: { 'Content-Type': 'application/json' }
  });
}
