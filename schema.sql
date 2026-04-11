-- ============================================
-- Options Screener Database Schema
-- Cloudflare D1 (SQLite)
-- ============================================

-- 1. 종목 메타데이터
CREATE TABLE IF NOT EXISTS symbols (
  symbol        TEXT PRIMARY KEY,
  name          TEXT NOT NULL,
  type          TEXT NOT NULL CHECK(type IN ('stock', 'etf')),
  sector        TEXT NOT NULL,
  sector_etf    TEXT,              -- 개별 종목의 경우 해당 섹터 ETF (예: XLK)
  is_active     INTEGER DEFAULT 1, -- 1=모니터링 중, 0=제외
  added_date    TEXT DEFAULT (date('now'))
);

-- 2. 만기별 일간 옵션 플로우 데이터
CREATE TABLE IF NOT EXISTS options_flow (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  date          TEXT NOT NULL,     -- 수집일 (YYYY-MM-DD)
  symbol        TEXT NOT NULL,
  expiry_date   TEXT NOT NULL,     -- 실제 만기일 (YYYY-MM-DD)
  dte           INTEGER NOT NULL,  -- 수집일 기준 잔존일수

  -- 거래량
  call_vol      INTEGER DEFAULT 0,
  put_vol       INTEGER DEFAULT 0,

  -- Open Interest
  call_oi       INTEGER DEFAULT 0,
  put_oi        INTEGER DEFAULT 0,

  -- Put/Call Ratio
  pcr_vol       REAL,              -- Volume 기준 PCR
  pcr_oi        REAL,              -- OI 기준 PCR

  -- Implied Volatility
  atm_iv        REAL,              -- ATM IV
  otm_call_iv   REAL,              -- OTM Call IV (skew용)
  otm_put_iv    REAL,              -- OTM Put IV (skew용)

  -- 현물
  stock_vol     INTEGER DEFAULT 0, -- 당일 현물 거래량

  FOREIGN KEY (symbol) REFERENCES symbols(symbol),
  UNIQUE(date, symbol, expiry_date)
);

-- 3. 종목별 20일 Rolling 기준값
CREATE TABLE IF NOT EXISTS options_baseline (
  symbol          TEXT PRIMARY KEY,
  updated_date    TEXT NOT NULL,   -- 마지막 업데이트일

  -- 거래량 기준
  avg_call_vol    REAL DEFAULT 0,
  avg_put_vol     REAL DEFAULT 0,
  avg_stock_vol   REAL DEFAULT 0,

  -- OI 기준
  avg_call_oi     REAL DEFAULT 0,
  avg_put_oi      REAL DEFAULT 0,

  -- PCR 기준
  avg_pcr_vol     REAL DEFAULT 1.0,
  avg_pcr_oi      REAL DEFAULT 1.0,

  -- IV 기준
  avg_atm_iv      REAL DEFAULT 0,
  avg_otm_call_iv REAL DEFAULT 0,
  avg_otm_put_iv  REAL DEFAULT 0,

  -- 표준편차 (이상 감지용 z-score 계산)
  std_call_vol    REAL DEFAULT 1,
  std_call_oi     REAL DEFAULT 1,
  std_pcr_oi      REAL DEFAULT 0.1,
  std_otm_call_iv REAL DEFAULT 0.01,

  FOREIGN KEY (symbol) REFERENCES symbols(symbol)
);

-- ============================================
-- 인덱스
-- ============================================
CREATE INDEX IF NOT EXISTS idx_flow_date_symbol
  ON options_flow(date, symbol);

CREATE INDEX IF NOT EXISTS idx_flow_symbol_expiry
  ON options_flow(symbol, expiry_date);

CREATE INDEX IF NOT EXISTS idx_flow_date_dte
  ON options_flow(date, dte);

-- ============================================
-- 초기 종목 데이터 (섹터 ETF 먼저)
-- ============================================
INSERT OR IGNORE INTO symbols VALUES
  -- 섹터 ETF
  ('SPY',  'SPDR S&P 500 ETF',            'etf',   'broad_market', NULL, 1, date('now')),
  ('QQQ',  'Invesco QQQ Trust',           'etf',   'technology',   NULL, 1, date('now')),
  ('XLK',  'Technology Select Sector',    'etf',   'technology',   NULL, 1, date('now')),
  ('XLE',  'Energy Select Sector',        'etf',   'energy',       NULL, 1, date('now')),
  ('XLF',  'Financial Select Sector',     'etf',   'financial',    NULL, 1, date('now')),
  ('XLV',  'Health Care Select Sector',   'etf',   'healthcare',   NULL, 1, date('now')),
  ('XLI',  'Industrial Select Sector',    'etf',   'industrial',   NULL, 1, date('now')),
  ('XLU',  'Utilities Select Sector',     'etf',   'utilities',    NULL, 1, date('now')),
  ('XLP',  'Consumer Staples Sector',     'etf',   'staples',      NULL, 1, date('now')),
  ('XLY',  'Consumer Discretionary',      'etf',   'discretionary',NULL, 1, date('now')),

  -- 기술 섹터
  ('AAPL', 'Apple Inc.',                  'stock', 'technology',   'XLK', 1, date('now')),
  ('MSFT', 'Microsoft Corp.',             'stock', 'technology',   'XLK', 1, date('now')),
  ('NVDA', 'NVIDIA Corp.',                'stock', 'technology',   'XLK', 1, date('now')),
  ('GOOGL','Alphabet Inc.',               'stock', 'technology',   'XLK', 1, date('now')),
  ('META', 'Meta Platforms Inc.',         'stock', 'technology',   'XLK', 1, date('now')),
  ('AMD',  'Advanced Micro Devices',      'stock', 'technology',   'XLK', 1, date('now')),
  ('AVGO', 'Broadcom Inc.',               'stock', 'technology',   'XLK', 1, date('now')),

  -- 에너지 섹터
  ('XOM',  'Exxon Mobil Corp.',           'stock', 'energy',       'XLE', 1, date('now')),
  ('OXY',  'Occidental Petroleum',        'stock', 'energy',       'XLE', 1, date('now')),
  ('CVX',  'Chevron Corp.',               'stock', 'energy',       'XLE', 1, date('now')),

  -- 금융 섹터
  ('JPM',  'JPMorgan Chase & Co.',        'stock', 'financial',    'XLF', 1, date('now')),
  ('BAC',  'Bank of America Corp.',       'stock', 'financial',    'XLF', 1, date('now')),
  ('GS',   'Goldman Sachs Group',         'stock', 'financial',    'XLF', 1, date('now')),

  -- 헬스케어
  ('UNH',  'UnitedHealth Group',          'stock', 'healthcare',   'XLV', 1, date('now')),
  ('LLY',  'Eli Lilly and Company',       'stock', 'healthcare',   'XLV', 1, date('now')),

  -- 산업재
  ('CAT',  'Caterpillar Inc.',            'stock', 'industrial',   'XLI', 1, date('now')),
  ('GE',   'GE Aerospace',               'stock', 'industrial',   'XLI', 1, date('now')),

  -- 필수소비재
  ('WMT',  'Walmart Inc.',               'stock', 'staples',      'XLP', 1, date('now')),
  ('KO',   'Coca-Cola Company',          'stock', 'staples',      'XLP', 1, date('now')),

  -- 테슬라 (별도)
  ('TSLA', 'Tesla Inc.',                 'stock', 'discretionary','XLY', 1, date('now'));
