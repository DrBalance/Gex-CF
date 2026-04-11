# D1 Options Screener 초기화 가이드

## 1단계: D1 데이터베이스 생성

```bash
cd Gex-CF
npx wrangler d1 create options-screener
```

출력 예시:
```
✅ Successfully created DB 'options-screener'
[[d1_databases]]
binding = "DB"
database_name = "options-screener"
database_id = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"  ← 이 ID를 복사
```

## 2단계: wrangler.toml 업데이트

`wrangler.toml`의 `database_id = "placeholder"` 부분을
위에서 복사한 실제 ID로 교체:

```toml
[[d1_databases]]
binding = "DB"
database_name = "options-screener"
database_id = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"  ← 실제 ID
```

## 3단계: 테이블 생성

```bash
# 로컬 테스트용
npx wrangler d1 execute options-screener --local --file=schema.sql

# 실제 D1에 적용
npx wrangler d1 execute options-screener --file=schema.sql
```

## 4단계: 종목 데이터 확인

```bash
npx wrangler d1 execute options-screener \
  --command="SELECT type, sector, COUNT(*) as cnt FROM symbols GROUP BY type, sector"
```

예상 출력:
```
type   | sector         | cnt
-------|----------------|----
etf    | broad_market   | 1
etf    | technology     | 2
etf    | energy         | 1
...
stock  | technology     | 7
stock  | energy         | 3
...
```

## 5단계: 배포

```bash
npx wrangler deploy
```

## 6단계: 수동 수집 테스트

```bash
# Cron 수동 실행 (로컬)
npx wrangler dev --test-scheduled
# 다른 터미널에서:
curl "http://localhost:8787/__scheduled?cron=0+21+*+*+1-5"
```

## 스크리너 API 확인

```bash
# 배포 후
curl "https://gexcf.workers.dev/api/screener"
curl "https://gexcf.workers.dev/api/screener?type=etf"
curl "https://gexcf.workers.dev/api/screener?type=stock"
```

## 유용한 D1 쿼리

```bash
# 수집된 데이터 확인
npx wrangler d1 execute options-screener \
  --command="SELECT date, symbol, COUNT(*) as expiries FROM options_flow GROUP BY date, symbol ORDER BY date DESC LIMIT 20"

# 이상 신호 직접 확인
npx wrangler d1 execute options-screener \
  --command="SELECT f.symbol, f.expiry_date, f.dte, f.call_oi, b.avg_call_oi FROM options_flow f JOIN options_baseline b USING(symbol) WHERE f.date = date('now') ORDER BY f.call_oi DESC LIMIT 10"
```
