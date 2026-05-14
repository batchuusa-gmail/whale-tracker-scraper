# ApexTrader — Trading Strategy Reference

Last updated: 2026-05-13

---

## Overview

Three independent engines run in parallel on Railway:

| Engine | File | Asset | Status |
|--------|------|-------|--------|
| Stock scanner + executor | `intraday_scanner.py` + `intraday_executor.py` | Equities | ✅ Live |
| Crypto scanner + executor | `crypto_scanner.py` + `crypto_executor.py` | Crypto | ✅ Live |
| Options engine | `options_scanner.py` + `options_executor.py` | Options | ✅ Live |

All engines trade **paper only** via Alpaca paper endpoint.

---

## 1. Stock Engine

### Universe
- 54-stock candidate pool (large/mid-cap liquid names)
- Each morning, top 25 ranked by **dollar volume** (price × volume)
- Filtered by ATR%: must be between **1.5% and 4%** daily ATR
  - Below 1.5%: too slow, not enough movement to profit
  - Above 4%: too volatile, stops get blown out

### Entry Signal (all 4 must be true)

| Condition | Rule |
|-----------|------|
| **EMA alignment** | EMA8 > EMA21 > EMA50 (bullish stack) |
| **RSI zone** | RSI(14) between 45–65 (trending, not overbought) |
| **Volume surge** | Current volume ≥ 1.5× 20-day average volume |
| **Pullback entry** | Price within 1.5% of EMA21 (buy the dip, not the breakout) |

> **Why pullback entry?** Buying near EMA21 gives better risk/reward. If the stock is 5% above EMA21 it's already extended — buying there means your stop is far away and the reward is small.

### Position Sizing

- **Per trade:** 15% of non-marginable buying power, hard capped at **$15,000**
- **Max simultaneous positions:** 25
- **Max trades per day:** 6 (per scan cycle, not accumulated)

### Exit Rules (checked every 30 seconds)

| Trigger | Action |
|---------|--------|
| Price ≤ **Stop** (1.5× ATR below entry) | Full close — market order |
| Price ≥ **Target 1** (1× ATR above entry) | Sell **50%** of shares, hold rest |
| Price ≥ **Target 2** (2× ATR above entry) | Full close remaining shares |

#### Example (ATR = $2, entry = $100, position = $15,000 = 150 shares)
```
Stop:     $97.00  → loss  = -$450  (-3%)
Target 1: $102.00 → sell 75 shares, lock +$150, hold 75 shares
Target 2: $104.00 → sell 75 shares, lock +$300
Total win if T2 hit: +$450 (+3% on full position)
```

### What does NOT trigger a sell
- **Time** — no time stop, no end-of-day forced close
- **Overnight** — positions held if above stop at market close
- **Circuit breaker** — disabled in stock engine

### Operating Hours
- Scanner active: **7:00 AM – 3:45 PM ET**
- Extended hours orders: `extended_hours=True`, `time_in_force=day`
- Scan frequency: every **60 seconds**
- Position monitor: every **30 seconds**

### Config (stored in Supabase `algo_settings` table)

| Parameter | Value | Notes |
|-----------|-------|-------|
| `min_score` | 60 | Out of 100 |
| `min_confidence` | 65% | Minimum signal confidence |
| `max_positions` | 25 | Max simultaneous open positions |
| `max_trades_per_day` | 6 | Per scan cycle |
| `position_size_pct` | 0.15 | 15% of buying power (set in code, no DB column yet) |
| `circuit_breaker_enabled` | true | Daily drawdown guard |

> **Note:** `position_size_pct` is hardcoded as default in `intraday_scanner.py` because `algo_settings` table doesn't have that column yet. To change it, edit the constant and redeploy.

---

## 2. Crypto Engine

### Universe
18 coins: BTC, ETH, SOL, AVAX, LINK, DOT, MATIC, ADA, XRP, DOGE, LTC, BCH, UNI, AAVE, CRV, ATOM, NEAR, FTM

### Strategy
- Momentum + trend following
- Crypto trades 24/7 — no market hours restriction
- Config stored in Supabase, editable via `/api/crypto/algo/config`

### Config (key fields)
| Parameter | Notes |
|-----------|-------|
| `trading_enabled` | Master on/off switch |
| Per-coin toggles | Enable/disable individual coins |

---

## 3. Options Engine

### Strategy: Income (Cash-Secured Puts)
- Sells **out-of-the-money puts** (delta ~0.25) on large-cap stocks
- **Pop (probability of profit):** ≥ 74%
- **DTE (days to expiration):** 30–50 days
- **IV Rank proxy:** prefers elevated IV (higher premium)
- Avoids positions with earnings within the DTE window

### Strategy: Growth (Long Calls/Spreads)
- Momentum-based directional bets

### Config
- Managed via `/api/options/algo/config`
- Key thresholds: `min_pop`, `max_dte`, `min_expected_return`

---

## 4. P&L Accounting

| Asset | Source of Truth |
|-------|----------------|
| Stocks (today P&L) | Alpaca account: `equity - last_equity` |
| Stocks (all-time P&L) | Alpaca account: `equity - $100,000` |
| Options P&L | Supabase `options_paper_trades` table |
| Crypto P&L | Supabase `crypto_trades` table |

> **Why Alpaca for stocks?** The Supabase `intraday_trades` table has data quality issues (unknown entry times, missing P&L on some rows). Alpaca's account endpoint gives the real-time accurate number.

---

## 5. Key API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/algo/status` | Scanner + executor health |
| `GET /api/intraday/positions` | Open stock positions with stops/targets |
| `GET /api/intraday/equity-curve` | Stocks cumulative P&L curve (stocks only) |
| `GET /api/alpaca/account` | Live account equity, today P&L, all-time P&L |
| `GET /api/scanner/config` | Current stock scanner config |
| `POST /api/scanner/config` | Update config fields |
| `GET /api/options/equity-curve` | Options cumulative P&L curve |
| `GET /api/crypto/equity-curve` | Crypto cumulative P&L curve |
| `GET /api/risk/circuit-breaker` | Circuit breaker state |

---

## 6. Why We Replaced the Old Scanner (2026-05-13)

The previous scanner used **whale/insider signals** (dark pool, 13F filings):

| Metric | Old Scanner | Target |
|--------|-------------|--------|
| Win rate | 30.9% | > 50% |
| Total P&L | -$4,764 | Positive |
| Health score | 36/100 | > 70 |
| Profit factor | 0.51 | > 1.5 |

Root cause: whale signals are lagging indicators. By the time a 13F filing is public, the move is done. The EMA/RSI scanner acts on real-time price structure instead.

---

## 7. Position Sizing History

| Date | Max trade | Reason |
|------|-----------|--------|
| Initial | $5,000 | Conservative default |
| 2026-05-13 | **$15,000** | $5K too small — 5% move only = $250 profit. $15K generates $750/winning trade, meaningful daily P&L on a $97K account |

---

## 8. Rules That Cannot Be Changed Without Explicit Approval

1. Paper trading only — never switch Alpaca to live endpoint
2. Never delete or replace routes in `main.py` — only add new ones
3. Stop loss and target multipliers (1.5×ATR stop, 1×ATR T1, 2×ATR T2)
4. Partial sell at T1 (50%) — protects capital on every winning trade
