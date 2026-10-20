# CWST on BNB — deep dive

*Data: `published_data/events.parquet`, lab settlements 2026-09-19 → 2026-10-01, main-bot BNB events 2026-08-04 → 2026-10-01. Reproduce with `python3 analysis/cwst_bnb.py`.*

## TL;DR

- **`cwst_bnb` has only 7 settlements (4 filled) — far too few to judge the signal itself.** Net −$0.39: both filled YES trades won (+$0.80), both filled NO trades lost (−$1.19).
- **The dominant factor on BNB in this window was direction, not signal quality.** Across *all* lab strategies on BNB, filled YES trades went **10/10 wins (+$4.32)** and filled NO trades went **0/10 (−$4.73)**. Every BNB 15M contract the lab touched settled YES.
- That matches the broader tape: of BNB contracts the main Pacman bots traded, **65% settled YES** (Aug 23 – Oct 1), rising to **~82% from Sep 18 onward**. For comparison: BTC 48%, SOL 48%, ETH 53%. Only XRP (79%) is more skewed.
- **This is a problem for cwst specifically**, because on its home turf (BTC+SOL) the cwst family's entire edge is on the **NO side**: NO 86/129 = 66.7% win rate, **+$9.50**; YES 59/100 = 59.0%, **−$0.69**. A NO-leaning signal walked into the most YES-skewed regime in the dataset.

## 1. The cwst family baseline (where it works)

Filled trades only; `breakeven_wr` = average entry price (for a binary, breakeven win rate equals the price paid).

| strategy | n | win rate | breakeven | net pnl | verdict |
|---|---|---|---|---|---|
| `cwst` | 78 | 59.0% | 53.5% | **+$4.25** | positive edge (BTC 62%, SOL 64%; ETH drags at 40%) |
| `cwst_early` | 133 | 60.2% | 59.4% | +$0.94 | roughly breakeven; ETH is the leak (21% wr, −$2.56) |
| `cwst_early_r` | 21 | **76.2%** | 62.7% | **+$2.83** | best variant per trade, small n |
| `cwst_early_x` | 6 | 33.3% | 60.2% | −$1.61 | negative, tiny n |
| `cwst_wide` | 18 | 55.6% | 57.5% | −$0.35 | ~breakeven |
| `cwst_lotto` | 4 | 0% | 6.8% | −$0.27 | lottery tickets, all dead so far |
| `cwst_bnb` | 4 | 50.0% | 59.8% | −$0.39 | see below |

Two structural facts about the family:

1. **ETH is consistently bad** for cwst (`cwst` 40% wr, `cwst_early` 21% wr vs ~40% breakeven). The signal does not transfer to every asset by default — relevant prior for BNB.
2. **The edge is NO-sided.** On BTC+SOL combined: NO side 66.7% wr at 0.593 avg price (+$9.50); YES side 59.0% at 0.597 (−$0.69). Essentially all realized cwst profit comes from fading moves, not riding them.

## 2. cwst_bnb: the full tape

Ran 2026-09-25 → 09-27 only.

| time (UTC) | ticker | side | result | qty | price | pnl |
|---|---|---|---|---|---|---|
| 09-25 05:45 | KXBNB15M-26SEP250645-45 | YES | WIN | 1 | 0.57 | +0.43 |
| 09-25 14:45 | KXBNB15M-26SEP251545-45 | YES | WIN | 1 | 0.63 | +0.37 |
| 09-25 18:00 | KXBNB15M-26SEP251900-00 | YES | WIN | 0 | 0.68 | 0.00 |
| 09-25 19:00 | KXBNB15M-26SEP252000-00 | NO | LOSS | 1 | 0.56 | −0.56 |
| 09-26 09:30 | KXBNB15M-26SEP261030-30 | NO | LOSS | 0 | 0.55 | −0.00 |
| 09-27 01:30 | KXBNB15M-26SEP270230-30 | NO | LOSS | 1 | 0.63 | −0.63 |
| 09-27 10:30 | KXBNB15M-26SEP271130-30 | YES | WIN | 0 | 0.65 | 0.00 |

The signal was actually **7/7 on direction when it said YES** (including unfilled touches) and **0/3 when it said NO**. With n=7 that is indistinguishable from "BNB only went up that week" — which it mostly did.

## 3. The BNB regime

Settlement direction of contracts the main Pacman bots traded (each row in `events.parquet` carries `-- settled YES/NO`):

| asset | settled YES share |
|---|---|
| XRP | 78.7% |
| **BNB** | **64.9%** (≈82% from Sep 18 on) |
| NEAR | 58.8% |
| ETH | 52.7% |
| ZEC | 51.5% |
| SOL | 48.4% |
| BTC | 47.7% |

BTC and SOL — where cwst earns — are balanced markets. BNB during the entire lab window was a one-way tape. Every BNB lab strategy's P&L in this period reduces to its YES/NO mix:

| strategy | YES (filled) | NO (filled) | net |
|---|---|---|---|
| `bnbyes` | 7/7 wins, +$2.99 | 0/7, −$3.34 | −$0.35 |
| `cwst_bnb` | 2/2 wins, +$0.80 | 0/2, −$1.19 | −$0.39 |
| `first` | 1/1 win, +$0.53 | 0/1, −$0.20 | +$0.33 |

Even `bnbyes` — nominally the "BNB resolves YES" hypothesis — lost money overall because half its filled trades were NO entries at 0.48 average. Its YES-only leg was perfect.

## 4. Conclusions and next steps

1. **Don't read `cwst_bnb`'s −$0.39 as a verdict on cwst mechanics** — n=4 filled, and 100% of the variance is explained by trade side in a YES-regime week.
2. **The real risk is structural**: cwst's realized edge on BTC/SOL comes from the NO side, and BNB (like XRP) runs persistently YES-skewed in this dataset. Porting a fade-biased signal to a drift-heavy market is the same mistake as `cwst_early` on ETH.
3. If the experiment continues, the cheapest robust tweak is a **regime/side gate on BNB: suppress or down-size NO entries while the rolling settled-YES share (say, trailing 30–50 contracts) is above ~60%**, rather than tuning cwst thresholds on 7 data points.
4. **Collect more data before touching parameters**: at the current cadence (~2–3 cwst_bnb settlements/day) it needs ~4–6 more weeks to reach n≈50 filled, the point where its BTC/SOL siblings became readable.
5. Housekeeping: the ingestion parser drops lab settlements into `bot_name` raw (asset = UNKNOWN, no side/pnl fields). Promoting the `[lab …] SETTLED …` format to a first-class parse (as `analysis/cwst_bnb.py` does ad hoc) would make all of this queryable in the dashboard.
