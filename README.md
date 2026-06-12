# Fresh Capital Confirmation (FCC)

An on-chain collateral-flow signal for Hyperliquid that answers a question every
trader asks during a move: **is this rally funded by fresh capital, or by margin
recycled from money already on the exchange — and whose money is it?**

## The idea in 30 seconds

Hyperliquid's margin pool has a property no CEX shares: a visible boundary.
Every dollar of trading collateral enters and leaves through an enumerable set
of on-chain deposit routes, and every deposit/withdrawal is a public token
transfer attributable to a wallet. FCC turns that complete ledger into a signal:

| Output | Meaning |
|---|---|
| `net_flow_24h`, `flow_z` | Rolling 24h net bridge flow, z-scored vs 30d history |
| `FCR` (Fresh Capital Ratio) | `net_flow / ΔOI` — ≤0 means leverage is expanding on recycled margin |
| `regime` | Confirmed Rally · Hollow Rally · Accumulation · Capitulation |
| Cohort layer | Deposits split by depositor class: **smart** (top-PnL leaderboard) · CEX-retail · fresh wallets · DeFi-native |
| `smart_inflow_z` | Net smart-cohort flow at a 30-day extreme is the strongest read |

Headline flow alone is misleading: +$40m of deposits is bullish if proven-PnL
wallets are wiring in ammo, and bearish if it's losing retail topping up margin.
The cohort decomposition is what turns an activity stat into a positioning signal.

## Repo layout

```
dags/fcc_dag.py     Proof-of-concept Airflow DAG (extraction → validation →
                    transform → load → alerting)
sql/ddl.sql         Warehouse schema (raw, dim, fct tables, watermarks, dead letter)
```

## Pipeline design highlights

- **Fully backfillable** — bridge history is complete on-chain, so the DAG is
  interval-aware (`data_interval_start/end`, `catchup=True`) and the same code
  path serves live runs and a historical backfill across Hyperliquid's entire
  life. The signal can be validated against every prior squeeze and flush
  before it ever ships to users.
- **Idempotent by construction** — raw rows keyed `(tx_hash, log_index)`; a
  50-block reorg margin is re-scanned every run and upserted harmlessly.
- **Validation before transform** — watermark monotonicity, unit sanity,
  duplicate detection, net-flow vs bridge balance-delta reconciliation (0.5%
  tolerance), and a label-coverage gate. Hard failures dead-letter the window
  with full context; nothing is silently dropped.
- **Graceful degradation** — if wallet-label coverage of deposit notional falls
  below 70%, only the cohort layer is suppressed; headline FCC still publishes.
- **Honest caveats** — deposit ≠ long; incentive-season deposit spikes are
  annotated, not silently adjusted; the smart cohort carries survivorship bias;
  one-hop wallet provenance is deliberately cheap (deeper graph traversal is an
  iteration, not a blocker).

## What this is not

A production system. It is a proof of concept optimised for showing design
reasoning: scheduling, validation, idempotency, backfill posture and
observability are real; secrets management, infra-as-code and the block-index
dimension table are noted as production follow-ups in code comments.
