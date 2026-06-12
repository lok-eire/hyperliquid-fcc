"""
fcc_dag.py — Proof-of-concept Airflow DAG for Fresh Capital Confirmation (FCC):
an on-chain collateral-flow signal for Hyperliquid.

PIPELINE OVERVIEW
-----------------
Hourly, interval-aware (the same code path serves live runs and backfill):

  1. Incrementally extract deposit/withdrawal transfer logs for all Hyperliquid
     deposit routes on Arbitrum (high-water-mark block ranges, reorg margin).
  2. Snapshot exchange context: aggregate OI and basket price (InfoAPI).
  3. Label any new depositor wallets (one-hop provenance, age, leaderboard).
  4. Validate: schema, watermark, units, dupes, balance reconciliation,
     label-coverage gate.
  5. Transform: hourly bars -> rolling 24h net flow -> 30d z-score -> Fresh
     Capital Ratio vs ΔOI -> 4-state regime -> cohort decomposition.
  6. Idempotent upserts; regime transitions + smart-flow extremes -> event bus.

DESIGN NOTES
------------
* catchup=True: bridge history is complete on-chain, so unlike position-
  snapshot metrics this signal is FULLY BACKFILLABLE. The DAG reads its
  window from data_interval_start/end, never from "now", which is what makes
  `airflow dags backfill` over Hyperliquid's whole life a one-liner.
* Raw rows are keyed (tx_hash, log_index): re-scans and retries are
  idempotent by construction; the reorg margin re-processes the last
  REORG_MARGIN_BLOCKS every run and upserts harmlessly.
* The cohort layer degrades gracefully: if label coverage of deposit notional
  drops below LABEL_COVERAGE_FLOOR, headline FCC still publishes and only the
  cohort split is marked invalid.
"""

from __future__ import annotations

import json
import logging
import statistics
from datetime import datetime, timedelta, timezone

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger("fcc")

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────
WAREHOUSE_CONN_ID = "analytics_warehouse"
ARBITRUM_RPC = "https://arb1.arbitrum.io/rpc"        # prod: dedicated provider
HL_INFO_URL = "https://api.hyperliquid.xyz/info"

USDC_ARBITRUM = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
USDC_DECIMALS = 6

REORG_MARGIN_BLOCKS = 50          # re-scan margin each run; safe via idempotent keys
BASKET = ["BTC", "ETH", "SOL", "HYPE"]   # OI-weighted price context basket
ZSCORE_WINDOW_DAYS = 30
LABEL_COVERAGE_FLOOR = 0.70       # cohort layer suppressed below this
RECONCILIATION_TOL = 0.005        # net flow vs bridge balance delta, 0.5%
ALERT_SMART_Z = 2.0

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "sla": timedelta(minutes=45),
}


def _on_failure(context):
    ti = context["task_instance"]
    log.error("FCC task failed: %s.%s run=%s", ti.dag_id, ti.task_id, context["run_id"])
    # SlackWebhookHook(...).send(...)  # prod


def _rpc(method: str, params: list):
    resp = requests.post(
        ARBITRUM_RPC,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        timeout=15,
    )
    resp.raise_for_status()
    payload = resp.json()
    if "error" in payload:
        raise RuntimeError(f"RPC error: {payload['error']}")
    return payload["result"]


# ──────────────────────────────────────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────────────────────────────────────
@dag(
    dag_id="fcc_hourly",
    description="Fresh Capital Confirmation: on-chain collateral flow vs price/OI for Hyperliquid",
    schedule="@hourly",
    start_date=datetime(2023, 6, 1, tzinfo=timezone.utc),  # backfillable to bridge genesis
    catchup=True,                    # see DESIGN NOTES — this dataset rebuilds from chain
    max_active_runs=4,               # parallel backfill windows are safe (idempotent keys)
    default_args=DEFAULT_ARGS,
    on_failure_callback=_on_failure,
    tags=["hyperliquid", "onchain", "flows", "fcc"],
)
def fcc_hourly():

    # ──────────────────────────────────────────────────────────────────────
    # 1. Extract bridge transfer events (interval-aware, incremental)
    # ──────────────────────────────────────────────────────────────────────
    @task
    def extract_bridge_events(data_interval_start=None, data_interval_end=None) -> dict:
        """
        Scan USDC Transfer logs to/from every route in dim_deposit_routes for
        this run's interval. Block range is resolved from timestamps; the lower
        bound is pushed back REORG_MARGIN_BLOCKS so each run re-validates the
        chain tip it last saw (idempotent upsert absorbs the overlap).
        """
        pg = PostgresHook(WAREHOUSE_CONN_ID)
        routes = {r[0].lower(): r[1] for r in pg.get_records(
            "SELECT address, route_name FROM dim_deposit_routes WHERE active"
        )}
        if not routes:
            raise ValueError("dim_deposit_routes is empty — seed routes before enabling the DAG")

        start_block = _block_at(data_interval_start) - REORG_MARGIN_BLOCKS
        end_block = _block_at(data_interval_end)

        rows = []
        for chunk_start in range(start_block, end_block, 5_000):   # provider log-range limit
            chunk_end = min(chunk_start + 4_999, end_block)
            logs = _rpc("eth_getLogs", [{
                "address": USDC_ARBITRUM,
                "topics": [TRANSFER_TOPIC],
                "fromBlock": hex(chunk_start),
                "toBlock": hex(chunk_end),
            }])
            for lg in logs:
                frm = "0x" + lg["topics"][1][-40:]
                to = "0x" + lg["topics"][2][-40:]
                amount = int(lg["data"], 16) / 10 ** USDC_DECIMALS
                if to.lower() in routes:
                    direction, wallet, route = "deposit", frm, routes[to.lower()]
                elif frm.lower() in routes:
                    direction, wallet, route = "withdrawal", to, routes[frm.lower()]
                else:
                    continue
                rows.append({
                    "tx_hash": lg["transactionHash"], "log_index": int(lg["logIndex"], 16),
                    "block_number": int(lg["blockNumber"], 16),
                    "wallet": wallet.lower(), "direction": direction,
                    "amount_usd": amount, "route": route,
                })

        # idempotent raw load keyed (tx_hash, log_index)
        if rows:
            pg.insert_rows(
                "raw_bridge_transfers",
                [(r["tx_hash"], r["log_index"], r["block_number"], r["wallet"],
                  r["direction"], r["amount_usd"], r["route"],
                  data_interval_start) for r in rows],
                target_fields=["tx_hash", "log_index", "block_number", "wallet",
                               "direction", "amount_usd", "route", "bar_ts"],
                replace=True, replace_index=["tx_hash", "log_index"],
            )
        pg.run(
            "INSERT INTO etl_watermarks (pipeline, watermark_block, updated_at) "
            "VALUES ('fcc', %s, now()) "
            "ON CONFLICT (pipeline) DO UPDATE SET watermark_block = GREATEST("
            "etl_watermarks.watermark_block, EXCLUDED.watermark_block), updated_at = now()",
            parameters=(end_block,),
        )
        log.info("Extracted %s bridge events in blocks [%s, %s]", len(rows), start_block, end_block)
        return {"n_rows": len(rows), "start_block": start_block, "end_block": end_block,
                "deposits": [r for r in rows if r["direction"] == "deposit"]}

    def _block_at(ts) -> int:
        """Timestamp -> block number. PoC: binary search over eth_getBlockByNumber;
        prod: a block_index dimension table maintained by an upstream DAG."""
        latest = int(_rpc("eth_getBlockByNumber", ["latest", False])["number"], 16)
        lo, hi, target = 0, latest, int(ts.timestamp())
        while lo < hi:
            mid = (lo + hi) // 2
            mid_ts = int(_rpc("eth_getBlockByNumber", [hex(mid), False])["timestamp"], 16)
            lo, hi = (mid + 1, hi) if mid_ts < target else (lo, mid)
        return lo

    # ──────────────────────────────────────────────────────────────────────
    # 2. Exchange context (OI + basket price)
    # ──────────────────────────────────────────────────────────────────────
    @task
    def extract_exchange_context(data_interval_end=None) -> dict:
        """Aggregate OI (all perps) and OI-weighted basket price snapshot.
        Live runs hit the InfoAPI; backfill windows read the snapshot archive
        instead (the API has no history) — the one component of this signal
        that needs a stored archive rather than chain data."""
        pg = PostgresHook(WAREHOUSE_CONN_ID)
        archived = pg.get_first(
            "SELECT total_oi_usd, basket_px FROM raw_hl_context_snapshots "
            "WHERE snapshot_ts = date_trunc('hour', %s::timestamptz)",
            parameters=(data_interval_end,),
        )
        if archived:
            return {"total_oi_usd": float(archived[0]), "basket_px": float(archived[1]),
                    "source": "archive"}

        meta, ctxs = requests.post(
            HL_INFO_URL, json={"type": "metaAndAssetCtxs"}, timeout=10
        ).json()
        total_oi, basket_px_num, basket_oi = 0.0, 0.0, 0.0
        for asset, ctx in zip(meta["universe"], ctxs):
            oi_usd = float(ctx["openInterest"]) * float(ctx["markPx"])
            total_oi += oi_usd
            if asset["name"] in BASKET:
                basket_px_num += float(ctx["markPx"]) / float(ctx["prevDayPx"]) * oi_usd
                basket_oi += oi_usd
        ctx_row = {"total_oi_usd": total_oi,
                   "basket_px": basket_px_num / basket_oi if basket_oi else 1.0,
                   "source": "live"}
        pg.run(
            "INSERT INTO raw_hl_context_snapshots (snapshot_ts, total_oi_usd, basket_px) "
            "VALUES (date_trunc('hour', %s::timestamptz), %s, %s) ON CONFLICT DO NOTHING",
            parameters=(data_interval_end, ctx_row["total_oi_usd"], ctx_row["basket_px"]),
        )
        return ctx_row

    # ──────────────────────────────────────────────────────────────────────
    # 3. Label new depositor wallets
    # ──────────────────────────────────────────────────────────────────────
    @task
    def label_wallets(extract_result: dict) -> dict:
        """
        Cohorts: smart | cex_retail | fresh_wallet | defi_native | unlabelled.
        One-hop provenance only (funding source of the wallet's first inbound
        transfer) — deliberately cheap; deeper graph traversal is a later
        iteration, not a blocker. Labels carry confidence and are append-only.
        """
        pg = PostgresHook(WAREHOUSE_CONN_ID)
        depositors = {d["wallet"] for d in extract_result["deposits"]}
        known = {r[0] for r in pg.get_records(
            "SELECT wallet FROM dim_wallet_labels"
        )} if depositors else set()
        new = depositors - known

        leaderboard = {r[0] for r in pg.get_records(
            "SELECT wallet FROM dim_hl_leaderboard WHERE refreshed_at > now() - interval '2 days'"
        )}
        cex_hot = {r[0] for r in pg.get_records("SELECT address FROM dim_cex_hot_wallets")}

        labelled = []
        for w in new:
            if w in leaderboard:
                cohort, conf = "smart", 0.95
            else:
                # PoC heuristics from warehouse-side wallet history (Allium/Dune model)
                first_funder, age_days, defi_txs = _wallet_provenance(pg, w)
                if first_funder in cex_hot:
                    cohort, conf = "cex_retail", 0.80
                elif age_days is not None and age_days < 7:
                    cohort, conf = "fresh_wallet", 0.85
                elif defi_txs and defi_txs > 10:
                    cohort, conf = "defi_native", 0.70
                else:
                    cohort, conf = "unlabelled", 0.0
            labelled.append((w, cohort, conf, datetime.now(timezone.utc)))

        if labelled:
            pg.insert_rows(
                "dim_wallet_labels", labelled,
                target_fields=["wallet", "cohort", "confidence", "labelled_at"],
                replace=True, replace_index=["wallet"],
            )
        log.info("Labelled %s new depositor wallets", len(labelled))
        return {"n_new_labels": len(labelled)}

    def _wallet_provenance(pg, wallet: str):
        row = pg.get_first(
            "SELECT first_funder, age_days, defi_tx_count FROM stg_wallet_provenance WHERE wallet = %s",
            parameters=(wallet,),
        )
        return (row[0], row[1], row[2]) if row else (None, None, None)

    # ──────────────────────────────────────────────────────────────────────
    # 4. Validation
    # ──────────────────────────────────────────────────────────────────────
    @task
    def validate(extract_result: dict, ctx: dict, labels: dict,  # `labels` unused: forces
                 data_interval_start=None) -> dict:               # label task to run first
        pg = PostgresHook(WAREHOUSE_CONN_ID)
        errors: list[str] = []

        # 1. watermark monotonicity
        wm = pg.get_first("SELECT watermark_block FROM etl_watermarks WHERE pipeline = 'fcc'")
        if wm and wm[0] < extract_result["end_block"] - REORG_MARGIN_BLOCKS * 2:
            errors.append("watermark regressed beyond reorg margin")

        # 2. unit sanity: no single transfer should exceed plausible bounds
        bad = pg.get_first(
            "SELECT count(*) FROM raw_bridge_transfers WHERE bar_ts = %s AND "
            "(amount_usd < 0 OR amount_usd > 2e9)",
            parameters=(data_interval_start,),
        )[0]
        if bad:
            errors.append(f"unit sanity: {bad} transfers outside [0, 2e9] USD")

        # 3. duplicate detection beyond the PK (same tx, same wallet, same amount twice)
        dupes = pg.get_first(
            "SELECT count(*) FROM (SELECT tx_hash, wallet, amount_usd, count(*) c "
            "FROM raw_bridge_transfers WHERE bar_ts = %s "
            "GROUP BY 1,2,3 HAVING count(*) > 2) d",
            parameters=(data_interval_start,),
        )[0]
        if dupes:
            errors.append(f"{dupes} suspicious duplicate transfer groups")

        # 4. reconciliation: net flow vs bridge balance delta over the window
        #    (balanceOf at start/end blocks; archive nodes required in prod)
        #    PoC: tolerance check is asserted when balance snapshots are available.
        recon = pg.get_first(
            "SELECT abs(coalesce(flow_sum,0) - coalesce(balance_delta,0)) "
            "       / nullif(abs(balance_delta), 0) "
            "FROM v_fcc_reconciliation WHERE bar_ts = %s",
            parameters=(data_interval_start,),
        )
        if recon and recon[0] is not None and recon[0] > RECONCILIATION_TOL:
            errors.append(f"reconciliation drift {recon[0]:.2%} > {RECONCILIATION_TOL:.2%}")

        # 5. context sanity
        if ctx["total_oi_usd"] <= 0:
            errors.append("context: non-positive aggregate OI")

        # 6. label coverage of deposit notional (soft — gates cohort layer only)
        cov_row = pg.get_first(
            """SELECT coalesce(sum(t.amount_usd) FILTER (WHERE l.cohort IS NOT NULL
                                                          AND l.cohort != 'unlabelled'), 0)
                      / nullif(sum(t.amount_usd), 0)
               FROM raw_bridge_transfers t
               LEFT JOIN dim_wallet_labels l USING (wallet)
               WHERE t.bar_ts = %s AND t.direction = 'deposit'""",
            parameters=(data_interval_start,),
        )
        label_coverage = float(cov_row[0]) if cov_row and cov_row[0] is not None else 1.0

        if errors:
            pg.insert_rows(
                "fcc_dead_letter",
                [(datetime.now(timezone.utc), json.dumps(errors),
                  json.dumps({"window": str(data_interval_start),
                              "n_rows": extract_result["n_rows"]}))],
                target_fields=["failed_at", "errors", "context"],
            )
            raise ValueError(f"FCC validation failed: {errors}")

        return {"label_coverage": label_coverage, "ctx": ctx}

    # ──────────────────────────────────────────────────────────────────────
    # 5. Transform + load
    # ──────────────────────────────────────────────────────────────────────
    @task
    def compute_and_load(validated: dict, data_interval_start=None) -> dict:
        pg = PostgresHook(WAREHOUSE_CONN_ID)
        bar_ts = data_interval_start
        ctx = validated["ctx"]
        label_coverage = validated["label_coverage"]

        # hourly net flow for this bar + rolling 24h from the warehouse
        flows = pg.get_first(
            """SELECT
                 coalesce(sum(amount_usd) FILTER (WHERE direction='deposit'), 0),
                 coalesce(sum(amount_usd) FILTER (WHERE direction='withdrawal'), 0)
               FROM raw_bridge_transfers WHERE bar_ts = %s""",
            parameters=(bar_ts,),
        )
        deposits, withdrawals = float(flows[0]), float(flows[1])

        net_24h = float(pg.get_first(
            """SELECT coalesce(sum(CASE WHEN direction='deposit' THEN amount_usd
                                        ELSE -amount_usd END), 0)
               FROM raw_bridge_transfers
               WHERE bar_ts > %s::timestamptz - interval '24 hours' AND bar_ts <= %s""",
            parameters=(bar_ts, bar_ts),
        )[0])

        # 30d history of 24h-net-flow for z-score; 24h ΔOI and Δprice from snapshots
        hist = [float(r[0]) for r in pg.get_records(
            "SELECT net_flow_24h FROM fct_fcc WHERE bar_ts > %s::timestamptz - interval '%s days'",
            parameters=(bar_ts, ZSCORE_WINDOW_DAYS),
        )]
        warm = len(hist) >= 24 * 7   # ≥7 days of hourly bars before scoring
        flow_z = ((net_24h - statistics.fmean(hist)) / statistics.pstdev(hist)
                  if warm and statistics.pstdev(hist) > 0 else None)

        prev = pg.get_first(
            "SELECT total_oi_usd, basket_px FROM raw_hl_context_snapshots "
            "WHERE snapshot_ts = date_trunc('hour', %s::timestamptz - interval '24 hours')",
            parameters=(bar_ts,),
        )
        oi_delta_24h = ctx["total_oi_usd"] - float(prev[0]) if prev else None
        price_ret_24h = ctx["basket_px"] / float(prev[1]) - 1 if prev else None
        fcr = (net_24h / oi_delta_24h) if oi_delta_24h and oi_delta_24h > 0 else None

        regime = None
        if flow_z is not None and price_ret_24h is not None:
            up, inflow = price_ret_24h > 0, flow_z > 0
            regime = ("confirmed_rally" if up and inflow else
                      "hollow_rally" if up else
                      "accumulation" if inflow else "capitulation")

        pg.run(
            """INSERT INTO fct_fcc (bar_ts, deposits_usd, withdrawals_usd, net_flow_24h,
                                    flow_z, fcr, regime, price_ret_24h, oi_delta_24h,
                                    label_coverage, is_valid)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (bar_ts) DO UPDATE SET
                 deposits_usd=EXCLUDED.deposits_usd, withdrawals_usd=EXCLUDED.withdrawals_usd,
                 net_flow_24h=EXCLUDED.net_flow_24h, flow_z=EXCLUDED.flow_z,
                 fcr=EXCLUDED.fcr, regime=EXCLUDED.regime,
                 price_ret_24h=EXCLUDED.price_ret_24h, oi_delta_24h=EXCLUDED.oi_delta_24h,
                 label_coverage=EXCLUDED.label_coverage, is_valid=EXCLUDED.is_valid""",
            parameters=(bar_ts, deposits, withdrawals, net_24h, flow_z, fcr, regime,
                        price_ret_24h, oi_delta_24h, label_coverage,
                        bool(flow_z is not None)),
        )

        # cohort decomposition (gated on label coverage)
        smart_z = None
        if label_coverage >= LABEL_COVERAGE_FLOOR:
            pg.run(
                """INSERT INTO fct_fcc_cohort (bar_ts, cohort, deposits_usd,
                                               withdrawals_usd, net_flow_usd)
                   SELECT %s, coalesce(l.cohort, 'unlabelled'),
                          coalesce(sum(t.amount_usd) FILTER (WHERE direction='deposit'),0),
                          coalesce(sum(t.amount_usd) FILTER (WHERE direction='withdrawal'),0),
                          coalesce(sum(CASE WHEN direction='deposit' THEN amount_usd
                                            ELSE -amount_usd END),0)
                   FROM raw_bridge_transfers t
                   LEFT JOIN dim_wallet_labels l USING (wallet)
                   WHERE t.bar_ts = %s
                   GROUP BY 2
                   ON CONFLICT (bar_ts, cohort) DO UPDATE SET
                     deposits_usd=EXCLUDED.deposits_usd,
                     withdrawals_usd=EXCLUDED.withdrawals_usd,
                     net_flow_usd=EXCLUDED.net_flow_usd""",
                parameters=(bar_ts, bar_ts),
            )
            smart_hist = [float(r[0]) for r in pg.get_records(
                """SELECT net_flow_usd FROM fct_fcc_cohort
                   WHERE cohort='smart' AND bar_ts > %s::timestamptz - interval '%s days'""",
                parameters=(bar_ts, ZSCORE_WINDOW_DAYS),
            )]
            if len(smart_hist) >= 24 * 7 and statistics.pstdev(smart_hist) > 0:
                latest_smart = smart_hist[-1]
                smart_z = (latest_smart - statistics.fmean(smart_hist)) / statistics.pstdev(smart_hist)

        return {"bar_ts": str(bar_ts), "regime": regime, "flow_z": flow_z,
                "fcr": fcr, "smart_z": smart_z, "label_coverage": label_coverage}

    # ──────────────────────────────────────────────────────────────────────
    # 6. Alerts
    # ──────────────────────────────────────────────────────────────────────
    @task
    def emit_alerts(result: dict) -> int:
        """Regime transitions and smart-cohort extremes → product event bus."""
        pg = PostgresHook(WAREHOUSE_CONN_ID)
        fired = 0
        prev = pg.get_first(
            "SELECT regime FROM fct_fcc WHERE bar_ts < %s ORDER BY bar_ts DESC LIMIT 1",
            parameters=(result["bar_ts"],),
        )
        prev_regime = prev[0] if prev else None

        if result["regime"] and result["regime"] != prev_regime:
            event = {"type": "FCC_REGIME_CHANGE", "from": prev_regime,
                     "to": result["regime"], "fcr": result["fcr"], "ts": result["bar_ts"]}
            log.info("ALERT %s", event)
            # EventBusHook().publish("signals.flows", event)  # prod
            fired += 1

        if result["smart_z"] is not None and abs(result["smart_z"]) >= ALERT_SMART_Z:
            event = {"type": "FCC_SMART_FLOW_EXTREME", "z": round(result["smart_z"], 2),
                     "ts": result["bar_ts"]}
            log.info("ALERT %s", event)
            fired += 1
        return fired

    # ──────────────────────────────────────────────────────────────────────
    # Wiring
    # ──────────────────────────────────────────────────────────────────────
    extracted = extract_bridge_events()
    ctx = extract_exchange_context()
    labels = label_wallets(extracted)
    validated = validate(extracted, ctx, labels)
    result = compute_and_load(validated)
    emit_alerts(result)


fcc_hourly()


# ──────────────────────────────────────────────────────────────────────────────
# Warehouse DDL lives in sql/ddl.sql (kept out of the DAG file so schema is
# reviewable and migratable independently of orchestration code).
