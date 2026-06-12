-- Warehouse schema for the FCC pipeline (run once)

CREATE TABLE IF NOT EXISTS dim_deposit_routes (
    address     TEXT PRIMARY KEY,        -- deposit contract on Arbitrum
    route_name  TEXT NOT NULL,           -- e.g. 'bridge2'
    active      BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS raw_bridge_transfers (
    tx_hash      TEXT NOT NULL,
    log_index    INTEGER NOT NULL,
    block_number BIGINT NOT NULL,
    wallet       TEXT NOT NULL,
    direction    TEXT NOT NULL,          -- 'deposit' | 'withdrawal'
    amount_usd   NUMERIC NOT NULL,
    route        TEXT NOT NULL,
    bar_ts       TIMESTAMPTZ NOT NULL,   -- hourly bar the event belongs to
    PRIMARY KEY (tx_hash, log_index)
);  -- partition by date(bar_ts) in prod

CREATE TABLE IF NOT EXISTS dim_wallet_labels (
    wallet      TEXT PRIMARY KEY,
    cohort      TEXT NOT NULL,           -- smart | cex_retail | fresh_wallet | defi_native | unlabelled
    confidence  NUMERIC NOT NULL,
    labelled_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_hl_context_snapshots (
    snapshot_ts  TIMESTAMPTZ PRIMARY KEY,
    total_oi_usd NUMERIC NOT NULL,
    basket_px    NUMERIC NOT NULL
);

CREATE TABLE IF NOT EXISTS fct_fcc (
    bar_ts          TIMESTAMPTZ PRIMARY KEY,
    deposits_usd    NUMERIC NOT NULL,
    withdrawals_usd NUMERIC NOT NULL,
    net_flow_24h    NUMERIC NOT NULL,
    flow_z          NUMERIC,             -- NULL during 7d warm-up
    fcr             NUMERIC,             -- NULL when ΔOI ≤ 0
    regime          TEXT,                -- confirmed_rally | hollow_rally | accumulation | capitulation
    price_ret_24h   NUMERIC,
    oi_delta_24h    NUMERIC,
    label_coverage  NUMERIC NOT NULL,
    is_valid        BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS fct_fcc_cohort (
    bar_ts          TIMESTAMPTZ NOT NULL,
    cohort          TEXT NOT NULL,
    deposits_usd    NUMERIC NOT NULL,
    withdrawals_usd NUMERIC NOT NULL,
    net_flow_usd    NUMERIC NOT NULL,
    PRIMARY KEY (bar_ts, cohort)
);

CREATE TABLE IF NOT EXISTS etl_watermarks (
    pipeline        TEXT PRIMARY KEY,
    watermark_block BIGINT NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS fcc_dead_letter (
    failed_at TIMESTAMPTZ NOT NULL,
    errors    JSONB NOT NULL,
    context   JSONB NOT NULL
);
