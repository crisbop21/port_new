-- Valuation snapshots — persisted ratio + score computations
-- Run this in the Supabase SQL Editor after daily_prices.sql.

CREATE TABLE IF NOT EXISTS valuation_snapshots (
    id              uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    symbol          text    NOT NULL,
    snapshot_date   date    NOT NULL,       -- date the snapshot was computed
    preset          text    NOT NULL,       -- scoring preset used (Balanced, Value, etc.)

    -- Valuation ratios
    pe_ttm          numeric,
    pb              numeric,
    ps              numeric,
    ev_ebitda       numeric,
    ev_revenue      numeric,
    peg             numeric,
    earnings_yield  numeric,
    fcf_yield       numeric,
    market_cap      numeric,
    enterprise_value numeric,

    -- Profitability
    gross_margin    numeric,
    operating_margin numeric,
    net_margin      numeric,
    roe             numeric,
    roa             numeric,

    -- Financial health
    debt_to_equity  numeric,
    current_ratio   numeric,
    interest_coverage numeric,
    cash_to_assets  numeric,
    dividend_yield  numeric,
    payout_ratio    numeric,

    -- Growth
    revenue_growth  numeric,
    eps_growth      numeric,
    net_income_growth numeric,

    -- Historical percentiles
    pe_percentile   numeric,
    pb_percentile   numeric,
    ps_percentile   numeric,
    ev_ebitda_percentile numeric,

    -- Composite scores (0-100)
    score_composite     numeric,
    score_valuation     numeric,
    score_profitability numeric,
    score_health        numeric,
    score_growth        numeric,

    -- Metadata
    price_used      numeric NOT NULL,       -- the close price used for computation
    computed_at     timestamptz NOT NULL DEFAULT now(),

    -- Same symbol + date + preset = one snapshot; upsert on re-computation
    UNIQUE (symbol, snapshot_date, preset)
);

CREATE INDEX IF NOT EXISTS idx_val_snap_symbol ON valuation_snapshots (symbol);
CREATE INDEX IF NOT EXISTS idx_val_snap_date   ON valuation_snapshots (snapshot_date);
CREATE INDEX IF NOT EXISTS idx_val_snap_preset ON valuation_snapshots (preset);

-- RLS — same permissive policy as other tables
ALTER TABLE valuation_snapshots ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow all on valuation_snapshots" ON valuation_snapshots
    FOR ALL USING (true) WITH CHECK (true);
