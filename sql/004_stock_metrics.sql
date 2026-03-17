-- Stock fundamental metrics from SEC EDGAR
-- Run this in the Supabase SQL Editor after 003_multi_statement.sql.

create table if not exists stock_metrics (
    id            uuid primary key default gen_random_uuid(),
    symbol        text    not null,
    metric_name   text    not null,
    metric_value  numeric not null,
    period_end    date    not null,       -- fiscal period the metric covers
    source        text    not null default 'SEC_EDGAR',
    cik           text,                    -- SEC Central Index Key (for traceability)
    filing_type   text,                    -- e.g. '10-K', '10-Q'
    fetched_at    timestamptz not null default now(),

    -- Same symbol+metric+period = same fact; prevent duplicates
    constraint uq_stock_metric unique (symbol, metric_name, period_end)
);

create index if not exists idx_stock_metrics_symbol on stock_metrics(symbol);
create index if not exists idx_stock_metrics_name   on stock_metrics(metric_name);
create index if not exists idx_stock_metrics_period on stock_metrics(period_end);

-- RLS — same permissive policy as existing tables
alter table stock_metrics enable row level security;
create policy "Allow all on stock_metrics" on stock_metrics
    for all using (true) with check (true);
