-- Add reporting-frequency awareness to stock_metrics.
-- Run this in the Supabase SQL Editor after 006_valuation_snapshots.sql.

-- fiscal_year: the XBRL 'fy' field — more reliable than deriving from
-- period_end.year for companies with non-calendar fiscal years (e.g. Apple Sep,
-- Microsoft Jun, Walmart Jan).
alter table stock_metrics add column if not exists fiscal_year integer;

-- duration_days: (period_end − period_start).days — distinguishes YTD-cumulative
-- values (~180 days for Q2) from standalone-quarterly values (~90 days for Q2).
alter table stock_metrics add column if not exists duration_days integer;

-- reporting_style: how this company reports quarterly income-statement data.
--   'cumulative_ytd'        — Q2 = 6-month YTD (most US large-caps)
--   'standalone_quarterly'  — Q2 = 3-month standalone
--   'annual_only'           — only 10-K, no 10-Q data
--   'mixed'                 — both durations found (rare)
alter table stock_metrics add column if not exists reporting_style text;
