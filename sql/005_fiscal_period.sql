-- Add fiscal period tracking for TTM computation.
-- Run this in the Supabase SQL Editor after 004_stock_metrics.sql.

-- fiscal_period: 'FY', 'Q1', 'Q2', 'Q3', 'Q4' — tells us how many months
-- the income statement value covers (needed for TTM).
alter table stock_metrics add column if not exists fiscal_period text;

-- period_start: the beginning of the reporting period.
-- For 10-K this is ~12 months before period_end; for 10-Q it varies.
-- Needed to compute the duration and derive isolated quarterly values.
alter table stock_metrics add column if not exists period_start date;

-- Update the unique constraint to include fiscal_period, since the same
-- period_end can appear in both a 10-Q (YTD cumulative) and a 10-K (annual).
-- Drop old constraint and recreate.
alter table stock_metrics drop constraint if exists uq_stock_metric;
alter table stock_metrics add constraint uq_stock_metric
    unique (symbol, metric_name, period_end, fiscal_period);
