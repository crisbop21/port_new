-- Migration: allow multiple statements per account with different date ranges.
-- The old constraint allowed only one statement per account_id; now each
-- (account_id, period_start, period_end) triple is unique.  Uploading a PDF
-- whose period already exists updates in place; a new period adds a row.
-- Trade deduplication across overlapping periods is handled at the application layer.
--
-- Run this in the Supabase SQL Editor.

ALTER TABLE statements DROP CONSTRAINT IF EXISTS uq_account;
ALTER TABLE statements ADD CONSTRAINT uq_account_period
    UNIQUE (account_id, period_start, period_end);
