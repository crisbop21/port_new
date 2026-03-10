-- Migration: change statements unique key from (account_id, period_start,
-- period_end) to just (account_id).  One statement per account — uploading
-- a new PDF always replaces the old data for that account.
--
-- Run this in the Supabase SQL Editor.

alter table statements drop constraint if exists uq_account_period;
alter table statements add constraint uq_account unique (account_id);
