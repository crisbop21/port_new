-- Duplicate-handling support for overlapping PDF uploads.
--
-- The application layer (src/db.py  _cleanup_overlaps) handles deduplication
-- when two PDFs for the same account cover overlapping date ranges.  The new
-- upload is always treated as authoritative:
--
--   * Fully-covered old statements are deleted (cascade removes children).
--   * Partially-overlapping old statements keep data outside the overlap but
--     lose trades/positions within the overlapping date range.
--
-- The indexes below speed up the overlap-cleanup queries that filter trades
-- and positions by date range within a statement.

create index if not exists idx_trades_stmt_date
    on trades (statement_id, trade_date);

create index if not exists idx_positions_stmt_date
    on positions (statement_id, statement_date);
