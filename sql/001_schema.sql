-- IBKR Trade Journal schema for Supabase (Postgres)
-- Run this in the Supabase SQL Editor to create the tables.

-- ── statements ──────────────────────────────────────────────────────────────
create table if not exists statements (
    id           uuid primary key default gen_random_uuid(),
    account_id   text    not null,
    period_start date    not null,
    period_end   date    not null,
    base_currency text   not null default 'USD',
    created_at   timestamptz not null default now(),

    constraint uq_account_period unique (account_id, period_start, period_end)
);

-- ── positions ───────────────────────────────────────────────────────────────
create table if not exists positions (
    id              uuid primary key default gen_random_uuid(),
    statement_id    uuid    not null references statements(id) on delete cascade,
    symbol          text    not null,
    asset_class     text    not null,
    quantity        numeric not null,
    cost_basis      numeric not null,
    market_price    numeric not null,
    market_value    numeric not null,
    unrealized_pnl  numeric not null,
    currency        text    not null default 'USD',
    statement_date  date    not null,
    expiry          date,
    strike          numeric,
    "right"         text,
    created_at      timestamptz not null default now()
);

create index if not exists idx_positions_statement on positions(statement_id);

-- ── trades ──────────────────────────────────────────────────────────────────
create table if not exists trades (
    id              uuid primary key default gen_random_uuid(),
    statement_id    uuid    not null references statements(id) on delete cascade,
    trade_date      timestamptz not null,
    symbol          text    not null,
    asset_class     text    not null,
    side            text    not null,
    quantity        numeric not null,
    price           numeric not null,
    proceeds        numeric not null,
    commission      numeric not null,
    realized_pnl    numeric not null,
    currency        text    not null default 'USD',
    expiry          date,
    strike          numeric,
    "right"         text,
    created_at      timestamptz not null default now()
);

create index if not exists idx_trades_statement on trades(statement_id);
create index if not exists idx_trades_date on trades(trade_date);
create index if not exists idx_trades_symbol on trades(symbol);

-- ── Row-Level Security (RLS) ────────────────────────────────────────────────
-- Enable RLS but allow anon key full access (single-user journal).
-- Tighten these policies if you add auth later.

alter table statements enable row level security;
alter table positions  enable row level security;
alter table trades     enable row level security;

create policy "Allow all on statements" on statements for all using (true) with check (true);
create policy "Allow all on positions"  on positions  for all using (true) with check (true);
create policy "Allow all on trades"     on trades     for all using (true) with check (true);
