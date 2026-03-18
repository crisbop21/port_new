-- Daily OHLCV prices from Yahoo Finance
CREATE TABLE IF NOT EXISTS daily_prices (
    id          uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    symbol      text NOT NULL,
    price_date  date NOT NULL,
    open        numeric NOT NULL,
    high        numeric NOT NULL,
    low         numeric NOT NULL,
    close       numeric NOT NULL,
    adj_close   numeric NOT NULL,
    volume      int8 NOT NULL,
    created_at  timestamptz DEFAULT now(),
    UNIQUE (symbol, price_date)
);

CREATE INDEX IF NOT EXISTS idx_daily_prices_symbol ON daily_prices (symbol);
CREATE INDEX IF NOT EXISTS idx_daily_prices_date   ON daily_prices (price_date);
