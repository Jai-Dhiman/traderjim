-- Migration: Add pending_fill and expired status to trades
-- Run with: wrangler d1 execute mahler-db --remote --file=src/migrations/0003_trade_status_pending_fill.sql

-- SQLite doesn't support ALTER CONSTRAINT, so we need to recreate the table
-- Must also handle the positions table that references trades

-- Disable foreign key checks during migration
PRAGMA foreign_keys=OFF;

-- Step 1: Create new trades table with updated constraint
CREATE TABLE IF NOT EXISTS trades_new (
    id TEXT PRIMARY KEY,
    recommendation_id TEXT REFERENCES recommendations(id),

    opened_at TEXT,
    closed_at TEXT,
    status TEXT CHECK (status IN ('pending_fill', 'open', 'closed', 'expired')),

    underlying TEXT NOT NULL,
    spread_type TEXT NOT NULL CHECK (spread_type IN ('bull_put', 'bear_call')),
    short_strike REAL NOT NULL,
    long_strike REAL NOT NULL,
    expiration TEXT NOT NULL,

    entry_credit REAL NOT NULL,
    exit_debit REAL,
    profit_loss REAL,

    contracts INTEGER DEFAULT 1,
    broker_order_id TEXT,

    reflection TEXT,
    lesson TEXT
);

-- Step 2: Copy data from old table
INSERT INTO trades_new SELECT * FROM trades;

-- Step 3: Drop old table
DROP TABLE trades;

-- Step 4: Rename new table
ALTER TABLE trades_new RENAME TO trades;

-- Step 5: Recreate indexes
CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
CREATE INDEX IF NOT EXISTS idx_trades_underlying ON trades(underlying);

-- Re-enable foreign key checks
PRAGMA foreign_keys=ON;
