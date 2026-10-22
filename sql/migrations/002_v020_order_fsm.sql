-- Migration 002: Add Order FSM and Execution Reconciliation Columns
ALTER TABLE trades ADD COLUMN client_order_id TEXT;
ALTER TABLE trades ADD COLUMN fsm_state TEXT DEFAULT 'PENDING' 
    CHECK(fsm_state IN ('PENDING', 'OPEN_ON_BOOK', 'PARTIALLY_FILLED', 'FILLED', 'CANCELLED', 'REJECTED', 'EXPIRED'));
ALTER TABLE trades ADD COLUMN exchange_order_id TEXT;
ALTER TABLE trades ADD COLUMN avg_fill_price REAL;
ALTER TABLE trades ADD COLUMN filled_units REAL DEFAULT 0.0;
ALTER TABLE trades ADD COLUMN slippage_paid_usd REAL DEFAULT 0.0;

CREATE TABLE IF NOT EXISTS order_state_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_order_id TEXT NOT NULL,
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    timestamp_utc INTEGER NOT NULL,
    reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_order_state_audit_client_id
    ON order_state_audit (client_order_id, id ASC);
