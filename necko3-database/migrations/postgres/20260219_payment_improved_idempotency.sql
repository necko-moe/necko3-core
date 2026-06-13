ALTER TABLE payments DROP CONSTRAINT unique_tx_hash;

ALTER TABLE payments ADD COLUMN log_index INTEGER NOT NULL DEFAULT -1;

ALTER TABLE payments ADD CONSTRAINT unique_payment_idempotency
    UNIQUE (tx_hash, log_index, network);