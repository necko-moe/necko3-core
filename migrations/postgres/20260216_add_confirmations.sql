ALTER TABLE chains
    ADD COLUMN required_confirmations BIGINT NOT NULL DEFAULT 15;