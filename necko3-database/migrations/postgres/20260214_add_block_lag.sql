ALTER TABLE "chains" ADD COLUMN "block_lag" SMALLINT NOT NULL DEFAULT 3;

ALTER TABLE "chains"
    ADD CONSTRAINT "check_block_lag_positive" CHECK ("block_lag" >= 0);