ALTER TABLE "chains" ADD COLUMN "safe_lag" SMALLINT NOT NULL DEFAULT 15;

ALTER TABLE "chains"
    ADD CONSTRAINT "check_safe_lag_positive" CHECK ("safe_lag" >= 0);