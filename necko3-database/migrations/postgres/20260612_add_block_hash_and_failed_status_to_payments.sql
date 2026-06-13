ALTER TABLE payments ADD COLUMN block_hash VARCHAR(128) NOT NULL DEFAULT '0x0';

ALTER TABLE "payments" DROP CONSTRAINT payments_status_check;
ALTER TABLE "payments" ADD CONSTRAINT payments_status_check
    CHECK ("status" IN ('Confirming', 'Confirmed', 'Failed', 'Cancelled'));