ALTER TABLE "payments" DROP CONSTRAINT payments_status_check;
ALTER TABLE "payments" ADD CONSTRAINT payments_status_check
    CHECK ("status" IN ('Pending', 'Confirming', 'Confirmed', 'Failed', 'Cancelled'));
-- pending = not even in blockchain