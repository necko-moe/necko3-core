ALTER TABLE "payments" DROP CONSTRAINT payments_status_check;
ALTER TABLE "payments" ADD CONSTRAINT payments_status_check
    CHECK ("status" IN ('Pending', 'Confirming', 'Confirmed', 'Failed'));

ALTER TABLE "webhooks" DROP CONSTRAINT webhooks_status_check;
ALTER TABLE "webhooks" ADD CONSTRAINT webhooks_status_check
    CHECK ("status" IN ('Pending', 'Processing', 'Delivered', 'Failed'));