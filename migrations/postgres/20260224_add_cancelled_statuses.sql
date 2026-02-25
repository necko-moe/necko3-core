ALTER TABLE "invoices" DROP CONSTRAINT invoices_status_check;
ALTER TABLE "invoices" ADD CONSTRAINT invoices_status_check
    CHECK ("status" IN ('Pending', 'Paid', 'Expired', 'Cancelled'));

ALTER TABLE "payments" DROP CONSTRAINT payments_status_check;
ALTER TABLE "payments" ADD CONSTRAINT payments_status_check
    CHECK ("status" IN ('Confirming', 'Confirmed', 'Cancelled'));

ALTER TABLE "webhooks" DROP CONSTRAINT webhooks_status_check;
ALTER TABLE "webhooks" ADD CONSTRAINT webhooks_status_check
    CHECK ("status" IN ('Pending', 'Processing', 'Sent', 'Failed', 'Cancelled'));