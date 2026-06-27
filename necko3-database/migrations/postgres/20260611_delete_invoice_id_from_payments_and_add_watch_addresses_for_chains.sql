ALTER TABLE "payments"
    DROP CONSTRAINT "payments_invoice_id_foreign";

ALTER TABLE "payments"
    DROP COLUMN "invoice_id";

ALTER TABLE "chains"
    ADD COLUMN "watch_addresses" TEXT[] NOT NULL DEFAULT '{}';
