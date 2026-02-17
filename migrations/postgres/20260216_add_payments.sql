CREATE TABLE "payments" (
    "id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "invoice_id" UUID NOT NULL,
    "from" VARCHAR(64) NOT NULL,
    "to" VARCHAR(64) NOT NULL,
    "network" VARCHAR(50) NOT NULL,
    "tx_hash" VARCHAR(66) NOT NULL,
    "amount_raw" NUMERIC(78, 0) NOT NULL,
    "block_number" BIGINT NOT NULL,
    "status" VARCHAR(20) NOT NULL CHECK ("status" IN ('Confirming', 'Confirmed')),
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT "payments_invoice_id_foreign"
        FOREIGN KEY ("invoice_id") REFERENCES "invoices" ("id") ON DELETE CASCADE,

    CONSTRAINT "unique_tx_hash" UNIQUE ("invoice_id", "tx_hash")
);

CREATE INDEX "idx_payments_confirming" ON "payments" ("status")
    WHERE ("status" = 'Confirming');