CREATE TABLE webhooks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    invoice_id UUID NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    url TEXT NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'Pending'
        CHECK ("status" IN ('Pending', 'Processing', 'Sent', 'Failed')),
    attempts INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 5,
    next_retry TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT "webhooks_invoice_id_foreign"
        FOREIGN KEY ("invoice_id") REFERENCES "invoices" ("id") ON DELETE CASCADE
);

ALTER TABLE invoices
    ADD COLUMN webhook_url TEXT,
    ADD COLUMN webhook_secret TEXT;

CREATE INDEX idx_webhooks_dispatch ON webhooks (status)
    WHERE status IN ('Pending', 'Processing');

CREATE INDEX idx_webhooks_invoice_id ON webhooks (invoice_id);