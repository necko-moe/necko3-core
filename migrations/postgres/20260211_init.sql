CREATE TABLE "chains" (
    "id" SERIAL PRIMARY KEY,
    "name" VARCHAR(50) NOT NULL UNIQUE,
    "rpc_url" TEXT NOT NULL,
    "chain_type" VARCHAR(20) NOT NULL,
    "xpub" TEXT NOT NULL,
    "native_symbol" VARCHAR(10) NOT NULL,
    "decimals" SMALLINT NOT NULL,
    "last_processed_block" BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE "tokens" (
    "id" SERIAL PRIMARY KEY,
    "chain_id" INTEGER NOT NULL,
    "symbol" VARCHAR(10) NOT NULL,
    "contract_address" VARCHAR(64) NOT NULL,
    "decimals" SMALLINT NOT NULL,
    CONSTRAINT "tokens_chain_id_foreign"
        FOREIGN KEY ("chain_id") REFERENCES "chains" ("id") ON DELETE CASCADE,
    CONSTRAINT "unique_token_per_chain" UNIQUE ("chain_id", "symbol")
);

CREATE TABLE "invoices" (
    "id" UUID PRIMARY KEY,
    "address" VARCHAR(64) NOT NULL,
    "address_index" INTEGER NOT NULL,
    "network" VARCHAR(50) NOT NULL,
    "token" VARCHAR(10) NOT NULL,
    "amount_raw" NUMERIC(78, 0) NOT NULL,
    "paid_raw" NUMERIC(78, 0) NOT NULL DEFAULT 0,
    "status" VARCHAR(20) NOT NULL CHECK ("status" IN ('Pending', 'Paid', 'Expired')),
    "created_at" TIMESTAMPTZ NOT NULL DEFAULT now(),
    "expires_at" TIMESTAMPTZ NOT NULL,

    CONSTRAINT "invoices_network_foreign"
        FOREIGN KEY ("network") REFERENCES "chains" ("name") ON DELETE RESTRICT
);

CREATE INDEX "idx_invoices_janitor" ON "invoices" ("status", "expires_at");
CREATE INDEX "idx_invoices_address" ON "invoices" ("address");