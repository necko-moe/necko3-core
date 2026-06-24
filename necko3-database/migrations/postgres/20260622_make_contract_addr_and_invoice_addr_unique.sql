DROP INDEX IF EXISTS "idx_invoices_address";

ALTER TABLE "invoices" ADD CONSTRAINT "uq_invoices_address" UNIQUE ("address");

ALTER TABLE "tokens" ADD CONSTRAINT "uq_token_contract_address" UNIQUE ("contract_address");