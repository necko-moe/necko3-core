ALTER TABLE "payments" ALTER COLUMN "log_index" DROP DEFAULT;
ALTER TABLE "payments" ALTER COLUMN "log_index" DROP NOT NULL;

UPDATE "payments" SET "log_index" = NULL WHERE "log_index" < 0;

ALTER TABLE "payments" ADD CONSTRAINT "check_log_index_positive" CHECK ("log_index" >= 0);
