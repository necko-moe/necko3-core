ALTER TABLE "chains" RENAME COLUMN "rpc_url" TO "rpc_urls";

ALTER TABLE "chains"
    ALTER COLUMN "rpc_urls" TYPE TEXT[]
        USING ARRAY["rpc_urls"];

ALTER TABLE "chains" ALTER COLUMN "rpc_urls" SET NOT NULL;