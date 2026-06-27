CREATE TABLE "indexed_blocks" (
    "chain_id" INTEGER NOT NULL,
    "block_number" BIGINT NOT NULL,
    "block_hash" VARCHAR(128) NOT NULL,

    PRIMARY KEY ("chain_id", "block_number")
);