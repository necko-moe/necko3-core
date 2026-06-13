CREATE TABLE xpub_states (
    xpub VARCHAR(255) PRIMARY KEY,
    last_used_index BIGINT NOT NULL DEFAULT -1
);