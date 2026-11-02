CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY NOT NULL,
    producer TEXT NOT NULL,
    destination TEXT NOT NULL,
    idempotency_key TEXT,
    payload BLOB NOT NULL,
    payload_bytes INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    expires_at INTEGER,
    state TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL,
    next_attempt_at INTEGER,
    last_error TEXT,
    delivered_at INTEGER,
    updated_at INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS messages_producer_idempotency
    ON messages (producer, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS messages_state_created
    ON messages (state, created_at);

CREATE INDEX IF NOT EXISTS messages_expiry
    ON messages (expires_at)
    WHERE expires_at IS NOT NULL;
