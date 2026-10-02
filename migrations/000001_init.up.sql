-- API keys are stored as SHA-256 hashes; the raw key is shown once at creation.
CREATE TABLE api_keys (
    id         BIGSERIAL PRIMARY KEY,
    name       TEXT        NOT NULL,
    key_hash   BYTEA       NOT NULL UNIQUE,
    revoked    BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Single-row-per-name counter used for range allocation of link IDs.
-- Each API instance claims a block with one atomic UPDATE ... RETURNING.
CREATE TABLE id_sequences (
    name    TEXT PRIMARY KEY,
    next_id BIGINT NOT NULL
);
INSERT INTO id_sequences (name, next_id) VALUES ('links', 0);

-- slug is the primary key: lookups on the redirect path are a single index probe,
-- and uniqueness of generated and custom slugs is enforced in one place.
CREATE TABLE links (
    slug         TEXT PRIMARY KEY,
    url          TEXT        NOT NULL,
    api_key_id   BIGINT      NOT NULL REFERENCES api_keys (id),
    is_custom    BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ
);
CREATE INDEX links_api_key_idx ON links (api_key_id);

-- No FK to links: the consumer must never fail a batch because of a link
-- that was deleted or has not replicated. event_id makes inserts idempotent.
CREATE TABLE clicks (
    event_id   UUID PRIMARY KEY,
    slug       TEXT        NOT NULL,
    clicked_at TIMESTAMPTZ NOT NULL,
    referer    TEXT        NOT NULL DEFAULT '',
    user_agent TEXT        NOT NULL DEFAULT '',
    ip         TEXT        NOT NULL DEFAULT ''
);
CREATE INDEX clicks_slug_time_idx ON clicks (slug, clicked_at DESC);
