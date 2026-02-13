CREATE TABLE IF NOT EXISTS channels (
    id BIGINT PRIMARY KEY,
    username TEXT,
    title TEXT NOT NULL,
    channel_type TEXT NOT NULL,
    link TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS users (
    id BIGINT PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    bio TEXT,
    photo TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS prompts (
    id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    text TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS messages (
    channel_id BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    detail JSONB NOT NULL,
    date TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (channel_id, message_id)
);

ALTER TABLE messages ADD COLUMN IF NOT EXISTS date TIMESTAMPTZ;
UPDATE messages
SET date = (detail->>'date')::TIMESTAMPTZ
WHERE date IS NULL;

CREATE OR REPLACE FUNCTION messages_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.detail IS DISTINCT FROM OLD.detail THEN
        NEW.updated_at = NOW();
    ELSE
        NEW.updated_at = OLD.updated_at;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_messages_set_updated_at ON messages;
CREATE TRIGGER trg_messages_set_updated_at
BEFORE UPDATE ON messages
FOR EACH ROW
EXECUTE FUNCTION messages_set_updated_at();

CREATE INDEX IF NOT EXISTS idx_messages_channel_id ON messages (channel_id);
CREATE INDEX IF NOT EXISTS idx_messages_channel_date ON messages (channel_id, date);

ALTER TABLE users DROP COLUMN IF EXISTS messages_count;
DROP INDEX IF EXISTS idx_users_activity;
DROP TABLE IF EXISTS channel_users;
DROP TABLE IF EXISTS migrations;
