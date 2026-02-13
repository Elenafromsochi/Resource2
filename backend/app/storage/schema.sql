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

ALTER TABLE users DROP COLUMN IF EXISTS messages_count;
DROP INDEX IF EXISTS idx_users_activity;
DROP TABLE IF EXISTS channel_users;
DROP TABLE IF EXISTS migrations;
