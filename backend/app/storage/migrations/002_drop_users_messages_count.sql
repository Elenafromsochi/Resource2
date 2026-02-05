ALTER TABLE users DROP COLUMN IF EXISTS messages_count;
DROP INDEX IF EXISTS idx_users_activity;
