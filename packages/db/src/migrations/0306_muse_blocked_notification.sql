-- Adds one internal notification-state field for the native blocked-issue
-- notification content versioning. No backfill, no wakes, and no cleanup of
-- any existing data.
ALTER TABLE "issues" ADD COLUMN IF NOT EXISTS "blocked_notification_state" jsonb;
-- Keep this field when rolling the runtime package back: older packages
-- ignore it. Do not drop it or alter the state during emergency rollbacks.
