-- Histórico dos alertas enviados por push; data = os campos do push. Teto e prazo no código (maxNotifs, notifKeep).
CREATE TABLE IF NOT EXISTS pogo_notifications (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id    uuid NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    data       jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS pogo_notifications_user ON pogo_notifications (user_id, id);
CREATE INDEX IF NOT EXISTS pogo_notifications_created ON pogo_notifications (created_at);
