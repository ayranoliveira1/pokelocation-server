-- Vínculo com o PoGo Coordinates: o JWT deles não expira, então só existe cifrado (AES-256-GCM, AAD = user_id).
CREATE TABLE IF NOT EXISTS pogo_accounts (
    user_id      uuid PRIMARY KEY REFERENCES users (id) ON DELETE CASCADE,
    provider     text NOT NULL CHECK (provider IN ('google', 'discord')),
    token_enc    bytea NOT NULL,
    display_name text,
    trainers     jsonb NOT NULL DEFAULT '[]',
    updated_at   timestamptz NOT NULL DEFAULT now()
);
