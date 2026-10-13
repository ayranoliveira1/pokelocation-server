-- Subprojeto 1 (contas, amigos, chaves). Spec §4. Envelopes (S6) e grupos (S9) vêm em migrações próprias.

CREATE TABLE users (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    email         text UNIQUE,  -- sempre minúsculo (normalizado no auth)
    password_hash text,         -- argon2id em formato PHC; NULL = só Google
    google_sub    text UNIQUE,
    friend_code   text NOT NULL UNIQUE CHECK (friend_code ~ '^[0-9A-HJKMNP-TV-Z]{8}$'), -- Crockford base32
    created_at    timestamptz NOT NULL DEFAULT now(),
    -- access token com iat anterior é recusado (logout, reuso de refresh, vínculo Google). Truncado ao segundo, como o iat.
    tokens_valid_after timestamptz NOT NULL DEFAULT date_trunc('second', now()),
    CHECK (email IS NOT NULL OR google_sub IS NOT NULL),
    CHECK (password_hash IS NULL OR email IS NOT NULL)
);

-- 1 aparelho por conta na v1 (spec §10): a chave é o usuário.
CREATE TABLE devices (
    user_id               uuid PRIMARY KEY REFERENCES users (id) ON DELETE CASCADE,
    registration_id       integer NOT NULL,
    identity_key          bytea NOT NULL,
    signed_prekey_id      integer NOT NULL,
    signed_prekey         bytea NOT NULL,
    signed_prekey_sig     bytea NOT NULL,
    kyber_last_resort_id  integer NOT NULL,
    kyber_last_resort     bytea NOT NULL,
    kyber_last_resort_sig bytea NOT NULL,
    fcm_token             text,
    updated_at            timestamptz NOT NULL DEFAULT now()
);

-- Removida ao ser entregue (GET /v1/keys/{userId}). Kyber sempre assinada; EC nunca.
CREATE TABLE one_time_prekeys (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id    uuid NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    kind       text NOT NULL CHECK (kind IN ('ec', 'kyber')),
    key_id     integer NOT NULL,
    public_key bytea NOT NULL,
    signature  bytea,
    CHECK ((kind = 'kyber') = (signature IS NOT NULL)),
    UNIQUE (user_id, kind, key_id)
);

-- Só o sha256 do token. Reuso de um token já usado (used_at) revoga a família inteira.
CREATE TABLE refresh_tokens (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id    uuid NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    family_id  uuid NOT NULL,
    token_hash bytea NOT NULL UNIQUE,
    expires_at timestamptz NOT NULL,
    used_at    timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX refresh_tokens_family ON refresh_tokens (family_id);
CREATE INDEX refresh_tokens_user ON refresh_tokens (user_id);

-- Pedido = linha pending (requester → addressee); o id é o do pedido na API. Um par só, em qualquer direção.
CREATE TABLE friendships (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    requester_id uuid NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    addressee_id uuid NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    status       text NOT NULL CHECK (status IN ('pending', 'accepted')),
    created_at   timestamptz NOT NULL DEFAULT now(),
    CHECK (requester_id <> addressee_id)
);
CREATE UNIQUE INDEX friendships_pair ON friendships (least(requester_id, addressee_id), greatest(requester_id, addressee_id));
CREATE INDEX friendships_requester ON friendships (requester_id);
CREATE INDEX friendships_addressee ON friendships (addressee_id);

CREATE TABLE blocks (
    blocker_id uuid NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    blocked_id uuid NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (blocker_id, blocked_id),
    CHECK (blocker_id <> blocked_id)
);
CREATE INDEX blocks_blocked ON blocks (blocked_id);
