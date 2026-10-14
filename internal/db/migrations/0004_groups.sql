-- Grupos (docs/api.md §7): o servidor só conhece dono e membros; nome e metadados vão dentro das mensagens cifradas.
CREATE TABLE groups (
    id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_id   uuid NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- O dono também é membro. joined_at decide quem herda o grupo quando o dono sai (o mais antigo).
CREATE TABLE group_members (
    group_id  uuid NOT NULL REFERENCES groups (id) ON DELETE CASCADE,
    user_id   uuid NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    joined_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (group_id, user_id)
);
CREATE INDEX group_members_user ON group_members (user_id); -- meus grupos, grupo em comum, teto de 25
