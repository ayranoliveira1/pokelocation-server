-- Subprojeto 2 (S6): fila de envelopes cifrados. Todo envelope é gravado e só sai no Ack do destinatário (ou em 30 dias).
CREATE TABLE envelopes (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    recipient_id  uuid NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    sender_id     uuid NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    client_msg_id text NOT NULL CHECK (octet_length(client_msg_id) BETWEEN 1 AND 64),
    payload       bytea NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (sender_id, recipient_id, client_msg_id) -- reenvio ignorado; também serve ao CASCADE/DELETE por remetente
);
CREATE INDEX envelopes_recipient ON envelopes (recipient_id, id); -- fila do destinatário, mais antigos primeiro
