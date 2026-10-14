-- Limpezas diárias (envelopes > 30 dias, refresh vencidos): sem estes índices cada lote varre a tabela inteira.
CREATE INDEX envelopes_created_at ON envelopes (created_at);
CREATE INDEX refresh_tokens_expires_at ON refresh_tokens (expires_at);
