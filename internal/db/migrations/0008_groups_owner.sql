-- migrate:no-transaction
-- Apagar uma conta passa pelo ON DELETE CASCADE de groups.owner_id: sem índice, varre a tabela de grupos.
CREATE INDEX CONCURRENTLY IF NOT EXISTS groups_owner ON groups (owner_id);
