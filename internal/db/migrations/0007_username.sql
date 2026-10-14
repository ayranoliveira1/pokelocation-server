-- migrate:no-transaction
-- @nome (docs/api.md §8): coluna nova em tabela existente e índice único com CONCURRENTLY, sem travar escrita.
-- O ALTER pega lock exclusivo em users: sem esperar atrás de transação longa (falha e tenta na próxima subida).
SET lock_timeout = '5s';
ALTER TABLE users ADD COLUMN IF NOT EXISTS username text CONSTRAINT users_username_check CHECK (username ~ '^[a-z][a-z0-9_.]{2,19}$' AND username NOT LIKE '%..%' AND username NOT LIKE '%.');
RESET lock_timeout;
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS users_username_key ON users (username);
