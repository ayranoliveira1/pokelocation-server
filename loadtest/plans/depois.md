# Depois da 0003

Seed de loadtest/seed com a 0003 aplicada (`envelopes_created_at` 18 MB, `refresh_tokens_expires_at` 4,5 MB).
Planos do apitest inteiro com o servidor 18b58ed (consulta única no Auth do WS, limpezas em lote). Compare com
[antes.md](antes.md).

## Carga de 15 mil (roteiro de loadtest/README.md)

| Métrica | Antes | Depois (b18bf29) | Meta |
|---|---|---|---|
| Conexões · quedas · perdas | 15 mil · 0 · 0 | 15 mil · 0 · 0 | 0 perdas |
| Latência posição p99 | 4–16 ms | 4,0 ms | < 50 ms |
| Latência envelope p99 | 4–16 ms | 9,4 ms | < 50 ms |
| RAM por conexão viva | 37 KB | 24,8 KB | ≤ 25 KB |
| RAM por conexão no pico | 55,7 KB | 32 KB | |
| CPU média | 0,48 núcleo (1 GiB) | 0,45 núcleo | |

Perfilar perturba a latência: a mesma carga com captura de pprof deu envelope p99 de 168 ms (o perfil de heap força
GC). Número de latência só vale de rodada sem pprof.

## Limpezas diárias

O DELETE único, antes e depois da 0003, apagando 82 mil envelopes de 900 mil e 89 mil refresh de 266 mil:

| Query | Sem índice | Com índice |
|---|---|---|
| `DELETE FROM envelopes WHERE created_at < now() - interval '30 days'` | 2004 ms, Seq Scan | 842 ms, Bitmap Index Scan |
| `DELETE FROM refresh_tokens WHERE expires_at < $1` | 235 ms, Seq Scan | 79 ms, Bitmap Index Scan |

Com a carga, o DELETE único chegou a 12,3 s (acima do statement_timeout de 10 s) e explica o envelope p99 de 103 ms
de uma rodada curta. O código passou a apagar em lotes de 5000, mas **o lote atual não usa os índices novos**:
`… WHERE id IN (SELECT id … WHERE created_at < … LIMIT $1)` sem `ORDER BY` vira Seq Scan com parada antecipada, cada
lote relê o começo já limpo da tabela, e no refresh o DELETE externo lê a tabela inteira a cada lote (Hash Semi Join).

| Limpeza completa (17–18 lotes) | Lote atual | `id = ANY(ARRAY(SELECT id … ORDER BY created_at LIMIT $1))` |
|---|---|---|
| envelopes, 83 mil | 1737 ms, pior lote 237 ms, Seq Scan | 1212–1689 ms, pior lote 138–144 ms, Index Scan dentro e fora |
| refresh, 89 mil | 393 ms, pior lote 41 ms, Seq Scan por lote | 253–267 ms, pior lote 17–49 ms, Index Scan dentro e fora |

Nesta escala os tempos ficam no ruído do cache; a diferença é de crescimento: a forma com `ORDER BY` custa o mesmo por
lote qualquer que seja o backlog. `TestIndicesCriticosSeed` falha nos dois lotes até a troca.

## Planos (apitest)

Seq Scan em tabela grande: nenhum. Sorts só em conjuntos pequenos (bloqueio: 0–1 linha; lista de amigos: até ~650).

| # | Query | Chamadas | Média ms | Máx ms | Linhas/chamada | Buffers hit/read por chamada | Plano (pior caso) | Alerta |
|---|---|---|---|---|---|---|---|---|
| 1 | `WITH nu AS ( INSERT INTO users (email, friend_code) SELECT $5 \|\| gen_random_uuid() \|\| $6, (SELECT string_a…` | 3 | 163.024 | 324.757 | 200.0 | 6724.7 / 794.0 | ModifyTable friendships → ModifyTable users | (ferramenta de teste)  |
| 2 | `SELECT pg_advisory_lock($1)` | 17 | 12.931 | 46.375 | 1.0 | 0.0 / 0.0 |  |  |
| 3 | `WITH r AS ( INSERT INTO refresh_tokens (user_id, family_id, token_hash, expires_at) VALUES ($1, gen_random_uui…` | 38 | 1.775 | 5.516 | 1.0 | 58.9 / 5.3 | Index Scan users_pkey → ModifyTable refresh_tokens |  |
| 4 | `INSERT INTO envelopes (recipient_id, sender_id, client_msg_id, payload) VALUES ($1, $2, $3, $4) ON CONFLICT (s…` | 154 | 0.354 | 5.126 | 1.0 | 28.4 / 0.5 | ModifyTable envelopes |  |
| 5 | `INSERT INTO users (email, password_hash, google_sub, friend_code) VALUES ($1, $2, $3, $4) RETURNING id` | 34 | 1.045 | 5.942 | 1.0 | 41.1 / 2.6 | ModifyTable users |  |
| 6 | `INSERT INTO one_time_prekeys (user_id, kind, key_id, public_key, signature) SELECT $1, $2, k, p, s FROM unnest…` | 15 | 1.107 | 5.533 | 17.1 | 282.7 / 2.1 | ModifyTable one_time_prekeys |  |
| 7 | `SELECT u.token_version, (SELECT coalesce(max(e.id), $3) FROM envelopes e WHERE e.recipient_id = u.id), ARRAY(S…` | 20 | 0.532 | 2.443 | 1.0 | 11.8 / 1.9 | Index Scan users_pkey → Index Only Scan envelopes_recipient → Bitmap Heap Scan friendships → Bitmap Index Scan friendships_requester → Bitmap Index Scan friendships_addressee |  |
| 8 | `INSERT INTO friendships (requester_id, addressee_id, status) VALUES ($1, $2, $3) RETURNING id` | 14 | 0.744 | 1.982 | 1.0 | 30.0 / 2.1 | ModifyTable friendships |  |
| 9 | `SELECT token_version FROM users WHERE id = $1` | 261 | 0.036 | 3.938 | 1.0 | 4.0 / 0.0 | Index Scan users_pkey |  |
| 10 | `SELECT id, status, requester_id = $1 FROM friendships WHERE least(requester_id, addressee_id) = least($1::uuid…` | 21 | 0.275 | 1.304 | 0.2 | 2.6 / 0.9 | Index Scan friendships_pair |  |
| 11 | `DELETE FROM one_time_prekeys WHERE id = (SELECT id FROM one_time_prekeys WHERE user_id = $1 AND kind = $2 ORDE…` | 64 | 0.085 | 0.907 | 0.7 | 15.2 / 0.0 | ModifyTable one_time_prekeys → Index Scan one_time_prekeys_user_id_kind_key_id_key → Index Scan one_time_prekeys_pkey |  |
| 12 | `SELECT blocker_id = $1 FROM blocks WHERE (blocker_id = $1 AND blocked_id = $2) OR (blocker_id = $2 AND blocked…` | 23 | 0.227 | 0.804 | 0.1 | 5.1 / 0.8 | Sort → Bitmap Heap Scan blocks → Bitmap Index Scan blocks_blocked → Bitmap Index Scan blocks_blocked | ⚠ Sort ((blocker_id = '86b486e3-0797-4f8a-9b5e-747ca9fc0fe9'::uuid)) DESC (0 linhas) |
| 13 | `SELECT u.id, u.friend_code FROM friendships f JOIN users u ON u.id = CASE WHEN f.requester_id = $1 THEN f.addr…` | 14 | 0.301 | 2.390 | 1.0 | 14.4 / 0.3 | Bitmap Heap Scan friendships → Bitmap Index Scan friendships_requester → Bitmap Index Scan friendships_addressee → Index Scan users_pkey → Bitmap Heap Scan blocks → Bitmap Index Scan blocks_pkey → Bitmap Index Scan blocks_blocked |  |
| 14 | `SELECT f.id, f.status, f.requester_id = $1, u.id, u.friend_code, f.created_at FROM friendships f JOIN users u …` | 11 | 0.253 | 1.310 | 95.9 | 392.5 / 0.4 | Sort → Bitmap Heap Scan friendships → Bitmap Index Scan friendships_requester → Bitmap Index Scan friendships_addressee → Index Scan users_pkey | ⚠ Sort f.created_at,f.id (500 linhas) |
| 15 | `SELECT id, id::text AS id_text, sender_id::text, client_msg_id, payload FROM envelopes WHERE recipient_id = $1…` | 33 | 0.078 | 0.730 | 6.2 | 8.4 / 0.0 | Index Scan envelopes_recipient |  |
| 16 | `INSERT INTO devices (user_id, registration_id, identity_key, signed_prekey_id, signed_prekey, signed_prekey_si…` | 9 | 0.276 | 1.492 | 1.0 | 12.2 / 0.3 | ModifyTable devices |  |
| 17 | `DELETE FROM envelopes WHERE recipient_id = $1 AND sender_id = $2 AND client_msg_id = $3` | 155 | 0.015 | 0.371 | 1.0 | 5.0 / 0.0 | ModifyTable envelopes → Index Scan envelopes_sender_id_recipient_id_client_msg_id_key |  |
| 18 | `INSERT INTO blocks (blocker_id, blocked_id) SELECT $1, id FROM users WHERE id = $2 ON CONFLICT DO NOTHING` | 4 | 0.590 | 1.225 | 0.5 | 19.8 / 1.8 | ModifyTable blocks → Index Only Scan users_pkey |  |
| 19 | `CREATE TABLE IF NOT EXISTS schema_version ( version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAU…` | 17 | 0.138 | 0.242 | 0.0 | 7.9 / 0.1 | sem plano no log |  |
| 20 | `SELECT email, friend_code FROM users WHERE id = $1` | 38 | 0.047 | 1.368 | 1.0 | 4.0 / 0.0 | Index Scan users_pkey |  |
| 21 | `SELECT count(*) FILTER (WHERE requester_id = $1), count(*) FILTER (WHERE addressee_id = $2) FROM friendships W…` | 17 | 0.095 | 0.389 | 1.0 | 5.4 / 1.6 | Bitmap Heap Scan friendships → Bitmap Index Scan friendships_requester → Bitmap Index Scan friendships_addressee |  |
| 22 | `SELECT user_id FROM refresh_tokens WHERE token_hash = $1` | 21 | 0.073 | 0.431 | 0.2 | 3.1 / 0.3 | Index Scan refresh_tokens_token_hash_key |  |
| 23 | `SELECT EXISTS (SELECT $3 FROM friendships WHERE status = $4 AND least(requester_id, addressee_id) = least($1::…` | 42 | 0.036 | 0.388 | 1.0 | 7.0 / 0.1 | Index Scan friendships_pair → Bitmap Heap Scan blocks → Bitmap Index Scan blocks_blocked → Bitmap Index Scan blocks_blocked |  |
| 24 | `SELECT count(*) FILTER (WHERE requester_id = $1 OR addressee_id = $1), count(*) FILTER (WHERE requester_id = $…` | 26 | 0.056 | 0.173 | 1.0 | 12.2 / 0.8 | Bitmap Heap Scan friendships → Bitmap Index Scan friendships_requester → Bitmap Index Scan friendships_addressee |  |
| 25 | `SELECT registration_id, identity_key, signed_prekey_id, signed_prekey, signed_prekey_sig, kyber_last_resort_id…` | 33 | 0.043 | 0.941 | 1.0 | 4.8 / 0.1 | Index Scan devices_pkey |  |
| 26 | `INSERT INTO refresh_tokens (user_id, family_id, token_hash, expires_at) VALUES ($1, $2, $3, $4)` | 3 | 0.459 | 0.627 | 1.0 | 17.7 / 1.3 | ModifyTable refresh_tokens |  |
| 27 | `SELECT id FROM users WHERE friend_code = $1` | 36 | 0.030 | 0.513 | 0.7 | 3.6 / 0.0 | Index Scan users_friend_code_key |  |
| 28 | `SELECT identity_key FROM devices WHERE user_id = $1` | 9 | 0.113 | 0.384 | 0.7 | 3.9 / 0.4 | Index Scan devices_pkey |  |
| 29 | `SELECT count(*) FILTER (WHERE kind = $2), count(*) FILTER (WHERE kind = $3) FROM one_time_prekeys WHERE user_i…` | 12 | 0.078 | 0.583 | 1.0 | 9.8 / 0.1 | Index Only Scan one_time_prekeys_user_id_kind_key_id_key |  |
| 30 | `SELECT id, password_hash, google_sub FROM users WHERE email = $1` | 26 | 0.035 | 0.290 | 0.2 | 3.2 / 0.0 | Index Scan users_email_key |  |
| 31 | `DELETE FROM envelopes WHERE (recipient_id = $1 AND sender_id = $2) OR (recipient_id = $2 AND sender_id = $1)` | 3 | 0.179 | 0.363 | 0.0 | 4.0 / 2.0 | ModifyTable envelopes → Bitmap Heap Scan envelopes → Bitmap Index Scan envelopes_sender_id_recipient_id_client_msg_id_key → Bitmap Index Scan envelopes_sender_id_recipient_id_client_msg_id_key |  |
| 32 | `UPDATE friendships SET status = $2 WHERE id = $1` | 11 | 0.033 | 0.060 | 1.0 | 7.0 / 0.0 | ModifyTable friendships → Index Scan friendships_pkey |  |
| 33 | `SELECT requester_id FROM friendships WHERE id = $1 AND addressee_id = $2 AND status = $3` | 14 | 0.026 | 0.103 | 0.7 | 3.9 / 0.1 | Index Scan friendships_pkey |  |
| 34 | `UPDATE users SET token_version = token_version + $2 WHERE id = $1` | 4 | 0.078 | 0.153 | 1.0 | 20.5 / 0.0 | ModifyTable users → Index Scan users_pkey |  |
| 35 | `WITH d AS (DELETE FROM friendships WHERE least(requester_id, addressee_id) = least($1::uuid, $2::uuid) AND gre…` | 6 | 0.040 | 0.120 | 1.0 | 5.3 / 0.0 | ModifyTable friendships → Index Scan friendships_pair |  |
| 36 | `SELECT coalesce(max(version), $1) FROM schema_version` | 17 | 0.012 | 0.056 | 1.0 | 0.9 / 0.1 | Seq Scan schema_version |  |
| 37 | `SELECT (CASE WHEN requester_id = $1 THEN addressee_id ELSE requester_id END)::text FROM friendships WHERE stat…` | 4 | 0.049 | 0.087 | 0.2 | 7.0 / 0.0 | Bitmap Heap Scan friendships → Bitmap Index Scan friendships_requester → Bitmap Index Scan friendships_addressee |  |
| 38 | `SELECT requester_id FROM friendships WHERE id = $1 AND addressee_id = $2 AND status = $3 FOR UPDATE` | 10 | 0.018 | 0.035 | 1.0 | 5.0 / 0.0 | Index Scan friendships_pkey |  |
| 39 | `SELECT pg_advisory_xact_lock(hashtextextended(least($1::uuid, $2::uuid)::text \|\| greatest($1::uuid, $2::uuid…` | 37 | 0.005 | 0.033 | 1.0 | 0.0 / 0.0 |  |  |
| 40 | `SELECT user_id, family_id FROM refresh_tokens WHERE token_hash = $1` | 15 | 0.011 | 0.019 | 0.1 | 3.1 / 0.0 | Index Scan refresh_tokens_token_hash_key |  |
| 41 | `DELETE FROM envelopes WHERE recipient_id = $1 AND id = ANY($2)` | 10 | 0.011 | 0.055 | 0.0 | 0.6 / 0.0 | ModifyTable envelopes → Index Scan envelopes_recipient |  |
| 42 | `SELECT token_version FROM users WHERE id = $1 FOR NO KEY UPDATE` | 5 | 0.017 | 0.022 | 1.0 | 5.0 / 0.0 | Index Scan users_pkey |  |
| 43 | `UPDATE refresh_tokens SET used_at = $2 WHERE token_hash = $1` | 3 | 0.023 | 0.027 | 1.0 | 7.0 / 0.0 | ModifyTable refresh_tokens → Index Scan refresh_tokens_token_hash_key |  |
| 44 | `SELECT family_id, expires_at, used_at FROM refresh_tokens WHERE token_hash = $1 FOR UPDATE` | 5 | 0.013 | 0.017 | 1.0 | 5.0 / 0.0 | Index Scan refresh_tokens_token_hash_key |  |
| 45 | `DELETE FROM refresh_tokens WHERE family_id = $1` | 4 | 0.016 | 0.026 | 1.8 | 5.8 / 0.0 | ModifyTable refresh_tokens → Index Scan refresh_tokens_family |  |
| 46 | `DELETE FROM one_time_prekeys WHERE user_id = $1` | 1 | 0.042 | 0.042 | 6.0 | 11.0 / 0.0 | ModifyTable one_time_prekeys → Bitmap Heap Scan one_time_prekeys → Bitmap Index Scan one_time_prekeys_user_id_kind_key_id_key |  |
| 47 | `SELECT pg_advisory_xact_lock(hashtextextended($2 \|\| $1::text, $3))` | 9 | 0.005 | 0.013 | 1.0 | 0.0 / 0.0 |  |  |
| 48 | `SELECT current_database()` | 1 | 0.039 | 0.039 | 1.0 | 0.0 / 0.0 |  |  |
| 49 | `DELETE FROM friendships WHERE id = $1 AND status = $3 AND (requester_id = $2 OR addressee_id = $2)` | 2 | 0.014 | 0.017 | 0.5 | 4.5 / 0.0 | ModifyTable friendships → Index Scan friendships_pkey |  |
| 50 | `DELETE FROM blocks WHERE blocker_id = $1 AND blocked_id = $2` | 2 | 0.008 | 0.010 | 0.5 | 3.5 / 0.0 | ModifyTable blocks → Index Scan blocks_blocked |  |
| 51 | `SELECT EXISTS (SELECT $2 FROM users WHERE id = $1)` | 2 | 0.006 | 0.006 | 1.0 | 4.0 / 0.0 | Index Only Scan users_pkey |  |

Só no auto_explain (aninhadas ou sem pg_stat_statements):

| Query | Execuções | Pior ms | Plano | Alerta |
|---|---|---|---|---|
| `SELECT ? FROM ONLY "public"."users" x WHERE "id" OPERATOR(pg_catalog.=) ? FOR KEY SHARE OF x` | 1839 | 0.147 | Index Scan users_pkey |  |
| `SELECT pg_reload_conf()` | 1 | 0.014 |  |  |
