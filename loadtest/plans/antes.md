# Planos antes da 0003

Servidor 1cb9e92 sobre o seed de loadtest/seed (200 mil usuários, 2 M de amizades, 1 M de envelopes, 4 M de one-time prekeys); apitest inteiro + loadtest -n 2000 -measure 30s. Planos do auto_explain (pior execução de cada query) juntados ao pg_stat_statements. A limpeza diária ainda rodava na subida do servidor.

| # | Query | Chamadas | Média ms | Máx ms | Linhas/chamada | Buffers hit/read por chamada | Plano (pior caso) | Alerta |
|---|---|---|---|---|---|---|---|---|
| 1 | `DELETE FROM envelopes WHERE created_at < now() - interval $1` | 2 | 3110.786 | 5573.708 | 61.5 | 1017.0 / 46419.0 | ModifyTable envelopes → Seq Scan envelopes | ⚠ Seq Scan envelopes (1000293 linhas) |
| 2 | `DELETE FROM refresh_tokens WHERE expires_at < $1` | 16 | 228.786 | 422.168 | 0.0 | 2510.2 / 3735.8 | ModifyTable refresh_tokens → Seq Scan refresh_tokens | ⚠ Seq Scan refresh_tokens (266063 linhas) |
| 3 | `copy "friendships" ( "requester_id", "addressee_id", "status" ) from stdin binary` | 1 | 2053.223 | 2053.223 | 10000.0 | 231505.0 / 9402.0 | sem plano no log | (ferramenta de teste)  |
| 4 | `INSERT INTO envelopes (recipient_id, sender_id, client_msg_id, payload) VALUES ($1, $2, $3, $4) ON CONFLICT (s…` | 1499 | 1.364 | 108.306 | 1.0 | 24.7 / 0.9 | ModifyTable envelopes |  |
| 5 | `WITH nu AS ( INSERT INTO users (email, friend_code) SELECT $5 \|\| gen_random_uuid() \|\| $6, (SELECT string_a…` | 3 | 611.023 | 1453.577 | 200.0 | 6838.7 / 670.7 | ModifyTable friendships → ModifyTable users | (ferramenta de teste)  |
| 6 | `SELECT coalesce(max(id), $2) FROM envelopes WHERE recipient_id = $1` | 2020 | 0.800 | 20.115 | 1.0 | 2.1 / 0.9 | Index Only Scan envelopes_recipient |  |
| 7 | `DELETE FROM users WHERE email LIKE $1` | 2 | 746.011 | 1344.527 | 1000.0 | 44305.5 / 4776.0 | ModifyTable users → Seq Scan users | (ferramenta de teste) ⚠ Seq Scan users (200000 linhas) |
| 8 | `SELECT pg_advisory_xact_lock($1)` | 18 | 41.517 | 214.971 | 1.0 | 0.0 / 0.0 |  |  |
| 9 | `WITH r AS ( INSERT INTO refresh_tokens (user_id, family_id, token_hash, expires_at) VALUES ($1, gen_random_uui…` | 38 | 10.639 | 44.070 | 1.0 | 48.3 / 4.8 | Index Scan users_pkey → ModifyTable refresh_tokens |  |
| 10 | `SELECT (CASE WHEN requester_id = $1 THEN addressee_id ELSE requester_id END)::text FROM friendships WHERE stat…` | 2024 | 0.172 | 17.635 | 9.9 | 7.3 / 0.0 | Bitmap Heap Scan friendships → Bitmap Index Scan friendships_requester → Bitmap Index Scan friendships_addressee |  |
| 11 | `SELECT token_version FROM users WHERE id = $1` | 4298 | 0.055 | 5.417 | 1.0 | 4.0 / 0.0 | Index Scan users_pkey |  |
| 12 | `SELECT blocker_id = $1 FROM blocks WHERE (blocker_id = $1 AND blocked_id = $2) OR (blocker_id = $2 AND blocked…` | 23 | 6.979 | 34.687 | 0.1 | 5.0 / 1.0 | Sort → Bitmap Heap Scan blocks → Bitmap Index Scan blocks_blocked → Bitmap Index Scan blocks_blocked | ⚠ Sort ((blocker_id = 'b78fc3d0-6e9a-4b36-a27a-927dc12b7b86'::uuid)) DESC (0 linhas) |
| 13 | `DELETE FROM envelopes WHERE recipient_id = $1 AND sender_id = $2 AND client_msg_id = $3` | 1500 | 0.081 | 30.753 | 1.0 | 5.0 / 0.0 | ModifyTable envelopes → Index Scan envelopes_sender_id_recipient_id_client_msg_id_key |  |
| 14 | `INSERT INTO users (email, password_hash, google_sub, friend_code) VALUES ($1, $2, $3, $4) RETURNING id` | 34 | 3.560 | 19.603 | 1.0 | 42.0 / 2.5 | ModifyTable users |  |
| 15 | `INSERT INTO friendships (requester_id, addressee_id, status) VALUES ($1, $2, $3) RETURNING id` | 14 | 8.444 | 40.470 | 1.0 | 29.5 / 1.4 | ModifyTable friendships |  |
| 16 | `copy "users" ( "id", "email", "friend_code" ) from stdin binary` | 1 | 102.372 | 102.372 | 2000.0 | 23438.0 / 1002.0 | sem plano no log | (ferramenta de teste)  |
| 17 | `SELECT id, id::text AS id_text, sender_id::text, client_msg_id, payload FROM envelopes WHERE recipient_id = $1…` | 2035 | 0.050 | 2.983 | 0.1 | 3.1 / 0.0 | Index Scan envelopes_recipient |  |
| 18 | `SELECT id, status, requester_id = $1 FROM friendships WHERE least(requester_id, addressee_id) = least($1::uuid…` | 21 | 4.253 | 33.239 | 0.2 | 3.0 / 0.6 | Index Scan friendships_pair |  |
| 19 | `INSERT INTO one_time_prekeys (user_id, kind, key_id, public_key, signature) SELECT $1, $2, k, p, s FROM unnest…` | 15 | 5.370 | 24.699 | 17.1 | 282.5 / 2.0 | ModifyTable one_time_prekeys |  |
| 20 | `SELECT count(*) FILTER (WHERE requester_id = $1), count(*) FILTER (WHERE addressee_id = $2) FROM friendships W…` | 17 | 4.674 | 27.033 | 1.0 | 6.5 / 0.6 | Bitmap Heap Scan friendships → Bitmap Index Scan friendships_requester → Bitmap Index Scan friendships_addressee |  |
| 21 | `DELETE FROM one_time_prekeys WHERE id = (SELECT id FROM one_time_prekeys WHERE user_id = $1 AND kind = $2 ORDE…` | 64 | 0.484 | 6.912 | 0.7 | 15.6 / 0.0 | ModifyTable one_time_prekeys → Sort → Bitmap Heap Scan one_time_prekeys → Bitmap Index Scan one_time_prekeys_user_id_kind_key_id_key → Index Scan one_time_prekeys_pkey | ⚠ Sort one_time_prekeys_1.id (3 linhas) |
| 22 | `SELECT f.id, f.status, f.requester_id = $1, u.id, u.friend_code, f.created_at FROM friendships f JOIN users u …` | 11 | 1.502 | 5.384 | 95.9 | 392.9 / 0.4 | Sort → Bitmap Heap Scan friendships → Bitmap Index Scan friendships_requester → Bitmap Index Scan friendships_addressee → Index Scan users_pkey | ⚠ Sort f.created_at,f.id (0 linhas) |
| 23 | `SELECT EXISTS (SELECT $3 FROM friendships WHERE status = $4 AND least(requester_id, addressee_id) = least($1::…` | 42 | 0.353 | 7.801 | 1.0 | 7.1 / 0.1 | Index Scan friendships_pair → Bitmap Heap Scan blocks → Bitmap Index Scan blocks_blocked → Bitmap Index Scan blocks_blocked |  |
| 24 | `SELECT count(*) FILTER (WHERE requester_id = $1 OR addressee_id = $1), count(*) FILTER (WHERE requester_id = $…` | 26 | 0.522 | 8.861 | 1.0 | 13.0 / 0.3 | Bitmap Heap Scan friendships → Bitmap Index Scan friendships_requester → Bitmap Index Scan friendships_addressee |  |
| 25 | `SELECT registration_id, identity_key, signed_prekey_id, signed_prekey, signed_prekey_sig, kyber_last_resort_id…` | 33 | 0.400 | 12.008 | 1.0 | 4.8 / 0.1 | Index Scan devices_pkey |  |
| 26 | `INSERT INTO devices (user_id, registration_id, identity_key, signed_prekey_id, signed_prekey, signed_prekey_si…` | 9 | 1.428 | 5.585 | 1.0 | 12.0 / 0.3 | ModifyTable devices |  |
| 27 | `INSERT INTO blocks (blocker_id, blocked_id) SELECT $1, id FROM users WHERE id = $2 ON CONFLICT DO NOTHING` | 4 | 2.898 | 9.297 | 0.5 | 20.0 / 1.5 | ModifyTable blocks → Index Only Scan users_pkey |  |
| 28 | `CREATE TABLE IF NOT EXISTS schema_version ( version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAU…` | 18 | 0.621 | 2.682 | 0.0 | 8.0 / 0.0 | sem plano no log |  |
| 29 | `INSERT INTO refresh_tokens (user_id, family_id, token_hash, expires_at) VALUES ($1, $2, $3, $4)` | 3 | 3.468 | 7.719 | 1.0 | 16.3 / 1.7 | ModifyTable refresh_tokens |  |
| 30 | `SELECT user_id FROM refresh_tokens WHERE token_hash = $1` | 21 | 0.355 | 7.041 | 0.2 | 3.1 / 0.2 | Index Scan refresh_tokens_token_hash_key |  |
| 31 | `SELECT identity_key FROM devices WHERE user_id = $1` | 9 | 0.689 | 2.944 | 0.7 | 3.6 / 0.4 | Index Scan devices_pkey |  |
| 32 | `SELECT u.id, u.friend_code FROM friendships f JOIN users u ON u.id = CASE WHEN f.requester_id = $1 THEN f.addr…` | 8 | 0.625 | 2.910 | 1.0 | 13.9 / 0.5 | Bitmap Heap Scan friendships → Bitmap Index Scan friendships_requester → Bitmap Index Scan friendships_addressee → Index Scan users_pkey → Bitmap Heap Scan blocks → Bitmap Index Scan blocks_pkey → Bitmap Index Scan blocks_blocked |  |
| 33 | `SELECT id FROM users WHERE friend_code = $1` | 36 | 0.071 | 0.567 | 0.7 | 3.6 / 0.1 | Index Scan users_friend_code_key |  |
| 34 | `SELECT count(*) FILTER (WHERE kind = $2), count(*) FILTER (WHERE kind = $3) FROM one_time_prekeys WHERE user_i…` | 12 | 0.179 | 0.501 | 1.0 | 9.8 / 0.1 | Index Only Scan one_time_prekeys_user_id_kind_key_id_key |  |
| 35 | `DELETE FROM envelopes WHERE (recipient_id = $1 AND sender_id = $2) OR (recipient_id = $2 AND sender_id = $1)` | 3 | 0.601 | 1.225 | 0.0 | 4.3 / 1.7 | ModifyTable envelopes → Bitmap Heap Scan envelopes → Bitmap Index Scan envelopes_sender_id_recipient_id_client_msg_id_key → Bitmap Index Scan envelopes_sender_id_recipient_id_client_msg_id_key |  |
| 36 | `SELECT id, password_hash, google_sub FROM users WHERE email = $1` | 26 | 0.063 | 0.638 | 0.2 | 3.2 / 0.0 | Index Scan users_email_key |  |
| 37 | `SELECT email, friend_code FROM users WHERE id = $1` | 39 | 0.036 | 0.130 | 1.0 | 4.0 / 0.0 | Index Scan users_pkey |  |
| 38 | `UPDATE friendships SET status = $2 WHERE id = $1` | 11 | 0.126 | 0.458 | 1.0 | 8.5 / 0.0 | ModifyTable friendships → Index Scan friendships_pkey |  |
| 39 | `UPDATE users SET token_version = token_version + $2 WHERE id = $1` | 4 | 0.342 | 0.570 | 1.0 | 21.2 / 0.0 | ModifyTable users → Index Scan users_pkey |  |
| 40 | `SELECT requester_id FROM friendships WHERE id = $1 AND addressee_id = $2 AND status = $3 FOR UPDATE` | 10 | 0.093 | 0.299 | 1.0 | 5.0 / 0.0 | Index Scan friendships_pkey |  |
| 41 | `SELECT coalesce(max(version), $1) FROM schema_version` | 18 | 0.046 | 0.392 | 1.0 | 0.9 / 0.1 | Seq Scan schema_version |  |
| 42 | `SELECT requester_id FROM friendships WHERE id = $1 AND addressee_id = $2 AND status = $3` | 14 | 0.057 | 0.229 | 0.7 | 4.0 / 0.0 | Index Scan friendships_pkey |  |
| 43 | `UPDATE refresh_tokens SET used_at = $2 WHERE token_hash = $1` | 3 | 0.223 | 0.529 | 1.0 | 7.0 / 0.0 | ModifyTable refresh_tokens → Index Scan refresh_tokens_token_hash_key |  |
| 44 | `SELECT pg_advisory_xact_lock(hashtextextended(least($1::uuid, $2::uuid)::text \|\| greatest($1::uuid, $2::uuid…` | 37 | 0.018 | 0.126 | 1.0 | 0.0 / 0.0 |  |  |
| 45 | `WITH d AS (DELETE FROM friendships WHERE least(requester_id, addressee_id) = least($1::uuid, $2::uuid) AND gre…` | 6 | 0.086 | 0.229 | 1.0 | 5.7 / 0.0 | ModifyTable friendships → Index Scan friendships_pair |  |
| 46 | `SELECT token_version FROM users WHERE id = $1 FOR NO KEY UPDATE` | 5 | 0.082 | 0.190 | 1.0 | 5.0 / 0.0 | Index Scan users_pkey |  |
| 47 | `SELECT user_id, family_id FROM refresh_tokens WHERE token_hash = $1` | 15 | 0.014 | 0.041 | 0.1 | 3.2 / 0.0 | Index Scan refresh_tokens_token_hash_key |  |
| 48 | `DELETE FROM one_time_prekeys WHERE user_id = $1` | 1 | 0.201 | 0.201 | 6.0 | 11.0 / 0.0 | ModifyTable one_time_prekeys → Bitmap Heap Scan one_time_prekeys → Bitmap Index Scan one_time_prekeys_user_id_kind_key_id_key |  |
| 49 | `DELETE FROM envelopes WHERE recipient_id = $1 AND id = ANY($2)` | 10 | 0.016 | 0.078 | 0.0 | 0.6 / 0.0 | ModifyTable envelopes → Index Scan envelopes_recipient |  |
| 50 | `SELECT family_id, expires_at, used_at FROM refresh_tokens WHERE token_hash = $1 FOR UPDATE` | 5 | 0.027 | 0.038 | 1.0 | 5.0 / 0.0 | Index Scan refresh_tokens_token_hash_key |  |
| 51 | `DELETE FROM refresh_tokens WHERE family_id = $1` | 4 | 0.030 | 0.040 | 1.8 | 5.8 / 0.0 | ModifyTable refresh_tokens → Index Scan refresh_tokens_family |  |
| 52 | `SELECT pg_advisory_xact_lock(hashtextextended($2 \|\| $1::text, $3))` | 9 | 0.012 | 0.028 | 1.0 | 0.0 / 0.0 |  |  |
| 53 | `SELECT EXISTS (SELECT $2 FROM users WHERE id = $1)` | 2 | 0.043 | 0.052 | 1.0 | 4.0 / 0.0 | Index Only Scan users_pkey |  |
| 54 | `DELETE FROM blocks WHERE blocker_id = $1 AND blocked_id = $2` | 2 | 0.030 | 0.040 | 0.5 | 3.5 / 0.0 | ModifyTable blocks → Index Scan blocks_blocked |  |
| 55 | `DELETE FROM friendships WHERE id = $1 AND status = $3 AND (requester_id = $2 OR addressee_id = $2)` | 2 | 0.025 | 0.034 | 0.5 | 4.5 / 0.0 | ModifyTable friendships → Index Scan friendships_pkey |  |
| 56 | `SELECT $1` | 1 | 0.004 | 0.004 | 1.0 | 0.0 / 0.0 |  |  |

Só no auto_explain (aninhadas ou sem pg_stat_statements):

| Query | Execuções | Pior ms | Plano | Alerta |
|---|---|---|---|---|
| `DELETE FROM ONLY "public"."blocks" WHERE ? OPERATOR(pg_catalog.=) "blocked_id"` | 2000 | 0.418 | ModifyTable blocks → Index Scan blocks_blocked |  |
| `DELETE FROM ONLY "public"."blocks" WHERE ? OPERATOR(pg_catalog.=) "blocker_id"` | 2000 | 0.290 | ModifyTable blocks → Index Scan blocks_pkey |  |
| `DELETE FROM ONLY "public"."devices" WHERE ? OPERATOR(pg_catalog.=) "user_id"` | 2000 | 2.167 | ModifyTable devices → Index Scan devices_pkey |  |
| `DELETE FROM ONLY "public"."envelopes" WHERE ? OPERATOR(pg_catalog.=) "recipient_id"` | 2000 | 0.665 | ModifyTable envelopes → Index Scan envelopes_recipient |  |
| `DELETE FROM ONLY "public"."envelopes" WHERE ? OPERATOR(pg_catalog.=) "sender_id"` | 2000 | 1.861 | ModifyTable envelopes → Index Scan envelopes_sender_id_recipient_id_client_msg_id_key |  |
| `DELETE FROM ONLY "public"."friendships" WHERE ? OPERATOR(pg_catalog.=) "addressee_id"` | 2000 | 0.636 | ModifyTable friendships → Bitmap Heap Scan friendships → Bitmap Index Scan friendships_addressee |  |
| `DELETE FROM ONLY "public"."friendships" WHERE ? OPERATOR(pg_catalog.=) "requester_id"` | 2000 | 0.976 | ModifyTable friendships → Bitmap Heap Scan friendships → Bitmap Index Scan friendships_requester |  |
| `DELETE FROM ONLY "public"."one_time_prekeys" WHERE ? OPERATOR(pg_catalog.=) "user_id"` | 2000 | 5.847 | ModifyTable one_time_prekeys → Bitmap Heap Scan one_time_prekeys → Bitmap Index Scan one_time_prekeys_user_id_kind_key_id_key |  |
| `DELETE FROM ONLY "public"."refresh_tokens" WHERE ? OPERATOR(pg_catalog.=) "user_id"` | 2000 | 0.471 | ModifyTable refresh_tokens → Index Scan refresh_tokens_user |  |
| `SELECT ? FROM ONLY "public"."users" x WHERE "id" OPERATOR(pg_catalog.=) ? FOR KEY SHARE OF x` | 24529 | 15.253 | Index Scan users_pkey |  |
| `SELECT pg_reload_conf()` | 1 | 0.019 |  |  |
