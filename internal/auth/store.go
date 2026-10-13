package auth

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayranoliveira1/pokelocation-server/internal/friends"
)

// Store: Postgres do auth. Concreto, sem interface: os testes usam Postgres real (testdb).
type Store struct {
	pool *pgxpool.Pool
}

var errNotFound = errors.New("auth: não encontrado")

func uniqueViolation(err error, constraint string) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23505" && pg.ConstraintName == constraint
}

// createUser insere o usuário com um friend_code novo (repete se colidir). nil = NULL.
func (s *Store) createUser(ctx context.Context, email, hash, sub *string) (string, error) {
	var id string
	var err error
	for range 5 { // 32^8 códigos: colisão é raríssima; o teto só evita laço infinito
		err = s.pool.QueryRow(ctx, `INSERT INTO users (email, password_hash, google_sub, friend_code)
			VALUES ($1, $2, $3, $4) RETURNING id`, email, hash, sub, friends.NewCode()).Scan(&id)
		if uniqueViolation(err, "users_email_key") {
			return "", errEmailTaken
		}
		if !uniqueViolation(err, "users_friend_code_key") {
			break
		}
	}
	return id, err
}

// userByEmail: id "" se não existe. hash/sub nil = NULL.
func (s *Store) userByEmail(ctx context.Context, email string) (id string, hash, sub *string, err error) {
	err = s.pool.QueryRow(ctx, `SELECT id, password_hash, google_sub FROM users WHERE email = $1`, email).
		Scan(&id, &hash, &sub)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil, nil, nil
	}
	return id, hash, sub, err
}

// userBySub: "" se não existe.
func (s *Store) userBySub(ctx context.Context, sub string) (string, error) {
	var id string
	err := s.pool.QueryRow(ctx, `SELECT id FROM users WHERE google_sub = $1`, sub).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return id, err
}

// version: token_version do usuário. Inexistente (ou id que nem é uuid) → errNotFound.
func (s *Store) version(ctx context.Context, id string) (int, error) {
	var v int
	err := s.pool.QueryRow(ctx, `SELECT token_version FROM users WHERE id = $1`, id).Scan(&v)
	var pg *pgconn.PgError
	if errors.Is(err, pgx.ErrNoRows) || errors.As(err, &pg) && pg.Code == "22P02" {
		return v, errNotFound
	}
	return v, err
}

// linkGoogle vincula o sub a uma conta sem sub e a entrega limpa ao dono do email (api.md §2): apaga a senha, mata os
// access (token_version), refresh, amizades, bloqueios, aparelho e prekeys. false = a conta já tem outro sub.
// UPDATE users antes dos DELETE: espera a rotação em voo (ver rotate).
// ponytail: sem o advisory lock dos pares do friends; um pedido de amizade do invasor já autenticado e em voo pode
// gravar depois do DELETE. Janela de milissegundos; travar cada par (pg_advisory_xact_lock) se virar caso real.
// ponytail: idem para um login do invasor com a senha velha que passa no argon2 logo antes do vínculo e grava a família
// logo depois (login tem 5/min por email). Se virar caso real: no login, gravar o refresh com
// INSERT … SELECT … FROM users WHERE id = $1 AND password_hash = $2 FOR NO KEY UPDATE (0 linhas → invalid_credentials).
// Devolve os ex-amigos (pedidos inclusos): a lista de amigos em memória deles no relay tem de ser recarregada.
func (s *Store) linkGoogle(ctx context.Context, id, sub string) (bool, []string, error) {
	ok := false
	var ex []string
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE users SET google_sub = $2, password_hash = NULL,
			token_version = token_version + 1 WHERE id = $1 AND google_sub IS NULL`, id, sub)
		if err != nil || tag.RowsAffected() == 0 {
			return err
		}
		ok = true
		rows, err := tx.Query(ctx, `DELETE FROM friendships WHERE requester_id = $1 OR addressee_id = $1
			RETURNING (CASE WHEN requester_id = $1 THEN addressee_id ELSE requester_id END)::text`, id)
		if err != nil {
			return err
		}
		if ex, err = pgx.CollectRows(rows, pgx.RowTo[string]); err != nil {
			return err
		}
		for _, q := range []string{
			`DELETE FROM refresh_tokens WHERE user_id = $1`,
			`DELETE FROM blocks WHERE blocker_id = $1 OR blocked_id = $1`,
			`DELETE FROM devices WHERE user_id = $1`,
			`DELETE FROM one_time_prekeys WHERE user_id = $1`,
			`DELETE FROM envelopes WHERE recipient_id = $1 OR sender_id = $1`,
		} {
			if _, err := tx.Exec(ctx, q, id); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return false, nil, err
	}
	return ok, ex, nil
}

func (s *Store) me(ctx context.Context, id string) (email *string, code string, err error) {
	err = s.pool.QueryRow(ctx, `SELECT email, friend_code FROM users WHERE id = $1`, id).Scan(&email, &code)
	if errors.Is(err, pgx.ErrNoRows) {
		err = errNotFound
	}
	return email, code, err
}

// newRefresh abre uma família nova (um login) e devolve o token_version para o access.
func (s *Store) newRefresh(ctx context.Context, userID string, hash []byte, exp time.Time) (int, error) {
	var v int
	err := s.pool.QueryRow(ctx, `WITH r AS (
			INSERT INTO refresh_tokens (user_id, family_id, token_hash, expires_at)
			VALUES ($1, gen_random_uuid(), $2, $3))
		SELECT token_version FROM users WHERE id = $1`, userID, hash, exp).Scan(&v)
	return v, err
}

// Travas: sempre o usuário (users) antes dos refresh. O rotate trava o usuário com FOR NO KEY UPDATE; revoke, reuso e
// vínculo fazem o UPDATE users antes do DELETE. Assim uma revogação espera a rotação em voo e o DELETE seguinte (comando
// novo = snapshot novo em READ COMMITTED) enxerga o sucessor que ela inseriu; e uma rotação que chega depois espera a
// revogação e não acha mais o token. Ordem fixa: sem deadlock.

// rotate troca old por next na mesma família e devolve o dono e o token_version. owner "" = desconhecido, vencido ou
// reusado; reuso apaga a família inteira, mata os access do dono (token_version+1) e o devolve em revoked.
func (s *Store) rotate(ctx context.Context, old, next []byte, now, exp time.Time) (owner string, version int,
	revoked string, err error) {
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var user, family string
		var v int
		var expires time.Time
		var used *time.Time
		err := tx.QueryRow(ctx, `SELECT user_id FROM refresh_tokens WHERE token_hash = $1`, old).Scan(&user)
		if err == nil {
			err = tx.QueryRow(ctx, `SELECT token_version FROM users WHERE id = $1 FOR NO KEY UPDATE`, user).Scan(&v)
		}
		if err == nil { // relido sob a trava do usuário: uma revogação que commitou enquanto esperávamos já o apagou
			err = tx.QueryRow(ctx, `SELECT family_id, expires_at, used_at FROM refresh_tokens
				WHERE token_hash = $1 FOR UPDATE`, old).Scan(&family, &expires, &used)
		}
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			return nil
		case err != nil:
			return err
		case used != nil:
			if _, err := tx.Exec(ctx, `UPDATE users SET token_version = token_version + 1 WHERE id = $1`, user); err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `DELETE FROM refresh_tokens WHERE family_id = $1`, family)
			revoked = user
			return err
		case !now.Before(expires):
			return nil
		}
		if _, err := tx.Exec(ctx, `UPDATE refresh_tokens SET used_at = $2 WHERE token_hash = $1`, old, now); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO refresh_tokens (user_id, family_id, token_hash, expires_at)
			VALUES ($1, $2, $3, $4)`, user, family, next, exp); err != nil {
			return err
		}
		owner, version = user, v
		return nil
	})
	if err != nil {
		return "", 0, "", err
	}
	return owner, version, revoked, nil
}

// revoke apaga a família do token e mata os access do dono (logout); devolve o dono. Token desconhecido: "".
func (s *Store) revoke(ctx context.Context, hash []byte) (string, error) {
	var owner string
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var user, family string
		err := tx.QueryRow(ctx, `SELECT user_id, family_id FROM refresh_tokens WHERE token_hash = $1`, hash).
			Scan(&user, &family)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE users SET token_version = token_version + 1 WHERE id = $1`, user); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `DELETE FROM refresh_tokens WHERE family_id = $1`, family)
		owner = user
		return err
	})
	if err != nil {
		return "", err
	}
	return owner, nil
}

// cleanup apaga os refresh vencidos (usados ficam até vencer: é com eles que o reuso é detectado).
func (s *Store) cleanup(ctx context.Context, now time.Time) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM refresh_tokens WHERE expires_at < $1`, now)
	return err
}
