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

type Store struct {
	pool *pgxpool.Pool
}

const (
	qInsertUser = `INSERT INTO users (email, password_hash, google_sub, friend_code)
			VALUES ($1, $2, $3, $4) RETURNING id`
	qUserByEmail = `SELECT id, password_hash, google_sub FROM users WHERE email = $1`
	qUserBySub   = `SELECT id FROM users WHERE google_sub = $1`
	qVersion     = `SELECT token_version FROM users WHERE id = $1`
	qLinkUser    = `UPDATE users SET google_sub = $2, password_hash = NULL,
			token_version = token_version + 1 WHERE id = $1 AND google_sub IS NULL`
	qLinkFriendships = `DELETE FROM friendships WHERE requester_id = $1 OR addressee_id = $1
			RETURNING (CASE WHEN requester_id = $1 THEN addressee_id ELSE requester_id END)::text`
	qDelRefreshOfUser   = `DELETE FROM refresh_tokens WHERE user_id = $1`
	qDelBlocksOfUser    = `DELETE FROM blocks WHERE blocker_id = $1 OR blocked_id = $1`
	qDelDeviceOfUser    = `DELETE FROM devices WHERE user_id = $1`
	qDelPrekeysOfUser   = `DELETE FROM one_time_prekeys WHERE user_id = $1`
	qDelEnvelopesOfUser = `DELETE FROM envelopes WHERE recipient_id = $1 OR sender_id = $1`
	qMe                 = `SELECT email, friend_code FROM users WHERE id = $1`
	qNewRefresh         = `WITH r AS (
			INSERT INTO refresh_tokens (user_id, family_id, token_hash, expires_at)
			VALUES ($1, gen_random_uuid(), $2, $3))
		SELECT token_version FROM users WHERE id = $1`
	qRefreshOwner     = `SELECT user_id FROM refresh_tokens WHERE token_hash = $1`
	qLockUser         = `SELECT token_version FROM users WHERE id = $1 FOR NO KEY UPDATE`
	qRefreshForUpdate = `SELECT family_id, expires_at, used_at FROM refresh_tokens
				WHERE token_hash = $1 FOR UPDATE`
	qBumpVersion   = `UPDATE users SET token_version = token_version + 1 WHERE id = $1`
	qDelFamily     = `DELETE FROM refresh_tokens WHERE family_id = $1`
	qMarkUsed      = `UPDATE refresh_tokens SET used_at = $2 WHERE token_hash = $1`
	qInsertRefresh = `INSERT INTO refresh_tokens (user_id, family_id, token_hash, expires_at)
			VALUES ($1, $2, $3, $4)`
	qRevokeLookup = `SELECT user_id, family_id FROM refresh_tokens WHERE token_hash = $1`
	qCleanup      = `DELETE FROM refresh_tokens WHERE id = ANY(ARRAY(
			SELECT id FROM refresh_tokens WHERE expires_at < $1 ORDER BY expires_at LIMIT $2))`
)

var errNotFound = errors.New("auth: não encontrado")

func uniqueViolation(err error, constraint string) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23505" && pg.ConstraintName == constraint
}

// createUser: nil = NULL.
func (s *Store) createUser(ctx context.Context, email, hash, sub *string) (string, error) {
	var id string
	var err error
	for range 5 { // colisão em 32^8 é raríssima; o teto só evita laço infinito
		err = s.pool.QueryRow(ctx, qInsertUser, email, hash, sub, friends.NewCode()).Scan(&id)
		if uniqueViolation(err, "users_email_key") {
			return "", errEmailTaken
		}
		if !uniqueViolation(err, "users_friend_code_key") {
			break
		}
	}
	return id, err
}

// userByEmail: id "" se não existe.
func (s *Store) userByEmail(ctx context.Context, email string) (id string, hash, sub *string, err error) {
	err = s.pool.QueryRow(ctx, qUserByEmail, email).
		Scan(&id, &hash, &sub)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil, nil, nil
	}
	return id, hash, sub, err
}

func (s *Store) userBySub(ctx context.Context, sub string) (string, error) {
	var id string
	err := s.pool.QueryRow(ctx, qUserBySub, sub).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return id, err
}

// version: id que nem é uuid também é errNotFound (o sub vem do token, não do banco).
func (s *Store) version(ctx context.Context, id string) (int, error) {
	var v int
	err := s.pool.QueryRow(ctx, qVersion, id).Scan(&v)
	var pg *pgconn.PgError
	if errors.Is(err, pgx.ErrNoRows) || errors.As(err, &pg) && pg.Code == "22P02" {
		return v, errNotFound
	}
	return v, err
}

// linkGoogle entrega a conta limpa: nada que o dono antigo do email deixou (senha, sessões, amigos, chaves, fila)
// sobrevive. false = a conta já tem outro sub. Devolve os ex-amigos: as listas em memória deles têm de ser recarregadas.
// UPDATE users antes dos DELETE: espera a rotação em voo (ver rotate).
// ponytail: sem os advisory locks de par do friends; um pedido de amizade do invasor já autenticado e em voo pode
// gravar depois do DELETE. Janela de milissegundos; travar cada par (pg_advisory_xact_lock) se virar caso real.
// ponytail: idem para um login do invasor com a senha velha que passa no argon2 logo antes do vínculo e grava a família
// logo depois (login tem 5/min por email). Se virar caso real: no login, gravar o refresh com
// INSERT … SELECT … FROM users WHERE id = $1 AND password_hash = $2 FOR NO KEY UPDATE (0 linhas → invalid_credentials).
func (s *Store) linkGoogle(ctx context.Context, id, sub string) (bool, []string, error) {
	ok := false
	var ex []string
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, qLinkUser, id, sub)
		if err != nil || tag.RowsAffected() == 0 {
			return err
		}
		ok = true
		rows, err := tx.Query(ctx, qLinkFriendships, id)
		if err != nil {
			return err
		}
		if ex, err = pgx.CollectRows(rows, pgx.RowTo[string]); err != nil {
			return err
		}
		for _, q := range []string{
			qDelRefreshOfUser,
			qDelBlocksOfUser,
			qDelDeviceOfUser,
			qDelPrekeysOfUser,
			qDelEnvelopesOfUser,
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
	err = s.pool.QueryRow(ctx, qMe, id).Scan(&email, &code)
	if errors.Is(err, pgx.ErrNoRows) {
		err = errNotFound
	}
	return email, code, err
}

// newRefresh: família nova (um login).
func (s *Store) newRefresh(ctx context.Context, userID string, hash []byte, exp time.Time) (int, error) {
	var v int
	err := s.pool.QueryRow(ctx, qNewRefresh, userID, hash, exp).Scan(&v)
	return v, err
}

// Travas: sempre o usuário (users) antes dos refresh. O rotate trava o usuário com FOR NO KEY UPDATE; revoke, reuso e
// vínculo fazem o UPDATE users antes do DELETE. Assim uma revogação espera a rotação em voo e o DELETE seguinte (comando
// novo = snapshot novo em READ COMMITTED) enxerga o sucessor que ela inseriu; e uma rotação que chega depois espera a
// revogação e não acha mais o token. Ordem fixa: sem deadlock.

// rotate: owner "" = desconhecido, vencido ou reusado. Reuso apaga a família inteira e mata os access do dono, que
// volta em revoked.
func (s *Store) rotate(ctx context.Context, old, next []byte, now, exp time.Time) (owner string, version int,
	revoked string, err error) {
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var user, family string
		var v int
		var expires time.Time
		var used *time.Time
		err := tx.QueryRow(ctx, qRefreshOwner, old).Scan(&user)
		if err == nil {
			err = tx.QueryRow(ctx, qLockUser, user).Scan(&v)
		}
		if err == nil { // relido sob a trava: uma revogação que comitou enquanto esperávamos já o apagou
			err = tx.QueryRow(ctx, qRefreshForUpdate, old).Scan(&family, &expires, &used)
		}
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			return nil
		case err != nil:
			return err
		case used != nil:
			if _, err := tx.Exec(ctx, qBumpVersion, user); err != nil {
				return err
			}
			_, err = tx.Exec(ctx, qDelFamily, family)
			revoked = user
			return err
		case !now.Before(expires):
			return nil
		}
		if _, err := tx.Exec(ctx, qMarkUsed, old, now); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, qInsertRefresh, user, family, next, exp); err != nil {
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

// revoke: token desconhecido devolve "".
func (s *Store) revoke(ctx context.Context, hash []byte) (string, error) {
	var owner string
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var user, family string
		err := tx.QueryRow(ctx, qRevokeLookup, hash).
			Scan(&user, &family)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, qBumpVersion, user); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, qDelFamily, family)
		owner = user
		return err
	})
	if err != nil {
		return "", err
	}
	return owner, nil
}

const (
	cleanupBatch = 1000
	batchWait    = 5 * time.Second
)

// cleanup em lotes curtos, cada um com prazo próprio: um DELETE único passaria do statement_timeout com muitos
// vencidos. Os usados ficam até vencer, porque é com eles que o reuso é detectado.
func (s *Store) cleanup(ctx context.Context, now time.Time) error {
	for {
		bctx, cancel := context.WithTimeout(ctx, batchWait)
		// ORDER BY faz o lote andar pelo índice de expires_at, e o ANY(ARRAY) apaga pela PK: sem isso, cada lote relia a
		// tabela desde o começo.
		tag, err := s.pool.Exec(bctx, qCleanup, now, cleanupBatch)
		cancel()
		if err != nil || tag.RowsAffected() < cleanupBatch {
			return err
		}
	}
}
