package push

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// O índice único devices_fcm_token garante: um token é de um aparelho só (troca de conta no mesmo celular).
	qClearFCMElsewhere = `UPDATE devices SET fcm_token = NULL WHERE fcm_token = $1 AND user_id <> $2`
	qSetFCM            = `UPDATE devices SET fcm_token = $2 WHERE user_id = $1`
	qGetFCMs           = `SELECT user_id::text, fcm_token FROM devices WHERE user_id = ANY($1) AND fcm_token IS NOT NULL`
	qClearFCM          = `UPDATE devices SET fcm_token = NULL WHERE user_id = $1 AND fcm_token = $2`
)

type Store struct {
	pool *pgxpool.Pool
}

// setToken: nil apaga. false = o usuário ainda não tem aparelho (o PUT /v1/keys cria). Duas contas registrando o
// mesmo token ao mesmo tempo: a perdedora bate no índice único e tenta de novo (agora tirando da vencedora).
func (s *Store) setToken(ctx context.Context, user string, token *string) (bool, error) {
	var ok bool
	var err error
	for range 3 {
		err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			if token != nil {
				if _, err := tx.Exec(ctx, qClearFCMElsewhere, *token, user); err != nil {
					return err
				}
			}
			tag, err := tx.Exec(ctx, qSetFCM, user, token)
			ok = tag.RowsAffected() == 1
			return err
		})
		var pg *pgconn.PgError
		if !errors.As(err, &pg) || pg.Code != "23505" || pg.ConstraintName != "devices_fcm_token" {
			break
		}
	}
	return ok, err
}

type target struct {
	User, Token string
}

// tokens: só quem tem aparelho e token.
func (s *Store) tokens(ctx context.Context, users []string) ([]target, error) {
	rows, err := s.pool.Query(ctx, qGetFCMs, users)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[target])
}

// clearToken só apaga se o token ainda é o que falhou: um PUT novo no meio não se perde.
func (s *Store) clearToken(ctx context.Context, user, token string) error {
	_, err := s.pool.Exec(ctx, qClearFCM, user, token)
	return err
}
