package pogo

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	qAccount       = `SELECT provider, token_enc FROM pogo_accounts WHERE user_id = $1`
	qUpsertAccount = `INSERT INTO pogo_accounts (user_id, provider, token_enc, display_name, trainers)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (user_id) DO UPDATE SET provider = excluded.provider, token_enc = excluded.token_enc,
			display_name = excluded.display_name, trainers = excluded.trainers, updated_at = now()`
	qDeleteAccount = `DELETE FROM pogo_accounts WHERE user_id = $1`
	// Só o token que falhou: um login novo no meio não se perde.
	qDeleteToken   = `DELETE FROM pogo_accounts WHERE user_id = $1 AND token_enc = $2`
	qUpdateProfile = `UPDATE pogo_accounts SET display_name = $3, trainers = $4, updated_at = now()
		WHERE user_id = $1 AND token_enc = $2`
	qUpdateTrainers = `UPDATE pogo_accounts SET trainers = $3, updated_at = now() WHERE user_id = $1 AND token_enc = $2`
)

type Store struct{ pool *pgxpool.Pool }

type account struct {
	provider string
	enc      []byte
}

var errNoAccount = errors.New("pogo: sem vínculo")

func (s *Store) account(ctx context.Context, user string) (account, error) {
	var a account
	err := s.pool.QueryRow(ctx, qAccount, user).Scan(&a.provider, &a.enc)
	if errors.Is(err, pgx.ErrNoRows) {
		return a, errNoAccount
	}
	return a, err
}

func (s *Store) upsert(ctx context.Context, user, provider string, enc []byte, p *Profile) error {
	_, err := s.pool.Exec(ctx, qUpsertAccount, user, provider, enc, p.DisplayName, trainersJSON(p))
	return err
}

func (s *Store) updateProfile(ctx context.Context, user string, enc []byte, p *Profile) error {
	_, err := s.pool.Exec(ctx, qUpdateProfile, user, enc, p.DisplayName, trainersJSON(p))
	return err
}

func (s *Store) updateTrainers(ctx context.Context, user string, enc []byte, p *Profile) error {
	_, err := s.pool.Exec(ctx, qUpdateTrainers, user, enc, trainersJSON(p))
	return err
}

func (s *Store) deleteAccount(ctx context.Context, user string) error {
	_, err := s.pool.Exec(ctx, qDeleteAccount, user)
	return err
}

func (s *Store) deleteToken(ctx context.Context, user string, enc []byte) error {
	_, err := s.pool.Exec(ctx, qDeleteToken, user, enc)
	return err
}

// []byte vai como JSON pronto para o jsonb.
func trainersJSON(p *Profile) []byte {
	b, _ := json.Marshal(p.Trainers)
	return b
}
