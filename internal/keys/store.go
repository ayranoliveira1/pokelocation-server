// Package keys guarda e entrega o pacote de chaves públicas do Signal (docs/api.md §4). Sem regra além da validação:
// handler + store, sem service.
package keys

import (
	"bytes"
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const maxStored = 200 // one-time guardadas por tipo

var (
	errForbidden = errors.New("forbidden")
	errNotFound  = errors.New("not_found")
	errTooMany   = errors.New("too many one-time prekeys")
)

type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool} }

// put grava/substitui o aparelho e acrescenta as one-time. identity nova (reinstalação) apaga as one-time antigas.
// Tudo ou nada: passou de maxStored num tipo, nada muda.
func (s *Store) put(ctx context.Context, user string, b *bundle) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// serializa PUTs do mesmo usuário (inclusive o primeiro, quando ainda não há linha para FOR UPDATE)
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('keys:' || $1::text, 0))`, user); err != nil {
			return err
		}
		var old []byte
		err := tx.QueryRow(ctx, `SELECT identity_key FROM devices WHERE user_id = $1`, user).Scan(&old)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if err == nil && !bytes.Equal(old, b.IdentityKey) {
			if _, err := tx.Exec(ctx, `DELETE FROM one_time_prekeys WHERE user_id = $1`, user); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `INSERT INTO devices (user_id, registration_id, identity_key,
				signed_prekey_id, signed_prekey, signed_prekey_sig,
				kyber_last_resort_id, kyber_last_resort, kyber_last_resort_sig)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (user_id) DO UPDATE SET registration_id = $2, identity_key = $3,
				signed_prekey_id = $4, signed_prekey = $5, signed_prekey_sig = $6,
				kyber_last_resort_id = $7, kyber_last_resort = $8, kyber_last_resort_sig = $9, updated_at = now()`,
			user, b.RegistrationID, b.IdentityKey,
			*b.SignedPrekey.KeyID, b.SignedPrekey.PublicKey, b.SignedPrekey.Signature,
			*b.KyberLastResort.KeyID, b.KyberLastResort.PublicKey, b.KyberLastResort.Signature); err != nil {
			return err
		}
		if err := insertOneTime(ctx, tx, user, "ec", b.OneTimePrekeys); err != nil {
			return err
		}
		if err := insertOneTime(ctx, tx, user, "kyber", b.KyberPrekeys); err != nil {
			return err
		}
		ec, ky, err := count(ctx, tx, user)
		if err != nil {
			return err
		}
		if ec > maxStored || ky > maxStored {
			return errTooMany // rollback
		}
		return nil
	})
}

// insertOneTime acrescenta num INSERT só (unnest); key_id repetido é ignorado. EC tem signature nil → NULL.
func insertOneTime(ctx context.Context, tx pgx.Tx, user, kind string, keys []preKey) error {
	if len(keys) == 0 {
		return nil
	}
	ids := make([]int64, len(keys))
	pubs := make([][]byte, len(keys))
	sigs := make([][]byte, len(keys))
	for i, k := range keys {
		ids[i], pubs[i], sigs[i] = *k.KeyID, k.PublicKey, k.Signature
	}
	_, err := tx.Exec(ctx, `INSERT INTO one_time_prekeys (user_id, kind, key_id, public_key, signature)
		SELECT $1, $2, k, p, s FROM unnest($3::integer[], $4::bytea[], $5::bytea[]) AS t(k, p, s)
		ON CONFLICT (user_id, kind, key_id) DO NOTHING`, user, kind, ids, pubs, sigs)
	return err
}

type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func count(ctx context.Context, q querier, user string) (ec, ky int, err error) {
	err = q.QueryRow(ctx, `SELECT count(*) FILTER (WHERE kind = 'ec'), count(*) FILTER (WHERE kind = 'kyber')
		FROM one_time_prekeys WHERE user_id = $1`, user).Scan(&ec, &ky)
	return ec, ky, err
}

func (s *Store) count(ctx context.Context, user string) (ec, ky int, err error) {
	return count(ctx, s.pool, user)
}

// fetch entrega o bundle de other a me (só amigo aceito, sem bloqueio em nenhum sentido), consumindo 1 one-time EC
// e 1 Kyber. Sem Kyber one-time, vai a last resort; sem EC, OneTimePrekey fica nil.
func (s *Store) fetch(ctx context.Context, me, other string) (*fetched, error) {
	var out *fetched
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var ok bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM friendships WHERE status = 'accepted'
				AND least(requester_id, addressee_id) = least($1::uuid, $2::uuid)
				AND greatest(requester_id, addressee_id) = greatest($1::uuid, $2::uuid))
			AND NOT EXISTS (SELECT 1 FROM blocks
				WHERE (blocker_id = $1 AND blocked_id = $2) OR (blocker_id = $2 AND blocked_id = $1))`,
			me, other).Scan(&ok); err != nil {
			return err
		}
		if !ok {
			return errForbidden
		}
		f := &fetched{SignedPrekey: &keyOut{}, KyberPrekey: &keyOut{}}
		err := tx.QueryRow(ctx, `SELECT registration_id, identity_key, signed_prekey_id, signed_prekey, signed_prekey_sig,
				kyber_last_resort_id, kyber_last_resort, kyber_last_resort_sig
			FROM devices WHERE user_id = $1`, other).Scan(&f.RegistrationID, &f.IdentityKey,
			&f.SignedPrekey.KeyID, &f.SignedPrekey.PublicKey, &f.SignedPrekey.Signature,
			&f.KyberPrekey.KeyID, &f.KyberPrekey.PublicKey, &f.KyberPrekey.Signature)
		if errors.Is(err, pgx.ErrNoRows) {
			return errNotFound
		}
		if err != nil {
			return err
		}
		const take = `DELETE FROM one_time_prekeys WHERE id = (SELECT id FROM one_time_prekeys
			WHERE user_id = $1 AND kind = $2 ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED)
			RETURNING key_id, public_key, signature`
		var ky keyOut
		err = tx.QueryRow(ctx, take, other, "kyber").Scan(&ky.KeyID, &ky.PublicKey, &ky.Signature)
		switch {
		case err == nil:
			f.KyberPrekey = &ky
		case !errors.Is(err, pgx.ErrNoRows):
			return err
		}
		var ec keyOut
		err = tx.QueryRow(ctx, take, other, "ec").Scan(&ec.KeyID, &ec.PublicKey, &ec.Signature)
		switch {
		case err == nil:
			f.OneTimePrekey = &ec
		case !errors.Is(err, pgx.ErrNoRows):
			return err
		}
		out = f
		return nil
	})
	return out, err
}
