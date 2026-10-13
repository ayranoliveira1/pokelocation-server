package friends

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store: tabelas users (só leitura), friendships e blocks. Operações num par rodam sob um advisory lock do par, o que
// serializa pedido cruzado, aceite e bloqueio simultâneos entre as mesmas duas pessoas.
type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool} }

const lockPair = `SELECT pg_advisory_xact_lock(hashtextextended(least($1::uuid, $2::uuid)::text || greatest($1::uuid, $2::uuid)::text, 0))`

const pairIs = `least(requester_id, addressee_id) = least($1::uuid, $2::uuid) AND greatest(requester_id, addressee_id) = greatest($1::uuid, $2::uuid)`

func (s *Store) userByCode(ctx context.Context, code string) (string, error) {
	var id string
	err := s.pool.QueryRow(ctx, `SELECT id FROM users WHERE friend_code = $1`, code).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrCodeNotFound
	}
	return id, err
}

// request: pedido de me para to (já existentes e distintos). Devolve o id do pedido/amizade, o status e se criou.
func (s *Store) request(ctx context.Context, me, to string) (id, status string, created bool, err error) {
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, lockPair, me, to); err != nil {
			return err
		}
		var mine bool // true: me bloqueou to (prioridade: 403 blocked); false: só to bloqueou me
		err := tx.QueryRow(ctx, `SELECT blocker_id = $1 FROM blocks
			WHERE (blocker_id = $1 AND blocked_id = $2) OR (blocker_id = $2 AND blocked_id = $1)
			ORDER BY blocker_id = $1 DESC LIMIT 1`, me, to).Scan(&mine)
		switch {
		case err == nil && mine:
			return ErrBlocked
		case err == nil:
			return ErrCodeNotFound // nunca revela o bloqueio a quem foi bloqueado
		case !errors.Is(err, pgx.ErrNoRows):
			return err
		}

		var fromMe bool
		err = tx.QueryRow(ctx, `SELECT id, status, requester_id = $1 FROM friendships WHERE `+pairIs+` FOR UPDATE`,
			me, to).Scan(&id, &status, &fromMe)
		switch {
		case err == nil && status == "accepted":
			return ErrAlreadyFriends
		case err == nil && fromMe:
			return nil // idempotente
		case err == nil: // pedido dela para mim: aceita
			status = "accepted"
			return acceptLocked(ctx, tx, id, me, to)
		case !errors.Is(err, pgx.ErrNoRows):
			return err
		}

		var out, in int
		if err := tx.QueryRow(ctx, `SELECT
			count(*) FILTER (WHERE requester_id = $1),
			count(*) FILTER (WHERE addressee_id = $2)
			FROM friendships WHERE status = 'pending' AND (requester_id = $1 OR addressee_id = $2)`,
			me, to).Scan(&out, &in); err != nil {
			return err
		}
		if out >= maxPending || in >= maxPending {
			return ErrLimit
		}
		if err := friendsRoom(ctx, tx, me, to); err != nil { // pedido que nunca poderia ser aceito
			return err
		}
		status, created = "pending", true
		return tx.QueryRow(ctx, `INSERT INTO friendships (requester_id, addressee_id, status)
			VALUES ($1, $2, 'pending') RETURNING id`, me, to).Scan(&id)
	})
	return id, status, created, err
}

// acceptLocked vira o pedido id (entre a e b, já travado) em amizade, se nenhum dos dois estiver no teto.
func acceptLocked(ctx context.Context, tx pgx.Tx, id, a, b string) error {
	if err := friendsRoom(ctx, tx, a, b); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE friendships SET status = 'accepted' WHERE id = $1`, id)
	return err
}

// friendsRoom: ErrLimit se a ou b já tem maxFriends amigos.
// ponytail: conta sem travar os outros pares de a e b; dois aceites simultâneos podem passar de 500 por 1.
// Trava por usuário se o teto virar regra dura.
func friendsRoom(ctx context.Context, tx pgx.Tx, a, b string) error {
	var na, nb int
	if err := tx.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE requester_id = $1 OR addressee_id = $1),
		count(*) FILTER (WHERE requester_id = $2 OR addressee_id = $2)
		FROM friendships WHERE status = 'accepted' AND (requester_id IN ($1, $2) OR addressee_id IN ($1, $2))`,
		a, b).Scan(&na, &nb); err != nil {
		return err
	}
	if na >= maxFriends || nb >= maxFriends {
		return ErrLimit
	}
	return nil
}

// accept: só o destinatário de um pedido pendente.
func (s *Store) accept(ctx context.Context, me, id string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var from string
		err := tx.QueryRow(ctx, `SELECT requester_id FROM friendships
			WHERE id = $1 AND addressee_id = $2 AND status = 'pending'`, id, me).Scan(&from)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, lockPair, me, from); err != nil {
			return err
		}
		// relê sob o lock: um bloqueio ou cancelamento pode ter apagado o pedido no meio
		err = tx.QueryRow(ctx, `SELECT requester_id FROM friendships
			WHERE id = $1 AND addressee_id = $2 AND status = 'pending' FOR UPDATE`, id, me).Scan(&from)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		return acceptLocked(ctx, tx, id, me, from)
	})
}

// decline: destinatário recusa ou quem pediu cancela.
func (s *Store) decline(ctx context.Context, me, id string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM friendships
		WHERE id = $1 AND status = 'pending' AND (requester_id = $2 OR addressee_id = $2)`, id, me)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

// remove desfaz amizade ou pedido entre os dois, em qualquer direção.
func (s *Store) remove(ctx context.Context, me, other string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM friendships WHERE `+pairIs, me, other)
	return err
}

// block: bloqueia e apaga amizade/pedido entre os dois.
func (s *Store) block(ctx context.Context, me, other string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, lockPair, me, other); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `INSERT INTO blocks (blocker_id, blocked_id)
			SELECT $1, id FROM users WHERE id = $2 ON CONFLICT DO NOTHING`, me, other)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			var exists bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM users WHERE id = $1)`, other).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				return ErrNotFound
			}
		}
		_, err = tx.Exec(ctx, `DELETE FROM friendships WHERE `+pairIs, me, other)
		return err
	})
}

func (s *Store) unblock(ctx context.Context, me, other string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM blocks WHERE blocker_id = $1 AND blocked_id = $2`, me, other)
	return err
}

// list: amigos e pedidos pendentes nos dois sentidos, mais antigos primeiro. Os tetos limitam o tamanho.
func (s *Store) list(ctx context.Context, me string) (List, error) {
	l := List{Friends: []Friend{}, Incoming: []Request{}, Outgoing: []Request{}}
	rows, err := s.pool.Query(ctx, `SELECT f.id, f.status, f.requester_id = $1, u.id, u.friend_code, f.created_at
		FROM friendships f
		JOIN users u ON u.id = CASE WHEN f.requester_id = $1 THEN f.addressee_id ELSE f.requester_id END
		WHERE f.requester_id = $1 OR f.addressee_id = $1
		ORDER BY f.created_at, f.id`, me)
	if err != nil {
		return l, err
	}
	var (
		id, status, other, code string
		outgoing                bool
		at                      time.Time
	)
	_, err = pgx.ForEachRow(rows, []any{&id, &status, &outgoing, &other, &code, &at}, func() error {
		switch {
		case status == "accepted":
			l.Friends = append(l.Friends, Friend{other, code})
		case outgoing:
			l.Outgoing = append(l.Outgoing, Request{id, other, code, at.UnixMilli()})
		default:
			l.Incoming = append(l.Incoming, Request{id, other, code, at.UnixMilli()})
		}
		return nil
	})
	return l, err
}
