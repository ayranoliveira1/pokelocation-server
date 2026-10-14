package relay

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

type queuedEnvelope struct {
	ID          int64  `json:"-"`
	IDText      string `json:"id"`
	From        string `json:"from"`
	ClientMsgID string `json:"client_msg_id"`
	Payload     []byte `json:"payload"`
}

// friends: o bloqueio apaga a amizade, então aceita já é sem bloqueio.
func (s *Store) friends(ctx context.Context, user string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT (CASE WHEN requester_id = $1 THEN addressee_id ELSE requester_id END)::text
		FROM friendships WHERE status = 'accepted' AND (requester_id = $1 OR addressee_id = $1) LIMIT $2`, user, maxFriendsRead)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// insert: false = reenvio, ignorado.
func (s *Store) insert(ctx context.Context, to, from string, msgID string, payload []byte) (bool, error) {
	tag, err := s.pool.Exec(ctx, `INSERT INTO envelopes (recipient_id, sender_id, client_msg_id, payload)
		VALUES ($1, $2, $3, $4) ON CONFLICT (sender_id, recipient_id, client_msg_id) DO NOTHING`, to, from, msgID, payload)
	return tag.RowsAffected() == 1, err
}

func (s *Store) ack(ctx context.Context, recipient, from, msgID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM envelopes WHERE recipient_id = $1 AND sender_id = $2 AND client_msg_id = $3`,
		recipient, from, msgID)
	return err
}

func (s *Store) ackIDs(ctx context.Context, recipient string, ids []int64) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM envelopes WHERE recipient_id = $1 AND id = ANY($2)`, recipient, ids)
	return err
}

var errNoUser = errors.New("relay: usuário não existe")

// session lê, num snapshot só, a versão dos tokens, o corte do backlog e os amigos (o Auth do WS).
func (s *Store) session(ctx context.Context, user string) (version int, cutoff int64, friends []string, err error) {
	err = s.pool.QueryRow(ctx, `SELECT u.token_version,
			(SELECT coalesce(max(e.id), 0) FROM envelopes e WHERE e.recipient_id = u.id),
			ARRAY(SELECT (CASE WHEN f.requester_id = u.id THEN f.addressee_id ELSE f.requester_id END)::text
				FROM friendships f
				WHERE f.status = 'accepted' AND (f.requester_id = u.id OR f.addressee_id = u.id) LIMIT $2)
		FROM users u WHERE u.id = $1`, user, maxFriendsRead).Scan(&version, &cutoff, &friends)
	var pg *pgconn.PgError
	if errors.Is(err, pgx.ErrNoRows) || errors.As(err, &pg) && pg.Code == "22P02" {
		err = errNoUser
	}
	return version, cutoff, friends, err
}

func (s *Store) page(ctx context.Context, recipient string, after, upTo int64, limit int) ([]queuedEnvelope, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, id::text AS id_text, sender_id::text, client_msg_id, payload FROM envelopes
		WHERE recipient_id = $1 AND id > $2 AND id <= $3 ORDER BY id LIMIT $4`, recipient, after, upTo, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[queuedEnvelope])
}

// cleanup em lotes curtos, cada um com prazo próprio: um DELETE único passaria do statement_timeout com a fila grande.
func (s *Store) cleanup(ctx context.Context) error {
	for {
		bctx, cancel := context.WithTimeout(ctx, batchWait)
		tag, err := s.pool.Exec(bctx, `DELETE FROM envelopes WHERE id IN
			(SELECT id FROM envelopes WHERE created_at < now() - interval '30 days' LIMIT $1)`, cleanupBatch)
		cancel()
		if err != nil || tag.RowsAffected() < cleanupBatch {
			return err
		}
	}
}
