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

const (
	qCircle = `SELECT
		ARRAY(SELECT (CASE WHEN requester_id = $1 THEN addressee_id ELSE requester_id END)::text
			FROM friendships WHERE status = 'accepted' AND (requester_id = $1 OR addressee_id = $1) LIMIT $2),
		ARRAY(SELECT DISTINCT gm2.user_id::text FROM group_members gm1
			JOIN group_members gm2 ON gm2.group_id = gm1.group_id
			WHERE gm1.user_id = $1 AND gm2.user_id <> $1 AND NOT EXISTS (SELECT 1 FROM blocks b
				WHERE (b.blocker_id = $1 AND b.blocked_id = gm2.user_id) OR (b.blocker_id = gm2.user_id AND b.blocked_id = $1))
			LIMIT $3)`
	qInsertEnvelope = `WITH room AS (SELECT count(*) < $5 OR EXISTS (SELECT 1 FROM envelopes
				WHERE sender_id = $2 AND recipient_id = $1 AND client_msg_id = $3) AS ok
			FROM (SELECT 1 FROM envelopes WHERE recipient_id = $1 LIMIT $5) q),
		ins AS (INSERT INTO envelopes (recipient_id, sender_id, client_msg_id, payload)
			SELECT $1, $2, $3, $4 FROM room WHERE ok
			ON CONFLICT (sender_id, recipient_id, client_msg_id) DO NOTHING RETURNING 1)
		SELECT (SELECT ok FROM room), EXISTS (SELECT 1 FROM ins)`
	qAck     = `DELETE FROM envelopes WHERE recipient_id = $1 AND sender_id = $2 AND client_msg_id = $3`
	qAckIDs  = `DELETE FROM envelopes WHERE recipient_id = $1 AND id = ANY($2)`
	qSession = `SELECT u.token_version, u.username IS NOT NULL,
			(SELECT coalesce(max(e.id), 0) FROM envelopes e WHERE e.recipient_id = u.id),
			ARRAY(SELECT (CASE WHEN f.requester_id = u.id THEN f.addressee_id ELSE f.requester_id END)::text
				FROM friendships f
				WHERE f.status = 'accepted' AND (f.requester_id = u.id OR f.addressee_id = u.id) LIMIT $2),
			ARRAY(SELECT DISTINCT gm2.user_id::text FROM group_members gm1
				JOIN group_members gm2 ON gm2.group_id = gm1.group_id
				WHERE gm1.user_id = u.id AND gm2.user_id <> u.id AND NOT EXISTS (SELECT 1 FROM blocks b
					WHERE (b.blocker_id = u.id AND b.blocked_id = gm2.user_id) OR (b.blocker_id = gm2.user_id AND b.blocked_id = u.id))
				LIMIT $3)
		FROM users u WHERE u.id = $1`
	qPage = `SELECT id, id::text AS id_text, sender_id::text, client_msg_id, group_id::text, payload FROM envelopes
		WHERE recipient_id = $1 AND id > $2 AND id <= $3 ORDER BY id LIMIT $4`
	// Membresia conferida no mesmo comando que grava: quem saiu do grupo entre a checagem e o INSERT não recebe cópia.
	qGroupSend = `WITH member AS (SELECT 1 FROM group_members WHERE group_id = $1 AND user_id = $2),
		elig AS (SELECT gm.user_id FROM group_members gm, member
			WHERE gm.group_id = $1 AND gm.user_id <> $2 AND NOT EXISTS (SELECT 1 FROM blocks b
				WHERE (b.blocker_id = $2 AND b.blocked_id = gm.user_id) OR (b.blocker_id = gm.user_id AND b.blocked_id = $2))),
		rcpt AS (SELECT user_id FROM elig
			WHERE (SELECT count(*) FROM (SELECT 1 FROM envelopes e WHERE e.recipient_id = elig.user_id LIMIT $5) q) < $5),
		ins AS (INSERT INTO envelopes (recipient_id, sender_id, client_msg_id, payload, group_id)
			SELECT user_id, $2, $3, $4, $1 FROM rcpt
			ON CONFLICT (sender_id, recipient_id, client_msg_id) DO NOTHING RETURNING recipient_id)
		SELECT EXISTS (SELECT 1 FROM member), ARRAY(SELECT recipient_id::text FROM ins), (SELECT count(*) FROM elig)`
	qCleanup = `DELETE FROM envelopes WHERE id = ANY(ARRAY(
			SELECT id FROM envelopes WHERE created_at < now() - interval '30 days' ORDER BY created_at LIMIT $1))`
)

type queuedEnvelope struct {
	ID          int64   `json:"-"`
	IDText      string  `json:"id"`
	From        string  `json:"from"`
	ClientMsgID string  `json:"client_msg_id"`
	GroupID     *string `json:"group_id,omitempty"`
	Payload     []byte  `json:"payload"`
}

// O bloqueio apaga a amizade: aceita já é sem bloqueio.
func (s *Store) circle(ctx context.Context, user string) (friends, coMembers []string, err error) {
	err = s.pool.QueryRow(ctx, qCircle, user, maxFriendsRead, maxGroupContacts).Scan(&friends, &coMembers)
	return friends, coMembers, err
}

// groupSend: eligible conta os membros que receberiam uma cópia, inclusive os de fila cheia.
func (s *Store) groupSend(ctx context.Context, group, from, msgID string, payload []byte) (member bool,
	recipients []string, eligible int, err error) {
	err = s.pool.QueryRow(ctx, qGroupSend, group, from, msgID, payload, maxPendingPerRecipient).
		Scan(&member, &recipients, &eligible)
	return member, recipients, eligible, err
}

// insert: room false = fila do destinatário cheia, nada gravado; fresh false = reenvio, ignorado.
func (s *Store) insert(ctx context.Context, to, from string, msgID string, payload []byte) (room, fresh bool, err error) {
	err = s.pool.QueryRow(ctx, qInsertEnvelope, to, from, msgID, payload, maxPendingPerRecipient).Scan(&room, &fresh)
	return room, fresh, err
}

func (s *Store) ack(ctx context.Context, recipient, from, msgID string) error {
	_, err := s.pool.Exec(ctx, qAck,
		recipient, from, msgID)
	return err
}

func (s *Store) ackIDs(ctx context.Context, recipient string, ids []int64) error {
	_, err := s.pool.Exec(ctx, qAckIDs, recipient, ids)
	return err
}

var errNoUser = errors.New("relay: usuário não existe")

func (s *Store) session(ctx context.Context, user string) (version int, named bool, cutoff int64, friends,
	coMembers []string, err error) {
	err = s.pool.QueryRow(ctx, qSession, user, maxFriendsRead, maxGroupContacts).Scan(&version, &named, &cutoff,
		&friends, &coMembers)
	var pg *pgconn.PgError
	if errors.Is(err, pgx.ErrNoRows) || errors.As(err, &pg) && pg.Code == "22P02" {
		err = errNoUser
	}
	return version, named, cutoff, friends, coMembers, err
}

func (s *Store) page(ctx context.Context, recipient string, after, upTo int64, limit int) ([]queuedEnvelope, error) {
	rows, err := s.pool.Query(ctx, qPage, recipient, after, upTo, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[queuedEnvelope])
}

// Em lotes com prazo próprio: um DELETE único passaria do statement_timeout.
func (s *Store) cleanup(ctx context.Context) error {
	for {
		bctx, cancel := context.WithTimeout(ctx, batchWait)
		// ORDER BY anda pelo índice de created_at e o ANY(ARRAY) apaga pela PK; sem eles cada lote relê a tabela.
		tag, err := s.pool.Exec(bctx, qCleanup, cleanupBatch)
		cancel()
		if err != nil || tag.RowsAffected() < cleanupBatch {
			return err
		}
	}
}
