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
	qInsertEnvelope = `INSERT INTO envelopes (recipient_id, sender_id, client_msg_id, payload)
		VALUES ($1, $2, $3, $4) ON CONFLICT (sender_id, recipient_id, client_msg_id) DO NOTHING`
	qAck     = `DELETE FROM envelopes WHERE recipient_id = $1 AND sender_id = $2 AND client_msg_id = $3`
	qAckIDs  = `DELETE FROM envelopes WHERE recipient_id = $1 AND id = ANY($2)`
	qSession = `SELECT u.token_version,
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
		rcpt AS (SELECT gm.user_id FROM group_members gm, member
			WHERE gm.group_id = $1 AND gm.user_id <> $2 AND NOT EXISTS (SELECT 1 FROM blocks b
				WHERE (b.blocker_id = $2 AND b.blocked_id = gm.user_id) OR (b.blocker_id = gm.user_id AND b.blocked_id = $2))),
		ins AS (INSERT INTO envelopes (recipient_id, sender_id, client_msg_id, payload, group_id)
			SELECT user_id, $2, $3, $4, $1 FROM rcpt
			ON CONFLICT (sender_id, recipient_id, client_msg_id) DO NOTHING RETURNING recipient_id)
		SELECT EXISTS (SELECT 1 FROM member), ARRAY(SELECT recipient_id::text FROM ins)`
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

// circle: amigos (o bloqueio apaga a amizade, então aceita já é sem bloqueio) e membros de grupos em comum sem
// bloqueio (contatos para o Envelope 1:1).
func (s *Store) circle(ctx context.Context, user string) (friends, coMembers []string, err error) {
	err = s.pool.QueryRow(ctx, qCircle, user, maxFriendsRead, maxGroupContacts).Scan(&friends, &coMembers)
	return friends, coMembers, err
}

// groupSend: member false = não é membro (nada gravado); recipients = só as cópias novas (reenvio não reentrega).
func (s *Store) groupSend(ctx context.Context, group, from, msgID string, payload []byte) (member bool,
	recipients []string, err error) {
	err = s.pool.QueryRow(ctx, qGroupSend, group, from, msgID, payload).Scan(&member, &recipients)
	return member, recipients, err
}

// insert: false = reenvio, ignorado.
func (s *Store) insert(ctx context.Context, to, from string, msgID string, payload []byte) (bool, error) {
	tag, err := s.pool.Exec(ctx, qInsertEnvelope, to, from, msgID, payload)
	return tag.RowsAffected() == 1, err
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

// session lê, num snapshot só, a versão dos tokens, o corte do backlog e os amigos (o Auth do WS).
func (s *Store) session(ctx context.Context, user string) (version int, cutoff int64, friends, coMembers []string,
	err error) {
	err = s.pool.QueryRow(ctx, qSession, user, maxFriendsRead, maxGroupContacts).Scan(&version, &cutoff, &friends,
		&coMembers)
	var pg *pgconn.PgError
	if errors.Is(err, pgx.ErrNoRows) || errors.As(err, &pg) && pg.Code == "22P02" {
		err = errNoUser
	}
	return version, cutoff, friends, coMembers, err
}

func (s *Store) page(ctx context.Context, recipient string, after, upTo int64, limit int) ([]queuedEnvelope, error) {
	rows, err := s.pool.Query(ctx, qPage, recipient, after, upTo, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[queuedEnvelope])
}

// cleanup em lotes curtos, cada um com prazo próprio: um DELETE único passaria do statement_timeout com a fila grande.
func (s *Store) cleanup(ctx context.Context) error {
	for {
		bctx, cancel := context.WithTimeout(ctx, batchWait)
		// ORDER BY faz o lote andar pelo índice de created_at, e o ANY(ARRAY) apaga pela PK: sem isso, cada lote relia a
		// tabela desde o começo.
		tag, err := s.pool.Exec(bctx, qCleanup, cleanupBatch)
		cancel()
		if err != nil || tag.RowsAffected() < cleanupBatch {
			return err
		}
	}
}
