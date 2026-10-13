package relay

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store: tabela envelopes e a leitura das amizades aceitas. Concreto, sem interface: testes com Postgres real.
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

// friends: amigos aceitos de user (bloqueio apaga a amizade, api.md §3: aceito já é sem bloqueio).
func (s *Store) friends(ctx context.Context, user string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT (CASE WHEN requester_id = $1 THEN addressee_id ELSE requester_id END)::text
		FROM friendships WHERE status = 'accepted' AND (requester_id = $1 OR addressee_id = $1)`, user)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// insert grava o envelope; false = reenvio (mesmo remetente, destinatário e client_msg_id), ignorado.
func (s *Store) insert(ctx context.Context, to, from string, msgID string, payload []byte) (bool, error) {
	tag, err := s.pool.Exec(ctx, `INSERT INTO envelopes (recipient_id, sender_id, client_msg_id, payload)
		VALUES ($1, $2, $3, $4) ON CONFLICT (sender_id, recipient_id, client_msg_id) DO NOTHING`, to, from, msgID, payload)
	return tag.RowsAffected() == 1, err
}

// ack apaga o envelope do destinatário (Ack do WS). Inexistente: nada.
func (s *Store) ack(ctx context.Context, recipient, from, msgID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM envelopes WHERE recipient_id = $1 AND sender_id = $2 AND client_msg_id = $3`,
		recipient, from, msgID)
	return err
}

// ackIDs apaga, entre ids, só os envelopes de recipient (REST).
func (s *Store) ackIDs(ctx context.Context, recipient string, ids []int64) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM envelopes WHERE recipient_id = $1 AND id = ANY($2)`, recipient, ids)
	return err
}

// maxID: maior id na fila de recipient agora (0 = vazia). Corte do backlog.
func (s *Store) maxID(ctx context.Context, recipient string) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `SELECT coalesce(max(id), 0) FROM envelopes WHERE recipient_id = $1`, recipient).Scan(&id)
	return id, err
}

// page: até limit envelopes de recipient com after < id ≤ upTo, mais antigos primeiro.
func (s *Store) page(ctx context.Context, recipient string, after, upTo int64, limit int) ([]queuedEnvelope, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, id::text AS id_text, sender_id::text, client_msg_id, payload FROM envelopes
		WHERE recipient_id = $1 AND id > $2 AND id <= $3 ORDER BY id LIMIT $4`, recipient, after, upTo, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[queuedEnvelope])
}

// cleanup: a fila expira em 30 dias.
// ponytail: sem índice em created_at, é um seq scan diário (a fila é pequena: sai no Ack). Índice ou corte por id se pesar.
func (s *Store) cleanup(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM envelopes WHERE created_at < now() - interval '30 days'`)
	return err
}
