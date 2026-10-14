package relay

import (
	"math"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
)

func TestIndices(t *testing.T) {
	t.Parallel()
	checkIndices(t, testdb.Pool(t))
}

func TestIndicesSeed(t *testing.T) { checkIndices(t, testdb.Seed(t)) }

// Toda query do store em tabela grande, pela constante que o store usa (o INSERT não tem plano a conferir).
func checkIndices(t *testing.T, pool *pgxpool.Pool) {
	s := testdb.SampleOf(t, pool)
	testdb.RequireIndex(t, pool, qFriends, []any{s.User, maxFriendsRead}, "friendships_requester")
	testdb.RequireIndex(t, pool, qFriends, []any{s.User, maxFriendsRead}, "friendships_addressee")
	testdb.RequireIndex(t, pool, qAck, []any{s.Recipient, s.Sender, s.MsgID},
		"envelopes_sender_id_recipient_id_client_msg_id_key")
	testdb.RequireIndex(t, pool, qAckIDs, []any{s.Recipient, []int64{1, 2}}, "envelopes_recipient", "envelopes_pkey")
	for _, idx := range [][]string{{"users_pkey"}, {"envelopes_recipient"}, {"friendships_requester", "friendships_addressee"}} {
		testdb.RequireIndex(t, pool, qSession, []any{s.User, maxFriendsRead}, idx...)
	}
	testdb.RequireIndexNoSort(t, pool, qPage, []any{s.Recipient, int64(0), int64(math.MaxInt64), pageSize}, "envelopes_recipient")
	testdb.RequireIndexNoSort(t, pool, qCleanup, []any{cleanupBatch}, "envelopes_created_at")
}
