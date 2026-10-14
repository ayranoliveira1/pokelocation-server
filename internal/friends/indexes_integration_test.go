package friends

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
)

func TestIndices(t *testing.T)     { t.Parallel(); checkIndices(t, testdb.Pool(t)) }
func TestIndicesSeed(t *testing.T) { checkIndices(t, testdb.Seed(t)) }

// qInsertRequest fica de fora: INSERT … VALUES não lê tabela.
func checkIndices(t *testing.T, pool *pgxpool.Pool) {
	s := testdb.SampleOf(t, pool)
	const anyID = "00000000-0000-4000-8000-000000000000"
	pair := []any{s.User, s.Friend}
	byID := []any{anyID, s.User}
	testdb.RequireIndex(t, pool, qUserByCode, []any{s.FriendCode}, "users_friend_code_key")
	testdb.RequireIndex(t, pool, qUserByUsername, []any{"bob_1"}, "users_username_key")
	testdb.RequireIndex(t, pool, qUserExists, []any{s.Friend}, "users_pkey")
	testdb.RequireIndex(t, pool, qBlock, pair, "users_pkey")
	testdb.RequireIndex(t, pool, qBlockBetween, pair, "blocks_pkey", "blocks_blocked")
	testdb.RequireIndex(t, pool, qUnblock, pair, "blocks_pkey", "blocks_blocked")
	testdb.RequireIndex(t, pool, qPairForUpdate, pair, "friendships_pair")
	testdb.RequireIndex(t, pool, qDeletePair, pair, "friendships_pair")
	testdb.RequireIndex(t, pool, qAccept, []any{anyID}, "friendships_pkey")
	testdb.RequireIndex(t, pool, qPendingTo, byID, "friendships_pkey")
	testdb.RequireIndex(t, pool, qPendingToForUpdate, byID, "friendships_pkey")
	testdb.RequireIndex(t, pool, qDecline, byID, "friendships_pkey")
	testdb.RequireIndex(t, pool, qDeleteEnvelopesPair, pair, "envelopes_sender_id_recipient_id_client_msg_id_key")
	for _, idx := range []string{"friendships_requester", "friendships_addressee"} {
		testdb.RequireIndex(t, pool, qList, []any{s.User, listLimit}, idx)
		testdb.RequireIndex(t, pool, qPendingCounts, pair, idx)
		testdb.RequireIndex(t, pool, qFriendsCount, pair, idx)
	}
	testdb.RequireIndex(t, pool, qList, []any{s.User, listLimit}, "users_pkey")
}
