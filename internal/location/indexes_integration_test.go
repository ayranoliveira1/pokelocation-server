package location

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
)

func TestIndices(t *testing.T)     { t.Parallel(); checkIndices(t, testdb.Pool(t)) }
func TestIndicesSeed(t *testing.T) { checkIndices(t, testdb.Seed(t)) }

func checkIndices(t *testing.T, pool *pgxpool.Pool) {
	s := testdb.SampleOf(t, pool)
	args := []any{s.User, friendsLimit}
	for _, idx := range []string{"friendships_requester", "friendships_addressee", "users_pkey"} {
		testdb.RequireIndex(t, pool, qFriends, args, idx)
	}
	testdb.RequireIndex(t, pool, qFriends, args, "blocks_pkey", "blocks_blocked")
}
