package keys

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
)

func TestIndices(t *testing.T)     { t.Parallel(); checkIndices(t, testdb.Pool(t)) }
func TestIndicesSeed(t *testing.T) { checkIndices(t, testdb.Seed(t)) }

// Toda leitura do store. qUpsertDevice e qInsertOneTime ficam de fora: INSERT de VALUES/unnest não lê tabela, e o
// ON CONFLICT usa o índice único pela definição.
func checkIndices(t *testing.T, pool *pgxpool.Pool) {
	s := testdb.SampleOf(t, pool)
	const unique = "one_time_prekeys_user_id_kind_key_id_key"
	testdb.RequireIndexNoSort(t, pool, qTake, []any{s.KeyUser, "kyber"}, unique)
	testdb.RequireIndex(t, pool, qCount, []any{s.KeyUser}, unique)
	testdb.RequireIndex(t, pool, qDeleteOneTime, []any{s.KeyUser}, unique)
	testdb.RequireIndex(t, pool, qIdentity, []any{s.KeyUser}, "devices_pkey")
	testdb.RequireIndex(t, pool, qDevice, []any{s.KeyUser}, "devices_pkey")
	testdb.RequireIndex(t, pool, qCanFetch, []any{s.User, s.Friend}, "friendships_pair")
	testdb.RequireIndex(t, pool, qCanFetch, []any{s.User, s.Friend}, "blocks_pkey", "blocks_blocked")
}
