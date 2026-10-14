package push

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
)

func TestIndices(t *testing.T) {
	t.Parallel()
	checkIndices(t, testdb.Pool(t))
}

func TestIndicesSeed(t *testing.T) { checkIndices(t, testdb.Seed(t)) }

func checkIndices(t *testing.T, pool *pgxpool.Pool) {
	s := testdb.SampleOf(t, pool)
	testdb.RequireIndex(t, pool, qClearFCMElsewhere, []any{"tok", s.KeyUser}, "devices_fcm_token")
	testdb.RequireIndex(t, pool, qSetFCM, []any{s.KeyUser, "tok"}, "devices_pkey")
	testdb.RequireIndex(t, pool, qGetFCMs, []any{[]string{s.KeyUser, s.User}}, "devices_pkey")
	testdb.RequireIndex(t, pool, qClearFCM, []any{s.KeyUser, "tok"}, "devices_pkey", "devices_fcm_token") // os dois são únicos
}
