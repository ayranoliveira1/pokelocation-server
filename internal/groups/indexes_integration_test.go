package groups

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
)

func TestIndices(t *testing.T)     { t.Parallel(); checkIndices(t, testdb.Pool(t)) }
func TestIndicesSeed(t *testing.T) { checkIndices(t, testdb.Seed(t)) }

// Toda leitura do store. qInsertGroup, qInsertMembers e qAddMember ficam de fora: INSERT de VALUES/unnest não lê
// tabela.
func checkIndices(t *testing.T, pool *pgxpool.Pool) {
	s := testdb.SampleOf(t, pool)
	const anyGroup = "00000000-0000-4000-8000-000000000000"
	member := []any{anyGroup, s.User}
	testdb.RequireIndex(t, pool, qFriendsOK, []any{s.User, []string{s.Friend}}, "friendships_pair")
	testdb.RequireIndex(t, pool, qFriendsOK, []any{s.User, []string{s.Friend}}, "blocks_pkey", "blocks_blocked")
	testdb.RequireIndex(t, pool, qGroupsFull, []any{[]string{s.User}, maxGroups}, "group_members_user")
	testdb.RequireIndex(t, pool, qLockGroup, []any{anyGroup}, "groups_pkey")
	testdb.RequireIndex(t, pool, qDeleteGroup, []any{anyGroup}, "groups_pkey")
	testdb.RequireIndex(t, pool, qIsMember, member, "group_members_pkey")
	testdb.RequireIndex(t, pool, qRemoveMember, member, "group_members_pkey")
	testdb.RequireIndex(t, pool, qMemberCount, []any{anyGroup}, "group_members_pkey")
	testdb.RequireIndex(t, pool, qMembers, []any{anyGroup}, "group_members_pkey")
	testdb.RequireIndex(t, pool, qUserGroups, []any{s.User}, "group_members_user")
	testdb.RequireIndex(t, pool, qTransfer, []any{anyGroup}, "groups_pkey")
	testdb.RequireIndex(t, pool, qTransfer, []any{anyGroup}, "group_members_pkey")
	for _, idx := range []string{"group_members_user", "groups_pkey", "group_members_pkey"} {
		testdb.RequireIndex(t, pool, qList, []any{s.User, listLimit}, idx)
	}
	for _, idx := range []string{"groups_pkey", "group_members_pkey"} {
		testdb.RequireIndex(t, pool, qGet, member, idx)
	}
}
