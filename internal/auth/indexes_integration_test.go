package auth

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
)

func TestIndices(t *testing.T) {
	t.Parallel()
	checkIndices(t, testdb.Pool(t))
}

func TestIndicesSeed(t *testing.T) { checkIndices(t, testdb.Seed(t)) }

// Toda query do store em tabela grande, pela constante que o store usa (os INSERT não têm plano a conferir).
func checkIndices(t *testing.T, pool *pgxpool.Pool) {
	s := testdb.SampleOf(t, pool)
	now := time.Now()
	for _, c := range []struct {
		sql     string
		args    []any
		indexes [][]string // cada grupo tem de aparecer (um índice do grupo basta)
	}{
		{qUserByEmail, []any{s.Email}, [][]string{{"users_email_key"}}},
		{qUserBySub, []any{s.GoogleSub}, [][]string{{"users_google_sub_key"}}},
		{qVersion, []any{s.User}, [][]string{{"users_pkey"}}},
		{qLinkUser, []any{s.User, "sub"}, [][]string{{"users_pkey"}}},
		{qLinkFriendships, []any{s.User}, [][]string{{"friendships_requester"}, {"friendships_addressee"}}},
		{qDelRefreshOfUser, []any{s.User}, [][]string{{"refresh_tokens_user"}}},
		{qDelBlocksOfUser, []any{s.User}, [][]string{{"blocks_pkey"}, {"blocks_blocked"}}},
		{qDelDeviceOfUser, []any{s.User}, [][]string{{"devices_pkey"}}},
		{qDelPrekeysOfUser, []any{s.User}, [][]string{{"one_time_prekeys_user_id_kind_key_id_key"}}},
		{qDelEnvelopesOfUser, []any{s.User}, [][]string{{"envelopes_recipient"},
			{"envelopes_sender_id_recipient_id_client_msg_id_key"}}},
		{qMe, []any{s.User}, [][]string{{"users_pkey"}}},
		{qNewRefresh, []any{s.User, s.TokenHash, now}, [][]string{{"users_pkey"}}},
		{qRefreshOwner, []any{s.TokenHash}, [][]string{{"refresh_tokens_token_hash_key"}}},
		{qLockUser, []any{s.User}, [][]string{{"users_pkey"}}},
		{qRefreshForUpdate, []any{s.TokenHash}, [][]string{{"refresh_tokens_token_hash_key"}}},
		{qBumpVersion, []any{s.User}, [][]string{{"users_pkey"}}},
		{qDelFamily, []any{s.Family}, [][]string{{"refresh_tokens_family"}}},
		{qMarkUsed, []any{s.TokenHash, now}, [][]string{{"refresh_tokens_token_hash_key"}}},
		{qRevokeLookup, []any{s.TokenHash}, [][]string{{"refresh_tokens_token_hash_key"}}},
	} {
		for _, group := range c.indexes {
			testdb.RequireIndex(t, pool, c.sql, c.args, group...)
		}
	}
	testdb.RequireIndexNoSort(t, pool, qCleanup, []any{now, cleanupBatch}, "refresh_tokens_expires_at")
}
