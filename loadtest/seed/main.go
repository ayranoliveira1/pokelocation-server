// Semeador da auditoria de banco: volume e distribuição parecidos com produção, para os planos do Postgres serem os
// de verdade. Só no Postgres de loadtest/seed/pg.sh: apaga tudo antes de semear.
//
//	go run ./loadtest/seed -db "$(loadtest/seed/pg.sh url)"
package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	mrand "math/rand/v2"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ayranoliveira1/pokelocation-server/internal/db"
)

var (
	dbURL       = flag.String("db", os.Getenv("S12_DATABASE_URL"), "Postgres da auditoria (loadtest/seed/pg.sh url)")
	nUsers      = flag.Int("users", 200_000, "usuários")
	nFriends    = flag.Int("friendships", 2_000_000, "linhas em friendships")
	pending     = flag.Float64("pending", 0.15, "fração de amizades pendentes")
	blockRate   = flag.Float64("blocks", 0.01, "bloqueios, como fração das amizades")
	nEnvelopes  = flag.Int("envelopes", 1_000_000, "envelopes na fila")
	maxAge      = flag.Duration("max-age", 33*24*time.Hour, "idade máxima dos envelopes (a limpeza apaga os de mais de 30 dias)")
	deviceRate  = flag.Float64("devices", 0.9, "fração de usuários com aparelho (chaves enviadas)")
	prekeyRate  = flag.Float64("prekey-users", 0.10, "fração de usuários com one-time prekeys")
	nPrekeys    = flag.Int("prekeys", 100, "one-time EC e Kyber por usuário com prekeys")
	kyberBytes  = flag.Int("kyber-bytes", 1569, "tamanho da chave Kyber; 1569 é o real (prekeys de 10% ≈ 3,4 GB)")
	refreshMean = flag.Int("refresh", 2, "refresh tokens por usuário, em média")
	scale       = flag.Float64("scale", 1, "multiplica todos os volumes (0.01 para um ensaio rápido)")
	migrateOnly = flag.Bool("migrate-only", false, "só aplica as migrações novas, sem apagar nem semear")
	wipeAny     = flag.Bool("i-know-this-wipes", false, "aceita semear (e apagar) um banco que não se chama s12")
)

// Tetos do contrato (docs/api.md §3) com folga, para o seed não criar o que a API recusaria.
const auditDB = "s12"

const (
	maxAccepted = 480
	maxPending  = 45
)

func main() {
	flag.Parse()
	if *dbURL == "" {
		fail("-db ou S12_DATABASE_URL")
	}
	sc := func(n int) int { return max(1, int(float64(n)**scale)) }
	*nUsers, *nFriends, *nEnvelopes = sc(*nUsers), sc(*nFriends), sc(*nEnvelopes)
	ctx := context.Background()

	// Conexão própria: o pool do servidor tem statement_timeout, e um COPY de milhões de linhas passa disso.
	conn, err := pgx.Connect(ctx, *dbURL)
	if err != nil {
		fail("banco: %v", err)
	}
	defer conn.Close(ctx)
	// O seed apaga tudo: um DSN errado (compose, produção) seria perda total.
	var name string
	if err := conn.QueryRow(ctx, `SELECT current_database()`).Scan(&name); err != nil {
		fail("banco: %v", err)
	}
	if name != auditDB && !*wipeAny {
		fail("o banco é %q, não %q (o de loadtest/seed/pg.sh): recuso apagar; -i-know-this-wipes para forçar", name, auditDB)
	}
	pool, err := db.Open(ctx, *dbURL)
	if err != nil {
		fail("banco: %v", err)
	}
	if err := db.Migrate(ctx, pool); err != nil {
		fail("migrate: %v", err)
	}
	pool.Close()
	if *migrateOnly {
		return
	}
	exec(ctx, conn, `TRUNCATE users, devices, one_time_prekeys, refresh_tokens, friendships, blocks, envelopes CASCADE`)

	t0 := time.Now()
	ids := seedUsers(ctx, conn)
	seedFriendships(ctx, conn, ids)
	seedEnvelopes(ctx, conn, ids)
	seedRefresh(ctx, conn, ids)
	seedKeys(ctx, conn, ids)
	step("VACUUM ANALYZE", func() int64 { exec(ctx, conn, `VACUUM ANALYZE`); return 0 })
	fmt.Printf("pronto em %v\n", time.Since(t0).Round(time.Second))
}

func seedUsers(ctx context.Context, c *pgx.Conn) []string {
	ids := make([]string, *nUsers)
	codes := map[string]bool{}
	i := 0
	step("users", func() int64 {
		return copyRows(ctx, c, "users", []string{"id", "email", "password_hash", "google_sub", "friend_code", "created_at"},
			func() ([]any, bool) {
				if i == len(ids) {
					return nil, false
				}
				ids[i] = uuid()
				code := friendCode()
				for codes[code] {
					code = friendCode()
				}
				codes[code] = true
				email, hash, sub := any(fmt.Sprintf("u%d@seed.test", i)), any("$argon2id$v=19$m=19456,t=2,p=1$seed$seed"), any(nil)
				if rn(5) == 0 { // ~20% entram pelo Google
					hash, sub = nil, "g"+strconv.Itoa(i)
				}
				row := []any{ids[i], email, hash, sub, code, ago(365 * 24 * time.Hour)}
				i++
				return row, true
			})
	})
	return ids
}

// seedFriendships: metade dos pares é uniforme e metade escolhe um dos lados por Zipf (há quem tenha centenas de
// amigos), respeitando os tetos por usuário e o par único em qualquer direção.
func seedFriendships(ctx context.Context, c *pgx.Conn, ids []string) {
	n := len(ids)
	popular := zipf(1, 1.07, 2, n)
	order := perm(n) // os populares não são os primeiros ids
	pairs := make(map[uint64]bool, *nFriends)
	accepted, pendOut, pendIn := make([]int16, n), make([]int8, n), make([]int8, n)
	key := func(a, b int) uint64 { return u64(min(a, b))*u64(n) + u64(max(a, b)) }

	made := 0
	step("friendships", func() int64 {
		return copyRows(ctx, c, "friendships", []string{"requester_id", "addressee_id", "status", "created_at"},
			func() ([]any, bool) {
				for made < *nFriends {
					a, b := rn(n), rn(n)
					if rn(2) == 0 {
						b = order[popular.Uint64()]
					}
					if a == b || pairs[key(a, b)] {
						continue
					}
					status := "accepted"
					if rf() < *pending {
						if pendOut[a] >= maxPending || pendIn[b] >= maxPending {
							continue
						}
						status = "pending"
						pendOut[a]++
						pendIn[b]++
					} else {
						if accepted[a] >= maxAccepted || accepted[b] >= maxAccepted {
							continue
						}
						accepted[a]++
						accepted[b]++
					}
					pairs[key(a, b)] = true
					made++
					return []any{ids[a], ids[b], status, ago(180 * 24 * time.Hour)}, true
				}
				return nil, false
			})
	})

	// O bloqueio apaga a amizade (friends/store.go), então só entre quem não tem par.
	nBlocks := int(float64(*nFriends) * *blockRate)
	blocked := map[uint64]bool{}
	step("blocks", func() int64 {
		return copyRows(ctx, c, "blocks", []string{"blocker_id", "blocked_id"}, func() ([]any, bool) {
			for len(blocked) < nBlocks {
				a, b := rn(n), rn(n)
				if a == b || pairs[key(a, b)] || blocked[u64(a)*u64(n)+u64(b)] {
					continue
				}
				blocked[u64(a)*u64(n)+u64(b)] = true
				return []any{ids[a], ids[b]}, true
			}
			return nil, false
		})
	})
}

// seedEnvelopes: a maioria das filas é curta, mas 20% dos envelopes vão para destinatários por Zipf (quem ficou
// semanas sem abrir o app acumula milhares).
func seedEnvelopes(ctx context.Context, c *pgx.Conn, ids []string) {
	n := len(ids)
	heavy := zipf(3, 1.2, 4, n)
	order := perm(n)
	i := 0
	step("envelopes", func() int64 {
		return copyRows(ctx, c, "envelopes", []string{"recipient_id", "sender_id", "client_msg_id", "payload", "created_at"},
			func() ([]any, bool) {
				if i == *nEnvelopes {
					return nil, false
				}
				to := rn(n)
				if rn(5) == 0 {
					to = order[heavy.Uint64()]
				}
				from := (to + 1 + rn(n-1)) % n
				i++
				return []any{ids[to], ids[from], strconv.Itoa(i), randBytes(64 + rn(449)), ago(*maxAge)}, true
			})
	})
}

// seedRefresh mistura famílias ativas, já rotacionadas (used_at) e vencidas.
func seedRefresh(ctx context.Context, c *pgx.Conn, ids []string) {
	u, left := 0, 0
	var family string
	step("refresh_tokens", func() int64 {
		return copyRows(ctx, c, "refresh_tokens", []string{"user_id", "family_id", "token_hash", "expires_at", "used_at", "created_at"},
			func() ([]any, bool) {
				for left == 0 {
					if u == len(ids) {
						return nil, false
					}
					u++
					left, family = rn(2**refreshMean+1), uuid()
				}
				left--
				exp, used := time.Now().Add(time.Duration(1+rn(30))*24*time.Hour), any(nil)
				switch rn(3) {
				case 0:
					used = ago(30 * 24 * time.Hour)
				case 1:
					exp = ago(60 * 24 * time.Hour)
				}
				return []any{ids[u-1], family, randBytes(32), exp, used, ago(60 * 24 * time.Hour)}, true
			})
	})
}

func seedKeys(ctx context.Context, c *pgx.Conn, ids []string) {
	i := 0
	step("devices", func() int64 {
		return copyRows(ctx, c, "devices", []string{"user_id", "registration_id", "identity_key", "signed_prekey_id",
			"signed_prekey", "signed_prekey_sig", "kyber_last_resort_id", "kyber_last_resort", "kyber_last_resort_sig", "fcm_token"},
			func() ([]any, bool) {
				for ; i < len(ids); i++ {
					if rf() >= *deviceRate {
						continue
					}
					i++
					return []any{ids[i-1], 1 + rn(16380), pub(33, 5), 1, pub(33, 5), randBytes(64),
						1, pub(*kyberBytes, 8), randBytes(64), "fcm-" + uuid()}, true
				}
				return nil, false
			})
	})

	// Prekeys só existem para quem enviou o pacote de chaves, isto é, quem tem aparelho.
	var withKeys []string
	rows, err := c.Query(ctx, `SELECT user_id::text FROM devices TABLESAMPLE BERNOULLI ($1)`, *prekeyRate/(*deviceRate)*100)
	if err != nil {
		fail("devices: %v", err)
	}
	if withKeys, err = pgx.CollectRows(rows, pgx.RowTo[string]); err != nil {
		fail("devices: %v", err)
	}
	u, k := 0, 0
	step("one_time_prekeys", func() int64 {
		return copyRows(ctx, c, "one_time_prekeys", []string{"user_id", "kind", "key_id", "public_key", "signature"},
			func() ([]any, bool) {
				if u == len(withKeys) {
					return nil, false
				}
				row := []any{withKeys[u], "ec", k + 1, pub(33, 5), nil}
				if k >= *nPrekeys {
					row = []any{withKeys[u], "kyber", k + 1, pub(*kyberBytes, 8), randBytes(64)}
				}
				if k++; k == 2**nPrekeys {
					u, k = u+1, 0
				}
				return row, true
			})
	})
}

func copyRows(ctx context.Context, c *pgx.Conn, table string, cols []string, next func() ([]any, bool)) int64 {
	n, err := c.CopyFrom(ctx, pgx.Identifier{table}, cols, pgx.CopyFromFunc(func() ([]any, error) {
		row, ok := next()
		if !ok {
			return nil, nil
		}
		return row, nil
	}))
	if err != nil {
		fail("COPY %s: %v", table, err)
	}
	return n
}

func step(name string, f func() int64) {
	t := time.Now()
	n := f()
	fmt.Printf("%-18s %10d linhas  %v\n", name, n, time.Since(t).Round(time.Millisecond))
}

func exec(ctx context.Context, c *pgx.Conn, sql string) {
	if _, err := c.Exec(ctx, sql); err != nil {
		fail("%s: %v", sql, err)
	}
}

// Aleatoriedade só para volume de teste, nunca segredo: um #nosec por helper em vez de um por chamada.
func rn(n int) int     { return mrand.IntN(n) }   // #nosec G404 -- dados de teste
func rf() float64      { return mrand.Float64() } // #nosec G404 -- dados de teste
func perm(n int) []int { return mrand.Perm(n) }   // #nosec G404 -- dados de teste
func u64(i int) uint64 { return uint64(i) }       // #nosec G115 -- índices e tamanhos, nunca negativos
func ago(d time.Duration) time.Time {
	return time.Now().Add(-time.Duration(mrand.Int64N(int64(d)))) // #nosec G404 -- dados de teste
}

func zipf(seed uint64, s, v float64, n int) *mrand.Zipf {
	return mrand.NewZipf(mrand.New(mrand.NewPCG(seed, seed+1)), s, v, u64(n-1)) // #nosec G404 -- dados de teste
}

func uuid() string {
	b := randBytes(16)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func friendCode() string {
	const abc = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	b := randBytes(8)
	for i := range b {
		b[i] = abc[b[i]&31]
	}
	return string(b)
}

func pub(n int, typ byte) []byte {
	b := randBytes(n)
	b[0] = typ
	return b
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

func fail(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "seed: "+format+"\n", a...)
	os.Exit(1)
}
