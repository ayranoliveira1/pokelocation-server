// Coleta os planos reais das queries do servidor no Postgres da auditoria (loadtest/seed/pg.sh):
//
//	go run ./loadtest/plans on       # zera pg_stat_statements e liga auto_explain para tudo
//	... roda o apitest e a carga contra esse Postgres ...
//	go run ./loadtest/plans report   # desliga, junta auto_explain + pg_stat_statements e imprime a tabela
//
// O auto_explain vai para o log do container (stderr); as linhas de continuação de uma mensagem vêm com um tab.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	dbURL     = flag.String("db", envOr("S12_DATABASE_URL", "postgres://s12:s12@127.0.0.1:55499/s12?sslmode=disable"), "Postgres da auditoria")
	container = flag.String("container", "pokeloc-s12-pg", "container do Postgres (para ler o log)")
	bigRows   = flag.Float64("big", 10_000, "tabela grande: a partir de quantas linhas um Seq Scan vira alerta")
	marker    = flag.String("since-file", os.TempDir()+"/pokeloc-s12-plans-since", "guarda o início da coleta entre on e report")
)

func main() {
	flag.Parse()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, *dbURL)
	if err != nil {
		fail("banco: %v", err)
	}
	defer c.Close(ctx)
	// ALTER SYSTEM com log_min_duration=0 num banco de verdade encheria o disco de log.
	var name string
	if err := c.QueryRow(ctx, `SELECT current_database()`).Scan(&name); err != nil {
		fail("banco: %v", err)
	}
	if name != "s12" {
		fail("o banco é %q, não s12 (o de loadtest/seed/pg.sh): recuso mexer na configuração", name)
	}
	switch flag.Arg(0) {
	case "on":
		run(ctx, c, `CREATE EXTENSION IF NOT EXISTS pg_stat_statements`)
		run(ctx, c, `SELECT pg_stat_statements_reset()`)
		setExplain(ctx, c, "0")
		if err := os.WriteFile(*marker, []byte(time.Now().UTC().Format(time.RFC3339)), 0o600); err != nil {
			fail("%v", err)
		}
		fmt.Println("coletando: rode o apitest e a carga agora")
	case "report":
		setExplain(ctx, c, "-1")
		since, err := os.ReadFile(*marker)
		if err != nil {
			fail("rode 'on' antes: %v", err)
		}
		report(ctx, c, string(since))
	default:
		fail("uso: plans on|report")
	}
}

func setExplain(ctx context.Context, c *pgx.Conn, v string) {
	run(ctx, c, `ALTER SYSTEM SET auto_explain.log_min_duration = `+v)
	run(ctx, c, `SELECT pg_reload_conf()`)
}

type node struct {
	Type     string   `json:"Node Type"`
	Relation string   `json:"Relation Name"`
	Index    string   `json:"Index Name"`
	Rows     float64  `json:"Actual Rows"`
	Loops    float64  `json:"Actual Loops"`
	SortKey  []string `json:"Sort Key"`
	Plans    []node   `json:"Plans"`
}

type explained struct {
	Query string `json:"Query Text"`
	Plan  node   `json:"Plan"`
}

type agg struct {
	worst    float64 // ms
	plan     node
	explains int
}

var durRe = regexp.MustCompile(`LOG:  duration: ([0-9.]+) ms  plan:`)

func report(ctx context.Context, c *pgx.Conn, since string) {
	big := map[string]float64{}
	rows, err := c.Query(ctx, `SELECT relname, reltuples FROM pg_class WHERE relkind = 'r' AND relnamespace = 'public'::regnamespace`)
	if err != nil {
		fail("pg_class: %v", err)
	}
	var rel string
	var tuples float64
	if _, err := pgx.ForEachRow(rows, []any{&rel, &tuples}, func() error { big[rel] = tuples; return nil }); err != nil {
		fail("pg_class: %v", err)
	}

	plans := readPlans(since)

	type stat struct {
		query                       string
		calls                       int64
		mean, max, rowsPer, hit, rd float64
	}
	var stats []stat
	rows, err = c.Query(ctx, `SELECT query, calls, mean_exec_time, max_exec_time, rows::float8 / calls,
		shared_blks_hit::float8 / calls, shared_blks_read::float8 / calls
		FROM pg_stat_statements
		WHERE calls > 0 AND query !~* '(pg_stat_statements|pg_catalog|pg_class|ALTER SYSTEM|pg_reload_conf|^(BEGIN|COMMIT|ROLLBACK|SET|SHOW|DISCARD))'
		ORDER BY total_exec_time DESC`)
	if err != nil {
		fail("pg_stat_statements: %v", err)
	}
	var st stat
	if _, err := pgx.ForEachRow(rows, []any{&st.query, &st.calls, &st.mean, &st.max, &st.rowsPer, &st.hit, &st.rd}, func() error {
		stats = append(stats, st)
		return nil
	}); err != nil {
		fail("pg_stat_statements: %v", err)
	}

	fmt.Printf("| # | Query | Chamadas | Média ms | Máx ms | Linhas/chamada | Buffers hit/read por chamada | Plano (pior caso) | Alerta |\n")
	fmt.Printf("|---|---|---|---|---|---|---|---|---|\n")
	matched := map[string]bool{}
	for i, s := range stats {
		q := norm(s.query)
		a, ok := plans[key(q)]
		plan, alert := "sem plano no log", ""
		if ok {
			matched[key(q)] = true
			plan, alert = summarize(a.plan, big)
		}
		if fromTool(q) {
			alert = "(ferramenta de teste) " + alert
		}
		fmt.Printf("| %d | `%s` | %d | %.3f | %.3f | %.1f | %.1f / %.1f | %s | %s |\n",
			i+1, short(q, 110), s.calls, s.mean, s.max, s.rowsPer, s.hit, s.rd, plan, alert)
	}
	// Queries internas (checagens de FK, cascatas) só aparecem no auto_explain (log_nested_statements).
	var extra []string
	for k := range plans {
		if !matched[k] {
			extra = append(extra, k)
		}
	}
	sort.Strings(extra)
	if len(extra) > 0 {
		fmt.Printf("\nSó no auto_explain (aninhadas ou sem pg_stat_statements):\n\n| Query | Execuções | Pior ms | Plano | Alerta |\n|---|---|---|---|---|\n")
		for _, k := range extra {
			a := plans[k]
			plan, alert := summarize(a.plan, big)
			fmt.Printf("| `%s` | %d | %.3f | %s | %s |\n", short(k, 110), a.explains, a.worst, plan, alert)
		}
	}
}

// readPlans: por query normalizada, o plano da execução mais lenta.
func readPlans(since string) map[string]*agg {
	out, err := exec.Command("docker", "logs", "--since", since, *container).CombinedOutput() // #nosec G204 -- nome do container vem da flag
	if err != nil {
		fail("docker logs: %v", err)
	}
	plans := map[string]*agg{}
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	var body strings.Builder
	var dur float64
	flush := func() {
		if body.Len() == 0 {
			return
		}
		var e explained
		if json.Unmarshal([]byte(body.String()), &e) == nil {
			k := key(e.Query)
			a := plans[k]
			if a == nil {
				a = &agg{}
				plans[k] = a
			}
			a.explains++
			if dur >= a.worst {
				a.worst, a.plan = dur, e.Plan
			}
		}
		body.Reset()
	}
	for sc.Scan() {
		l := sc.Text()
		if strings.HasPrefix(l, "\t") {
			if body.Len() > 0 || strings.HasPrefix(l, "\t{") {
				body.WriteString(l[1:])
			}
			continue
		}
		flush()
		if m := durRe.FindStringSubmatch(l); m != nil {
			_, _ = fmt.Sscan(m[1], &dur)
		}
	}
	flush()
	return plans
}

// summarize: nós de acesso e ordenação do plano; alerta para Seq Scan em tabela grande e para Sort.
func summarize(n node, big map[string]float64) (plan, alert string) {
	var parts, alerts []string
	var walk func(n node)
	walk = func(n node) {
		switch {
		case n.Relation != "" || n.Index != "":
			p := n.Type
			if n.Index != "" {
				p += " " + n.Index
			} else {
				p += " " + n.Relation
			}
			parts = append(parts, p)
			if n.Type == "Seq Scan" && big[n.Relation] >= *bigRows {
				alerts = append(alerts, fmt.Sprintf("⚠ Seq Scan %s (%.0f linhas)", n.Relation, big[n.Relation]))
			}
		case n.Type == "Sort" || n.Type == "Incremental Sort":
			parts = append(parts, n.Type)
			alerts = append(alerts, fmt.Sprintf("⚠ %s %s (%.0f linhas)", n.Type, strings.Join(n.SortKey, ","), n.Rows*max(n.Loops, 1)))
		}
		for _, c := range n.Plans {
			walk(c)
		}
	}
	walk(n)
	return strings.Join(parts, " → "), strings.Join(alerts, "; ")
}

var (
	spaces   = regexp.MustCompile(`\s+`)
	literals = regexp.MustCompile(`'(?:[^']|'')*'|\$\d+|\b\d+\b`)
)

func norm(q string) string { return strings.TrimSpace(spaces.ReplaceAllString(q, " ")) }

// key junta as duas fontes: o pg_stat_statements troca constantes por $n e o auto_explain guarda o texto original.
func key(q string) string { return literals.ReplaceAllString(norm(q), "?") }

// Queries das ferramentas da auditoria (seed, limpeza do loadtest, seed do apitest), não do servidor.
var toolPrefixes = []string{"copy ", "DELETE FROM users WHERE email LIKE", "WITH nu AS ( INSERT INTO users", "TRUNCATE", "VACUUM"}

func fromTool(q string) bool {
	return slices.ContainsFunc(toolPrefixes, func(p string) bool { return strings.HasPrefix(q, p) })
}

func short(s string, n int) string {
	s = strings.ReplaceAll(s, "|", "\\|")
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

func run(ctx context.Context, c *pgx.Conn, sql string) {
	if _, err := c.Exec(ctx, sql); err != nil {
		fail("%s: %v", sql, err)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func fail(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "plans: "+format+"\n", a...)
	os.Exit(1)
}
