package pogo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ayranoliveira1/pokelocation-server/internal/push"
	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
)

// failFCM recusa só a 1ª mensagem de cada lote.
type failFCM struct{ fakeFCM }

func (f *failFCM) SendData(ctx context.Context, user, token string, msgs []push.Data) error {
	_ = f.fakeFCM.SendData(ctx, user, token, msgs)
	return &push.FailedMsgs{Idx: []int{0}, Err: errors.New("fcm recusou")}
}

type page struct {
	Items []Notification
	Next  *string
}

func listPage(t *testing.T, h *Handler, user, query string) page {
	t.Helper()
	w := serve(h, user, "GET", "/v1/pogo/notifications"+query, "")
	var p page
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &p) != nil || p.Items == nil {
		t.Fatalf("lista%s: %d %s", query, w.Code, w.Body.String())
	}
	return p
}

// alertSetup: um usuário com regra no mundo e o /search devolvendo n Pokémon novos a cada chamada.
func alertSetup(t *testing.T, pool *pgxpool.Pool, sender push.Sender) (*Handler, *worker, string, func(n int)) {
	t.Helper()
	var mu sync.Mutex
	var results string
	up := http.NewServeMux()
	up.HandleFunc("POST /search", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		_, _ = io.WriteString(w, `{"results":[`+results+`]}`)
	})
	h := newHandler(t, pool, fakeUpstream(t, up))
	ps := push.New(pool, sender)
	ps.DataFailed = h.Unsent
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go ps.Run(ctx)
	u := newUser(t, pool)
	tok := "f-" + u
	withDevice(t, pool, u, &tok)
	link(t, h, u, "t")
	addRule(t, h, u, `{"name":"a","filters":{},"scope":"world"}`)
	clk := &clock{t: time.Now().Add(5 * time.Second)}
	w := newWorker(h.svc, h.q, ps)
	w.now = clk.now
	seq := 0
	round := func(n int) {
		var rs []string
		for range n {
			seq++
			rs = append(rs, fmt.Sprintf(`{"encounter_id":"e%d","reveal_code":"r%d","pokemon_name":"P%d","despawn_at_ms":%d}`,
				seq, seq, seq, clk.now().Add(10*time.Minute).UnixMilli()))
		}
		mu.Lock()
		results = strings.Join(rs, ",")
		mu.Unlock()
		drain(t, w)
		clk.add(defaultInterval + time.Second)
	}
	return h, w, u, round
}

func countNotifs(t *testing.T, pool *pgxpool.Pool, user string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM pogo_notifications WHERE user_id = $1`,
		user).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestNotifications(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	fcm := &fakeFCM{}
	h, _, u, round := alertSetup(t, pool, fcm)
	round(3)
	sent := fcm.take(t, 1)[0].msgs
	round(2)
	sent = append(sent, fcm.take(t, 1)[0].msgs...)

	p := listPage(t, h, u, "?limit=2")
	var got []Notification
	for p.Next != nil {
		got = append(got, p.Items...)
		if len(p.Items) != 2 {
			t.Fatalf("página: %+v", p)
		}
		p = listPage(t, h, u, "?limit=2&before="+*p.Next)
	}
	got = append(got, p.Items...)
	if len(got) != 5 {
		t.Fatalf("histórico: %+v", got)
	}
	for i, n := range got { // mais novo primeiro: o inverso do envio
		m := sent[len(sent)-1-i]
		var data map[string]string
		_ = json.Unmarshal(n.Data, &data)
		if m.Fields["h"] != n.ID || data["e"] != m.Fields["e"] || data["n"] != m.Fields["n"] || data["h"] != "" ||
			time.Since(time.UnixMilli(n.CreatedAt)) > time.Minute {
			t.Fatalf("item %d: %+v, push %v", i, n, m.Fields)
		}
	}
	if p := listPage(t, h, u, ""); len(p.Items) != 5 || p.Next != nil {
		t.Fatalf("padrão: %+v", p)
	}
	for _, q := range []string{"?limit=0", "?limit=51", "?limit=x", "?before=0", "?before=x"} {
		wantError(t, serve(h, u, "GET", "/v1/pogo/notifications"+q, ""), 400, "bad_request")
	}

	other := newUser(t, pool)
	if p := listPage(t, h, other, ""); len(p.Items) != 0 || p.Next != nil {
		t.Fatalf("outro usuário: %+v", p)
	}
	for _, id := range []string{got[0].ID, "x", "99999999999999999999"} {
		if w := serve(h, other, "DELETE", "/v1/pogo/notifications/"+id, ""); w.Code != 204 {
			t.Fatalf("apagar %s de outro: %d", id, w.Code)
		}
	}
	if serve(h, other, "DELETE", "/v1/pogo/notifications", "").Code != 204 || countNotifs(t, pool, u) != 5 {
		t.Fatal("outro usuário apagou o histórico")
	}
	if serve(h, u, "DELETE", "/v1/pogo/notifications/"+got[0].ID, "").Code != 204 {
		t.Fatal("apagar um")
	}
	if p := listPage(t, h, u, ""); len(p.Items) != 4 || p.Items[0].ID != got[1].ID {
		t.Fatalf("depois de apagar um: %+v", p)
	}
	if serve(h, u, "DELETE", "/v1/pogo/notifications", "").Code != 204 || countNotifs(t, pool, u) != 0 {
		t.Fatal("apagar tudo")
	}

	round(1)
	fcm.take(t, 1)
	if _, err := pool.Exec(context.Background(), `DELETE FROM users WHERE id = $1`, u); err != nil {
		t.Fatal(err)
	}
	if countNotifs(t, pool, u) != 0 {
		t.Fatal("excluir a conta não apagou o histórico")
	}
}

func TestNotificationsUnsent(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	fcm := &failFCM{}
	h, _, u, round := alertSetup(t, pool, fcm)
	round(2)
	m := fcm.take(t, 1)[0].msgs
	for deadline := time.Now().Add(5 * time.Second); countNotifs(t, pool, u) != 1; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("aviso recusado pelo FCM ficou no histórico")
		}
	}
	if p := listPage(t, h, u, ""); len(p.Items) != 1 || p.Items[0].ID != m[1].Fields["h"] {
		t.Fatalf("ficou %+v, queria só o aceito %v", p.Items, m[1].Fields)
	}
}

func TestNotificationsRetention(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	h := newHandler(t, pool, "http://127.0.0.1:1")
	ctx := context.Background()
	u := newUser(t, pool)
	for i := range maxNotifs + 3 {
		ms := []push.Data{{Fields: map[string]string{"e": fmt.Sprint(i)}}}
		if err := h.svc.store.record(ctx, u, ms); err != nil {
			t.Fatal(err)
		}
	}
	p := listPage(t, h, u, "?limit=1")
	var oldest string
	if err := pool.QueryRow(ctx, `SELECT data->>'e' FROM pogo_notifications WHERE user_id = $1 ORDER BY id LIMIT 1`,
		u).Scan(&oldest); err != nil {
		t.Fatal(err)
	}
	if n := countNotifs(t, pool, u); n != maxNotifs || oldest != "3" || !strings.Contains(string(p.Items[0].Data), `"202"`) {
		t.Fatalf("teto: %d linhas, mais antigo %s, mais novo %s", n, oldest, p.Items[0].Data)
	}
	if _, err := pool.Exec(ctx, `UPDATE pogo_notifications SET created_at = now() - interval '8 days'
		WHERE user_id = $1 AND data->>'e' = '3'`, u); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, qExpiredNotifs, time.Now().Add(-notifKeep)); err != nil {
		t.Fatal(err)
	}
	if n := countNotifs(t, pool, u); n != maxNotifs-1 {
		t.Fatalf("limpeza: %d linhas", n)
	}
}
