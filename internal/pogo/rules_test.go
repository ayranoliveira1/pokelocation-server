package pogo

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
)

func TestRules(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	h := newHandler(t, pool, "http://127.0.0.1:1")
	u, other := newUser(t, pool), newUser(t, pool) // sem vínculo PoGo: o CRUD não exige

	w := serve(h, u, "POST", "/v1/pogo/rules", `{"name":"  Dratini 100 ","filters":{"pokemon_ids":[147],"iv_min":100},
		"scope":"near","lat":-23.5,"lng":-46.6,"radius_km":2}`)
	var near Rule
	if w.Code != 201 || json.Unmarshal(w.Body.Bytes(), &near) != nil || near.Name != "Dratini 100" || !near.Enabled ||
		near.ID == "" || near.CreatedAt == 0 || *near.RadiusKm != 2 {
		t.Fatalf("criar: %d %s", w.Code, w.Body.String())
	}
	w = serve(h, u, "POST", "/v1/pogo/rules", `{"name":"Mundo","filters":{},"scope":"world","lat":1,"lng":2,
		"radius_km":3,"enabled":false}`)
	if w.Code != 201 || !strings.Contains(w.Body.String(), `"lat":null,"lng":null,"radius_km":null,"enabled":false`) {
		t.Fatalf("world: %d %s", w.Code, w.Body.String())
	}

	var list struct{ Rules []Rule }
	w = serve(h, u, "GET", "/v1/pogo/rules", "")
	if json.Unmarshal(w.Body.Bytes(), &list) != nil || len(list.Rules) != 2 || list.Rules[0].ID != near.ID {
		t.Fatalf("listar: %s", w.Body.String())
	}
	if w := serve(h, other, "GET", "/v1/pogo/rules", ""); w.Body.String() != `{"rules":[]}`+"\n" {
		t.Fatalf("outro usuário: %s", w.Body.String())
	}

	w = serve(h, u, "PUT", "/v1/pogo/rules/"+strings.ToUpper(near.ID), `{"name":"X","filters":{"iv_min":90},
		"scope":"near","lat":1,"lng":1,"radius_km":5,"enabled":false}`)
	var upd Rule
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &upd) != nil || upd.ID != near.ID || upd.Enabled ||
		upd.CreatedAt != near.CreatedAt || string(upd.Filters) != `{"iv_min":90}` {
		t.Fatalf("put: %d %s", w.Code, w.Body.String())
	}
	put := `{"name":"X","filters":{},"scope":"world"}`
	wantError(t, serve(h, other, "PUT", "/v1/pogo/rules/"+near.ID, put), 404, "not_found")
	wantError(t, serve(h, u, "PUT", "/v1/pogo/rules/xyz", put), 404, "not_found")
	wantError(t, serve(h, other, "DELETE", "/v1/pogo/rules/"+near.ID, ""), 404, "not_found")
	if w := serve(h, u, "DELETE", "/v1/pogo/rules/"+near.ID, ""); w.Code != 204 {
		t.Fatalf("delete: %d", w.Code)
	}
	wantError(t, serve(h, u, "DELETE", "/v1/pogo/rules/"+near.ID, ""), 404, "not_found")

	bad := newUser(t, pool)
	for _, b := range []string{
		`{"name":"","filters":{},"scope":"world"}`,
		`{"name":"` + strings.Repeat("é", maxName+1) + `","filters":{},"scope":"world"}`,
		`{"name":"a","filters":[],"scope":"world"}`,
		`{"name":"a","scope":"world"}`,
		`{"name":"a","filters":{"x":"` + strings.Repeat("a", maxFilters) + `"},"scope":"world"}`,
		`{"name":"a","filters":{},"scope":"near","lat":1,"lng":1}`,
		`{"name":"a","filters":{},"scope":"near","lat":1,"lng":1,"radius_km":51}`,
		`{"name":"a","filters":{},"scope":"near","lat":91,"lng":1,"radius_km":1}`,
		`{"name":"a","filters":{},"scope":"near","lat":1,"lng":1,"radius_km":0}`,
		`{"name":"a","filters":{},"scope":"x"}`,
	} {
		wantError(t, serve(h, bad, "POST", "/v1/pogo/rules", b), 400, "bad_request")
	}
}

func TestRulesLimit(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	h := newHandler(t, pool, "http://127.0.0.1:1")
	u := newUser(t, pool)
	errs := make(chan int, maxRules+5)
	for range maxRules + 5 { // juntas: o teto vale mesmo com corrida
		go func() {
			errs <- serve(h, u, "POST", "/v1/pogo/rules", `{"name":"a","filters":{},"scope":"world"}`).Code
		}()
	}
	created, full := 0, 0
	for range maxRules + 5 {
		switch <-errs {
		case 201:
			created++
		case 409:
			full++
		}
	}
	if created != maxRules || full != 5 {
		t.Fatalf("criadas %d, recusadas %d", created, full)
	}
	wantError(t, serve(h, u, "POST", "/v1/pogo/rules", `{"name":"a","filters":{},"scope":"world"}`), 409, "too_many_rules")
}

func TestRulesTrainer(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	h := newHandler(t, pool, "http://127.0.0.1:1")
	rule := func(filters string) string { return `{"name":"a","filters":` + filters + `,"scope":"world"}` }
	post := func(u, filters string) *httptest.ResponseRecorder {
		return serve(h, u, "POST", "/v1/pogo/rules", rule(filters))
	}
	noLink, noTrainer, ok := newUser(t, pool), newUser(t, pool), newUser(t, pool)
	link(t, h, noTrainer, "t", `{"trainer_code":"111","active":false}`)
	link(t, h, ok, "t", `{"trainer_code":"111","active":true}`, `{"trainer_code":"222","active":false}`,
		`{"trainer_code":"333"}`)

	for _, f := range []string{`{"shiny_only":true}`, `{"background_only":true}`, `{"shiny_account":"111"}`} {
		wantError(t, post(noLink, f), 422, "trainer_required")
		wantError(t, post(noTrainer, f), 422, "trainer_required")
	}
	for _, f := range []string{`{}`, `{"shiny_only":false,"background_only":false,"shiny_account":null}`} {
		if w := post(noLink, f); w.Code != 201 {
			t.Fatalf("sem shiny, sem vínculo: %d %s", w.Code, w.Body.String())
		}
	}
	for _, f := range []string{`{"shiny_only":true}`, `{"background_only":true,"shiny_account":"111"}`,
		`{"shiny_account":"333"}`} {
		if w := post(ok, f); w.Code != 201 || !strings.Contains(w.Body.String(), `"disabled_reason":null`) {
			t.Fatalf("%s: %d %s", f, w.Code, w.Body.String())
		}
	}
	for _, f := range []string{`{"shiny_account":"222"}`, `{"shiny_only":true,"shiny_account":"999"}`} {
		wantError(t, post(ok, f), 422, "shiny_account_unavailable")
	}
	wantError(t, post(ok, `{"shiny_only":"sim"}`), 400, "bad_request")

	id := addRule(t, h, noTrainer, rule(`{}`))
	wantError(t, serve(h, noTrainer, "PUT", "/v1/pogo/rules/"+id, rule(`{"shiny_only":true}`)), 422, "trainer_required")

	// desligada não roda: criar, desligar e renomear sem treinador valem; religar confere
	off := `{"name":"b","filters":{"shiny_only":true},"scope":"world","enabled":false}`
	if w := serve(h, noTrainer, "POST", "/v1/pogo/rules", off); w.Code != 201 {
		t.Fatalf("POST desligada: %d %s", w.Code, w.Body.String())
	}
	if w := serve(h, noTrainer, "PUT", "/v1/pogo/rules/"+id, off); w.Code != 200 {
		t.Fatalf("PUT desligando: %d %s", w.Code, w.Body.String())
	}
	wantError(t, serve(h, noTrainer, "PUT", "/v1/pogo/rules/"+id,
		`{"name":"b","filters":{"shiny_only":true},"scope":"world","enabled":true}`), 422, "trainer_required")
}

// O worker desliga a regra só se ela ainda é a que ele leu; um PUT do usuário limpa o motivo.
func TestDisableRule(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	h := newHandler(t, pool, "http://127.0.0.1:1")
	u := newUser(t, pool)
	link(t, h, u, "t", `{"trainer_code":"111"}`)
	fcm := "f-" + u
	withDevice(t, pool, u, &fcm)
	id := addRule(t, h, u, `{"name":"a","filters":{"shiny_only":true,"pokemon_ids":[1,2]},"scope":"world"}`)
	ctx := context.Background()
	_, _, rules, err := h.svc.store.userRules(ctx, u)
	if err != nil || len(rules) != 1 {
		t.Fatal(rules, err)
	}
	stale := rules[0]
	stale.Filters = json.RawMessage(`{"shiny_only":true}`) // o usuário editou depois da leitura
	if err := h.svc.store.disableRule(ctx, u, &stale, reasonTrainer); err != nil {
		t.Fatal(err)
	}
	var list struct{ Rules []Rule }
	get := func() Rule {
		_ = json.Unmarshal(serve(h, u, "GET", "/v1/pogo/rules", "").Body.Bytes(), &list)
		return list.Rules[0]
	}
	if r := get(); !r.Enabled || r.DisabledReason != nil {
		t.Fatalf("desligou uma regra editada: %+v", r)
	}
	if err := h.svc.store.disableRule(ctx, u, &rules[0], reasonTrainer); err != nil {
		t.Fatal(err)
	}
	if r := get(); r.Enabled || r.DisabledReason == nil || *r.DisabledReason != "trainer_required" {
		t.Fatalf("não desligou: %+v", r)
	}
	if w := serve(h, u, "PUT", "/v1/pogo/rules/"+id, `{"name":"a","filters":{"shiny_only":true},"scope":"world",
		"enabled":true}`); w.Code != 200 || !strings.Contains(w.Body.String(), `"enabled":true,"disabled_reason":null`) {
		t.Fatalf("PUT: %d %s", w.Code, w.Body.String())
	}
}

// trainer/confirm e /me atualizam os treinadores salvos: o alerta de shiny vale na hora.
func TestTrainerRefresh(t *testing.T) {
	t.Parallel()
	pool := testdb.Pool(t)
	up := http.NewServeMux()
	up.HandleFunc("POST /trainer/confirm", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"link":{"trainer_code":"521284665027"},"accounts":[{"trainer_code":"521284665027",`+
			`"active":true},{"trainer_code":"1","active":false}],"cap":1}`)
	})
	up.HandleFunc("GET /users/me", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"display_name":"N","trainers":[{"trainer_code":"777","active":true}]}`)
	})
	h := newHandler(t, pool, fakeUpstream(t, up))
	shiny := func(acc string) string {
		return `{"name":"a","filters":{"shiny_only":true,"shiny_account":"` + acc + `"},"scope":"world"}`
	}
	u := newUser(t, pool)
	link(t, h, u, secretTok)
	wantError(t, serve(h, u, "POST", "/v1/pogo/rules", shiny("521284665027")), 422, "trainer_required")
	if w := serve(h, u, "POST", "/v1/pogo/trainer/confirm", `{"trainer_code":"521284665027"}`); w.Code != 200 ||
		!strings.Contains(w.Body.String(), `"cap":1`) {
		t.Fatalf("confirm: %d %s", w.Code, w.Body.String())
	}
	if w := serve(h, u, "POST", "/v1/pogo/rules", shiny("521284665027")); w.Code != 201 {
		t.Fatalf("depois do confirm: %d %s", w.Code, w.Body.String())
	}
	wantError(t, serve(h, u, "POST", "/v1/pogo/rules", shiny("777")), 422, "shiny_account_unavailable")
	if w := serve(h, u, "GET", "/v1/pogo/me", ""); w.Code != 200 {
		t.Fatalf("me: %d", w.Code)
	}
	if w := serve(h, u, "POST", "/v1/pogo/rules", shiny("777")); w.Code != 201 {
		t.Fatalf("depois do /me: %d %s", w.Code, w.Body.String())
	}
}
