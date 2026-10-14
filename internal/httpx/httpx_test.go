package httpx

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestDecode(t *testing.T) {
	for body, ok := range map[string]bool{
		`{"a":"x"}`:          true,
		`{"a":"x"} `:         true,
		`{"a":`:              false,
		`{"a":"x"}{"a":"y"}`: false,
		`{"a":"x"}}`:         false,
		`{"a":"` + strings.Repeat("x", 64) + `"}`: false, // passa de max
	} {
		w := httptest.NewRecorder()
		var v struct{ A string }
		got := Decode(w, httptest.NewRequest("POST", "/", strings.NewReader(body)), 32, &v)
		if got != ok {
			t.Errorf("%q: got %v", body, got)
		}
		if !got && (w.Code != 400 || !strings.Contains(w.Body.String(), `"error":"bad_request"`)) {
			t.Errorf("%q: resposta %d %s", body, w.Code, w.Body)
		}
	}
}

func TestDBError(t *testing.T) {
	for err, want := range map[error]int{
		fmt.Errorf("x: %w", &pgconn.PgError{Code: "23505", Message: "segredo@x"}): 500,
		context.DeadlineExceeded: 503,
	} {
		w := httptest.NewRecorder()
		DBError(w, "t", err)
		if w.Code != want || strings.Contains(w.Body.String(), "segredo") {
			t.Errorf("%v: %d %s", err, w.Code, w.Body)
		}
	}
}

func TestTimed(t *testing.T) {
	var deadline time.Time
	Timed(time.Second, func(_ http.ResponseWriter, r *http.Request) { deadline, _ = r.Context().Deadline() })(
		httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
	if d := time.Until(deadline); d <= 0 || d > time.Second {
		t.Fatalf("prazo %v", d)
	}
}
