package httpx

import (
	"net/http/httptest"
	"strings"
	"testing"
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
