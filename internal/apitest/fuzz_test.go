package apitest

import (
	"strings"
	"testing"
)

// FuzzREST manda corpos arbitrários a toda rota REST com JSON (docs/api.md §2–§5) do binário real, autenticado.
// Nenhuma entrada pode dar 5xx, derrubar o servidor ou responder erro fora do formato {"error": código}.
// Cada worker do fuzz sobe o seu servidor (e containers): rode com -parallel 1, pelo gtest. A cobertura que guia o
// fuzz é só a do cliente (o servidor é outro processo), então é mutação quase às cegas, e minimizar entradas
// "interessantes" não ajuda: sem -fuzzminimizetime 1x o motor passa até 60 s por entrada nova parado em 0 execs/s.
//
//	tools/gtest.sh -run '^$' -fuzz '^FuzzREST$' -fuzztime 60s -fuzzminimizetime 1x -parallel 1 ./internal/apitest/
func FuzzREST(f *testing.F) {
	s := start(f)
	s.need("POST", "/v1/auth/login")
	u := s.register("fuzz")
	routes := []struct{ m, p string }{
		{"POST", "/v1/auth/register"},
		{"POST", "/v1/auth/login"},
		{"POST", "/v1/auth/refresh"},
		{"POST", "/v1/auth/logout"},
		{"POST", "/v1/auth/google/exchange"},
		{"POST", "/v1/friends/requests"},
		{"PUT", "/v1/keys"},
		{"POST", "/v1/envelopes/ack"},
	}
	seeds := []string{
		`{"email":"x@teste.dev","password":"senha-longa-1"}`,
		`{"refresh_token":"lixo"}`,
		`{"code":"abc","code_verifier":"x"}`,
		`{"code":"7K3M-9QXA"}`,
		`{"registration_id":1,"identity_key":"BQ==","signed_prekey":{"key_id":1,"public_key":"BQ==","signature":"AA=="},` +
			`"kyber_last_resort":{"key_id":1,"public_key":"CA==","signature":"AA=="},"one_time_prekeys":[],"kyber_prekeys":[]}`,
		`{"ids":["1","x",""]}`,
		`{"ids":[` + strings.Repeat(`"1",`, 200) + `"1"]}`,
		`{"ids":"x"}`, `{"email":null}`, `[]`, `null`, `{}`, `{"a":1}{"a":2}`, "\xff\xfe", `{"email":"` + strings.Repeat("a", 5000) + `"}`,
	}
	for i := range routes {
		for _, b := range seeds {
			f.Add(uint8(i), b)
		}
	}
	f.Fuzz(func(t *testing.T, ri uint8, body string) {
		s := s.on(t)
		r := routes[int(ri)%len(routes)]
		res, err := s.raw(r.m, r.p, "Bearer "+u.access, body)
		if err != nil {
			t.Fatalf("%s %s: sem resposta (servidor caiu?): %v", r.m, r.p, err)
		}
		if res.status >= 500 {
			t.Fatalf("%s %s: %d %s", r.m, r.p, res.status, res.raw)
		}
		if res.status >= 400 && res.status != 404 && res.errCode() == "" { // 404 de texto = rota do S6 ainda ausente
			t.Fatalf("%s %s: erro %d fora do formato: %q", r.m, r.p, res.status, res.raw)
		}
	})
}
