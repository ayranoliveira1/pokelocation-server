package ratelimit

import (
	"testing"
	"time"
)

func TestPerMinute(t *testing.T) {
	l := New(10)
	t0 := time.Unix(1_000_000, 0)
	for i := range 10 {
		if !l.Allow("a", t0) {
			t.Fatalf("chamada %d recusada", i+1)
		}
	}
	if l.Allow("a", t0) {
		t.Fatal("11ª no mesmo minuto aceita")
	}
	if !l.Allow("b", t0) {
		t.Fatal("chave b afetada pela a")
	}
	if !l.Allow("a", t0.Add(6*time.Second)) { // recarga de 1 a cada 60/10 s
		t.Fatal("sem recarga depois de 6 s")
	}
	if l.Allow("a", t0.Add(6*time.Second)) {
		t.Fatal("recarregou mais de 1 em 6 s")
	}
}

// Parada há 1 min = balde cheio: sai do mapa (memória não cresce com chaves antigas).
func TestSweep(t *testing.T) {
	l := New(10)
	t0 := time.Unix(1_000_000, 0)
	for i := range 100 {
		l.Allow(string(rune('A'+i)), t0)
	}
	l.Allow("z", t0.Add(2*time.Minute))
	if n := len(l.m); n != 1 {
		t.Fatalf("mapa com %d chaves depois da varredura, want 1", n)
	}
}

// Janela qualquer (ex.: 5 trocas de @nome por dia): balde de n, recarga de 1 a cada janela/n, varredura após a janela.
func TestWindow(t *testing.T) {
	l := NewWindow(5, 24*time.Hour)
	t0 := time.Unix(1_000_000, 0)
	for i := range 5 {
		if !l.Allow("u", t0) {
			t.Fatalf("troca %d recusada", i+1)
		}
	}
	if l.Allow("u", t0.Add(4*time.Hour)) {
		t.Fatal("6ª aceita antes de 24/5 h")
	}
	if !l.Allow("u", t0.Add(24*time.Hour/5)) {
		t.Fatal("sem recarga depois de 24/5 h")
	}
	l.Allow("v", t0.Add(25*time.Hour)) // passou a janela: u (parado desde t0+4,8 h) sai do mapa só depois de 24 h
	if _, ok := l.m["u"]; !ok {
		t.Fatal("u varrido antes de ficar parado uma janela inteira")
	}
	l.Allow("v", t0.Add(50*time.Hour))
	if _, ok := l.m["u"]; ok {
		t.Fatal("u parado há mais de uma janela continua no mapa")
	}
}
