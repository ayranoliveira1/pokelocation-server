//go:build !race

// Sem -race: com o detector, o sync.Pool descarta itens de propósito e a contagem de alocações perde o sentido.
// CI: go test -run ZeroAllocs ./... (sem -race).

package relay

import (
	"testing"
	"time"

	"github.com/ayranoliveira1/pokelocation-server/internal/wire"
)

// Meta: 0 alocações por frame repassado (o INSERT no Postgres fica de fora).
func TestZeroAllocs(t *testing.T) {
	h := New(nil, nil, nil)
	const a, b = "0b9f8c56-0d7e-4c1f-9a39-3c0d1e8e7a11", "1c0f9d67-1e8f-4d20-8b4a-4d1e2f9f8b22"
	ca := fakeConn(h, a, b)
	cb1, cb2 := fakeConn(h, b, a), fakeConn(h, b, a)
	msg := wire.AppendEnvelope(nil, []byte(b), []byte("m1"), make([]byte, 600))
	loc := wire.AppendLocationUpdate(nil, []byte(a), make([]byte, 200))
	for name, fn := range map[string]func(){
		"envelope": func() {
			f, _ := wire.Decode(msg)
			if !ca.friend(f.Peer) {
				t.Fatal("não é amigo")
			}
			h.relayEnvelope(ca, f, true)
		},
		"limite de chat": func() { h.chat.Allow(a, time.Unix(0, 0)) },
		"limite de grupo": func() {
			h.groupRcpt.Allow(a, time.Unix(0, 0))
			h.groupRcpt.Debit(a, time.Unix(0, 0), 49)
		},
		"posição": func() {
			ob := getOut()
			ob.b = append(ob.b, loc...)
			h.sendFriends(a, ob)
		},
	} {
		fn() // aquece o pool
		drain(ca)
		drain(cb1)
		drain(cb2)
		if n := testing.AllocsPerRun(1000, func() {
			fn()
			drain(ca)
			drain(cb1)
			drain(cb2)
		}); n != 0 {
			t.Errorf("%s: %.1f allocs/op", name, n)
		}
	}
}

func BenchmarkRelayEnvelope(b *testing.B) {
	h := New(nil, nil, nil)
	const ua, ub = "0b9f8c56-0d7e-4c1f-9a39-3c0d1e8e7a11", "1c0f9d67-1e8f-4d20-8b4a-4d1e2f9f8b22"
	ca, cb := fakeConn(h, ua, ub), fakeConn(h, ub, ua)
	msg := wire.AppendEnvelope(nil, []byte(ub), []byte("m1"), make([]byte, 600))
	b.ReportAllocs()
	for b.Loop() {
		f, _ := wire.Decode(msg)
		if ca.friend(f.Peer) {
			h.relayEnvelope(ca, f, true)
		}
		drain(ca)
		drain(cb)
	}
}

func TestZeroAllocsOnDemandWriter(t *testing.T) {
	h := New(nil, nil, nil)
	const a, b = "0b9f8c56-0d7e-4c1f-9a39-3c0d1e8e7a11", "1c0f9d67-1e8f-4d20-8b4a-4d1e2f9f8b22"
	ca := readyConn(h, a, func([]byte) error { return nil })
	cb := readyConn(h, b, func([]byte) error { return nil })
	ca.u.friends.Store(newFriendSet([]string{b}))
	msg := wire.AppendEnvelope(nil, []byte(b), []byte("m1"), make([]byte, 600))
	run := func() {
		f, _ := wire.Decode(msg)
		h.relayEnvelope(ca, f, true)
		waitIdle(ca)
		waitIdle(cb)
	}
	for range 100 {
		run()
	}
	if n := testing.AllocsPerRun(1000, run); n != 0 {
		t.Errorf("%.2f allocs/op com o escritor subindo a cada frame", n)
	}
}
