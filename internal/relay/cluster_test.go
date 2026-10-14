package relay

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"

	"github.com/ayranoliveira1/pokelocation-server/internal/testdb"
	"github.com/ayranoliveira1/pokelocation-server/internal/wire"
)

func TestCluster(t *testing.T) {
	e1 := newEnv(t)
	e2 := newEnvPool(t, e1.pool)
	rdb := testdb.Redis(t)
	e1.hub.EnableCluster(t.Context(), rdb)
	e2.hub.EnableCluster(t.Context(), rdb)

	a, b := e1.user(), e1.user()
	e1.friends(a, b)
	ca := e1.online(a)
	cb := e2.online(b) // o Auth só termina com u:{b} assinado: o envelope logo abaixo não pode se perder

	ca.send(envelope(b, "m1", "oi"))
	if f := ca.read(); f.Type != wire.Ack || string(f.Peer) != b {
		t.Fatalf("a: quero Ack, veio %d", f.Type)
	}
	if f := cb.read(); f.Type != wire.Envelope || string(f.Peer) != a || string(f.Payload) != "oi" {
		t.Fatalf("b: quero Envelope de a, veio %d %s", f.Type, f.Payload)
	}

	e1.hub.SendFriends(a, wire.AppendLocationUpdate(Buf(), []byte(a), []byte("p1")))
	if f := cb.read(); f.Type != wire.LocationUpdate || string(f.Payload) != "p1" {
		t.Fatalf("b: quero LocationUpdate, veio %d", f.Type)
	}

	// a nas duas instâncias: cada conexão recebe uma vez só (sem eco)
	ca2 := e2.online(a)
	subscribed(t, rdb, a, 2)
	cb.send(envelope(a, "m2", "oi2"))
	_ = cb.read()
	for _, c := range []*client{ca, ca2} {
		if f := c.read(); f.Type != wire.Envelope || string(f.Payload) != "oi2" {
			t.Fatalf("a: quero Envelope oi2, veio %d", f.Type)
		}
		c.quiet(300 * time.Millisecond)
	}

	if _, err := e1.pool.Exec(context.Background(), `DELETE FROM friendships`); err != nil {
		t.Fatal(err)
	}
	e1.hub.ReloadFriends(b)
	deadline := time.Now().Add(5 * time.Second)
	for {
		cb.send(envelope(a, "m3-"+time.Now().Format("150405.000"), "x"))
		f := cb.read()
		if f.Type == wire.Error && string(f.Code) == "forbidden" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("instância 2 não recarregou os amigos de b")
		}
		time.Sleep(50 * time.Millisecond)
	}

	e1.hub.Kick(b)
	cb.wantClose(websocket.ClosePolicyViolation)
	subscribed(t, rdb, b, 0)
}

func TestClusterCtlNotDropped(t *testing.T) {
	e1 := newEnv(t)
	e2 := newEnvPool(t, e1.pool)
	rdb := testdb.Redis(t)
	e1.hub.enableCluster(t.Context(), rdb, 0, false) // fila de capacidade 0 e sem publisher: todo frame descartado
	e2.hub.EnableCluster(t.Context(), rdb)

	a, b := e1.user(), e1.user()
	e1.friends(a, b)
	cb := e2.online(b)

	if _, err := e1.pool.Exec(context.Background(), `DELETE FROM friendships`); err != nil {
		t.Fatal(err)
	}
	e1.hub.ReloadFriends(b)
	deadline := time.Now().Add(5 * time.Second)
	for i := 0; ; i++ {
		cb.send(envelope(a, fmt.Sprintf("r%d", i), "x"))
		if f := cb.read(); f.Type == wire.Error && string(f.Code) == "forbidden" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Reload não chegou com a fila de frames cheia")
		}
		time.Sleep(50 * time.Millisecond)
	}
	e1.hub.Kick(b)
	cb.wantClose(websocket.ClosePolicyViolation)
}

func subscribed(t *testing.T, rdb *redis.Client, user string, n int64) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if m, _ := rdb.PubSubNumSub(context.Background(), "u:"+user).Result(); m["u:"+user] == n {
			return
		}
	}
	t.Fatalf("u:%s sem %d assinantes", user, n)
}
