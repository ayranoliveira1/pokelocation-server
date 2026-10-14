package relay

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

func fakeConn(h *Hub, user string, friends ...string) *Conn {
	c := newConn(h, nil)
	c.setUser(user)
	h.register(c, newFriendSet(friends))
	return c
}

// drain faz o papel do writer.
func drain(c *Conn) (n int, last []byte) {
	for {
		it, closing := c.q.pop()
		if closing || it.ob == nil {
			return n, last
		}
		last = it.ob.b
		it.ob.release()
		n++
	}
}

func outFrame(b string) *outBuf {
	ob := getOut()
	ob.b = append(ob.b, b...)
	return ob
}

func TestQueue(t *testing.T) {
	c := newConn(New(nil, nil, nil), nil)
	for i := range queueCap {
		if !c.push(outFrame(string(rune('A'+i%26))), true) {
			t.Fatal("push de posição falhou")
		}
	}
	c.push(outFrame("nova"), true)
	if it, _ := c.q.pop(); string(it.ob.b) != "B" {
		t.Fatalf("a mais velha não saiu: %s", it.ob.b)
	}
	drain(c)

	for range queueCap {
		c.push(outFrame("env"), false)
	}
	if !c.push(outFrame("pos"), true) {
		t.Fatal("posição sem vaga deveria só ser descartada")
	}
	if c.push(outFrame("env"), false) {
		t.Fatal("envelope com a fila cheia de envelopes entrou")
	}
	if n, _ := drain(c); n != queueCap {
		t.Fatalf("na fila: %d", n)
	}

	for range queueCap {
		c.push(outFrame("pos"), true)
	}
	if !c.push(outFrame("env"), false) {
		t.Fatal("envelope não tirou uma posição")
	}
	if n, last := drain(c); n != queueCap || string(last) != "env" {
		t.Fatalf("drain: %d %s", n, last)
	}

	c.push(outFrame("x"), false)
	c.kill(1008, "unauthorized")
	<-c.done
	if _, closing := c.q.pop(); !closing {
		t.Fatal("pop não viu o fechamento")
	}
	if c.push(outFrame("y"), false) != true {
		t.Fatal("push depois do kill deveria ser ignorado")
	}
}

func TestFriendSet(t *testing.T) {
	ids := []string{
		"c0000000-0000-0000-0000-000000000003", "a0000000-0000-0000-0000-000000000001",
		"b0000000-0000-0000-0000-000000000002", "a0000000-0000-0000-0000-000000000001",
	}
	fs := newFriendSet(ids)
	if fs.len() != 3 {
		t.Fatalf("len %d", fs.len())
	}
	for i := 1; i < fs.len(); i++ {
		if string(fs.id(i-1)) >= string(fs.id(i)) {
			t.Fatal("fora de ordem")
		}
	}
	for _, id := range ids {
		if !fs.has([]byte(id)) {
			t.Errorf("sem %s", id)
		}
	}
	for _, id := range []string{"", "0", "a0000000-0000-0000-0000-000000000000", "d0000000-0000-0000-0000-000000000000"} {
		if fs.has([]byte(id)) {
			t.Errorf("tem %q", id)
		}
	}
	if e := newFriendSet(nil); e.len() != 0 || e.has([]byte(ids[0])) {
		t.Fatal("vazio")
	}
}

func readyConn(h *Hub, user string, write func([]byte) error) *Conn {
	c := fakeConn(h, user)
	c.write = write
	c.setReady()
	return c
}

func waitIdle(c *Conn) {
	for {
		c.q.mu.Lock()
		idle := !c.q.running
		c.q.mu.Unlock()
		if idle {
			return
		}
		runtime.Gosched()
	}
}

func TestOnDemandWriterOrder(t *testing.T) {
	var mu sync.Mutex
	var got []string
	c := readyConn(New(nil, nil, nil), "0b9f8c56-0d7e-4c1f-9a39-3c0d1e8e7a11", func(b []byte) error {
		mu.Lock()
		got = append(got, string(b))
		mu.Unlock()
		if len(b)%7 == 0 {
			time.Sleep(10 * time.Microsecond) // escritor lento de vez em quando
		}
		return nil
	})
	const n = 5000
	for i := range n {
		for !c.push(outFrame(fmt.Sprint(i)), false) {
			runtime.Gosched() // fila cheia: espera o escritor
		}
		if i%3 == 0 {
			runtime.Gosched()
		}
	}
	waitIdle(c)
	mu.Lock()
	defer mu.Unlock()
	if len(got) != n {
		t.Fatalf("escritos %d de %d", len(got), n)
	}
	for i, g := range got {
		if g != fmt.Sprint(i) {
			t.Fatalf("posição %d: %q", i, g)
		}
	}
}

func TestOnDemandWriterClose(t *testing.T) {
	var mu sync.Mutex
	var got []string
	c := readyConn(New(nil, nil, nil), "0b9f8c56-0d7e-4c1f-9a39-3c0d1e8e7a11", func(b []byte) error {
		mu.Lock()
		got = append(got, string(b))
		mu.Unlock()
		return nil
	})
	c.push(outFrame("a"), false)
	waitIdle(c)
	c.kill(0, "")
	<-c.done // o close também sobe sob demanda e termina
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0] != "a" {
		t.Fatalf("%v", got)
	}
}
