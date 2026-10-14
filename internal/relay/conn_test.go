package relay

import "testing"

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
		if !c.q.push(outFrame(string(rune('A'+i%26))), true) {
			t.Fatal("push de posição falhou")
		}
	}
	c.q.push(outFrame("nova"), true)
	if it, _ := c.q.pop(); string(it.ob.b) != "B" {
		t.Fatalf("a mais velha não saiu: %s", it.ob.b)
	}
	drain(c)

	for range queueCap {
		c.q.push(outFrame("env"), false)
	}
	if !c.q.push(outFrame("pos"), true) {
		t.Fatal("posição sem vaga deveria só ser descartada")
	}
	if c.q.push(outFrame("env"), false) {
		t.Fatal("envelope com a fila cheia de envelopes entrou")
	}
	if n, _ := drain(c); n != queueCap {
		t.Fatalf("na fila: %d", n)
	}

	for range queueCap {
		c.q.push(outFrame("pos"), true)
	}
	if !c.q.push(outFrame("env"), false) {
		t.Fatal("envelope não tirou uma posição")
	}
	if n, last := drain(c); n != queueCap || string(last) != "env" {
		t.Fatalf("drain: %d %s", n, last)
	}

	c.q.push(outFrame("x"), false)
	c.kill(1008, "unauthorized")
	if _, closing := c.q.pop(); !closing {
		t.Fatal("pop não viu o fechamento")
	}
	if c.q.push(outFrame("y"), false) != true {
		t.Fatal("push depois do kill deveria ser ignorado")
	}
}
