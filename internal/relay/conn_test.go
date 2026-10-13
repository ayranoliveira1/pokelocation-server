package relay

import "testing"

// Conexão sem socket: só a fila (o writer de teste é drain).
func fakeConn(h *Hub, user string, friends ...string) *Conn {
	c := newConn(h, nil)
	c.setUser(user)
	h.register(c, newFriendSet(friends))
	return c
}

// drain faz o papel do writer: tira tudo da fila e devolve os buffers.
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
	// cheia de posições: posição nova descarta a mais velha
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

	// cheia de envelopes: posição nova é descartada, envelope novo falha (quem chama fecha)
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

	// envelope com a fila cheia de posições: tira uma posição
	for range queueCap {
		c.q.push(outFrame("pos"), true)
	}
	if !c.q.push(outFrame("env"), false) {
		t.Fatal("envelope não tirou uma posição")
	}
	if n, last := drain(c); n != queueCap || string(last) != "env" {
		t.Fatalf("drain: %d %s", n, last)
	}

	// fechando: nada entra, pop avisa
	c.q.push(outFrame("x"), false)
	c.kill(1008, "unauthorized")
	if _, closing := c.q.pop(); !closing {
		t.Fatal("pop não viu o fechamento")
	}
	if c.q.push(outFrame("y"), false) != true {
		t.Fatal("push depois do kill deveria ser ignorado")
	}
}
