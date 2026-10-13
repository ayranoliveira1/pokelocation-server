package wire

import (
	"bytes"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

const uid = "0f8fad5b-d9cb-469f-a165-70867728950e"

// Bytes escritos à mão no formato do social/ws/Frames.java: se isto quebrar, o app não fala mais com o servidor.
func TestGolden(t *testing.T) {
	u := hex.EncodeToString([]byte(uid))
	for name, c := range map[string]struct {
		got  []byte
		want string
	}{
		"auth":          {AppendAuth(nil, []byte("tk")), "01" + "0002" + "746b"},
		"envelope":      {AppendEnvelope(nil, []byte(uid), []byte("m1"), []byte{0xca, 0xfe}), "02" + "0024" + u + "0002" + "6d31" + "00000002" + "cafe"},
		"ack":           {AppendAck(nil, []byte(uid), []byte("m1")), "03" + "0024" + u + "0002" + "6d31"},
		"location":      {AppendLocation(nil, []byte{1}), "04" + "00000001" + "01"},
		"locationUpd":   {AppendLocationUpdate(nil, []byte(uid), []byte{1}), "05" + "0024" + u + "00000001" + "01"},
		"locationOff":   {AppendLocationOff(nil, nil), "06"},
		"locationOffSC": {AppendLocationOff(nil, []byte(uid)), "06" + "0024" + u},
		"error":         {AppendError(nil, "forbidden"), "07" + "0009" + hex.EncodeToString([]byte("forbidden"))},
	} {
		if g := hex.EncodeToString(c.got); g != c.want {
			t.Errorf("%s: %s, want %s", name, g, c.want)
		}
		f, err := Decode(c.got)
		if err != nil {
			t.Errorf("%s: decode: %v", name, err)
			continue
		}
		if re := reencode(f); !bytes.Equal(re, c.got) {
			t.Errorf("%s: reencode %x", name, re)
		}
	}
}

func TestDecodeInvalid(t *testing.T) {
	env := AppendEnvelope(nil, []byte(uid), []byte("m1"), []byte("x"))
	for name, c := range map[string]struct {
		b    []byte
		want error
	}{
		"vazio":              {nil, ErrBadFrame},
		"tipo 0":             {[]byte{0}, ErrBadFrame},
		"tipo 8":             {[]byte{8}, ErrBadFrame},
		"sobra":              {append(env[:len(env):len(env)], 0), ErrBadFrame},
		"cortado":            {env[:len(env)-1], ErrBadFrame},
		"bin além do fim":    {[]byte{Location, 0xff, 0xff, 0xff, 0xff}, ErrBadFrame},
		"uuid maiúsculo":     {AppendAck(nil, []byte(strings.ToUpper(uid)), []byte("m")), ErrBadFrame},
		"uuid curto":         {AppendAck(nil, []byte(uid[1:]), []byte("m")), ErrBadFrame},
		"msg id vazio":       {AppendAck(nil, []byte(uid), nil), ErrBadFrame},
		"msg id 65":          {AppendAck(nil, []byte(uid), bytes.Repeat([]byte("a"), 65)), ErrBadFrame},
		"auth vazio":         {AppendAuth(nil, nil), ErrBadFrame},
		"auth 2049":          {AppendAuth(nil, bytes.Repeat([]byte("a"), MaxToken+1)), ErrBadFrame},
		"envelope 32KiB+1":   {AppendEnvelope(nil, []byte(uid), []byte("m"), make([]byte, MaxEnvelopePayload+1)), ErrTooLarge},
		"location 1KiB+1":    {AppendLocation(nil, make([]byte, MaxLocationPayload+1)), ErrTooLarge},
		"locationUpd 1KiB+1": {AppendLocationUpdate(nil, []byte(uid), make([]byte, MaxLocationPayload+1)), ErrTooLarge},
		"envelope vazio":     {AppendEnvelope(nil, []byte(uid), []byte("m"), nil), ErrBadFrame},
		"location vazia":     {AppendLocation(nil, nil), ErrBadFrame},
		"locationUpd vazia":  {AppendLocationUpdate(nil, []byte(uid), nil), ErrBadFrame},
	} {
		if _, err := Decode(c.b); !errors.Is(err, c.want) {
			t.Errorf("%s: %v, want %v", name, err, c.want)
		}
	}
	for _, ok := range [][]byte{
		AppendEnvelope(nil, []byte(uid), []byte("m"), make([]byte, MaxEnvelopePayload)),
		AppendLocation(nil, make([]byte, MaxLocationPayload)),
	} {
		if _, err := Decode(ok); err != nil {
			t.Errorf("no limite: %v", err)
		}
	}
}

// Os campos fatiam o frame com cap fechado: um append num campo não sobrescreve o vizinho.
func TestFieldsDoNotAlias(t *testing.T) {
	b := AppendEnvelope(nil, []byte(uid), []byte("m1"), []byte("pp"))
	f, _ := Decode(b)
	_ = append(f.MsgID, 'X')
	if g, _ := Decode(b); string(g.Payload) != "pp" {
		t.Fatal("append em MsgID alterou o frame")
	}
}

// Meta da spec §6: repassar um frame (decode + reencode para o destinatário) não aloca. CI falha se piorar.
func TestZeroAllocs(t *testing.T) {
	env := AppendEnvelope(nil, []byte(uid), []byte("4d7e1f4a-msg"), make([]byte, 512))
	loc := AppendLocation(nil, make([]byte, 120))
	from := []byte(uid)
	out := make([]byte, 0, 1024)
	if n := testing.AllocsPerRun(1000, func() {
		f, _ := Decode(env)
		out = AppendEnvelope(out[:0], from, f.MsgID, f.Payload)
		f, _ = Decode(loc)
		out = AppendLocationUpdate(out[:0], from, f.Payload)
	}); n != 0 {
		t.Fatalf("%v alocações por repasse, want 0", n)
	}
}

func reencode(f Frame) []byte {
	switch f.Type {
	case Auth:
		return AppendAuth(nil, f.Peer)
	case Envelope:
		return AppendEnvelope(nil, f.Peer, f.MsgID, f.Payload)
	case Ack:
		return AppendAck(nil, f.Peer, f.MsgID)
	case Location:
		return AppendLocation(nil, f.Payload)
	case LocationUpdate:
		return AppendLocationUpdate(nil, f.Peer, f.Payload)
	case LocationOff:
		return AppendLocationOff(nil, f.Peer)
	default:
		return AppendError(nil, string(f.Code))
	}
}

// Nenhuma entrada derruba o Decode; o que ele aceita volta idêntico pelo Append.
func FuzzDecode(f *testing.F) {
	f.Add(AppendEnvelope(nil, []byte(uid), []byte("m1"), []byte("x")))
	f.Add(AppendAuth(nil, []byte("tk")))
	f.Add(AppendLocationOff(nil, []byte(uid)))
	f.Add([]byte{Location, 0, 0, 0, 1, 7})
	f.Fuzz(func(t *testing.T, b []byte) {
		fr, err := Decode(b)
		if err != nil {
			return
		}
		if re := reencode(fr); !bytes.Equal(re, b) {
			t.Fatalf("aceito mas não reencoda igual: %x → %x", b, re)
		}
	})
}

func BenchmarkRelayEnvelope(b *testing.B) {
	env := AppendEnvelope(nil, []byte(uid), []byte("4d7e1f4a-msg"), make([]byte, 512))
	from := []byte(uid)
	out := make([]byte, 0, 1024)
	b.ReportAllocs()
	for b.Loop() {
		f, _ := Decode(env)
		out = AppendEnvelope(out[:0], from, f.MsgID, f.Payload)
	}
}

func BenchmarkRelayLocation(b *testing.B) {
	loc := AppendLocation(nil, make([]byte, 120))
	from := []byte(uid)
	out := make([]byte, 0, 256)
	b.ReportAllocs()
	for b.Loop() {
		f, _ := Decode(loc)
		out = AppendLocationUpdate(out[:0], from, f.Payload)
	}
}
