// Package wire: frames binários do WebSocket (docs/api.md §6), espelhados por social/ws/Frames.java no app.
// [tipo u8][campos]; big-endian; str = u16 + bytes; bin = u32 + bytes; nada sobrando.
// Caminho quente sem alocação: Decode fatia o buffer recebido e os Append* escrevem num buffer do chamador.
package wire

import (
	"encoding/binary"
	"errors"
	"math"
)

const (
	Auth           byte = 1 // C→S: str token
	Envelope       byte = 2 // C→S: str to, str client_msg_id, bin payload · S→C: str from, …
	Ack            byte = 3 // C→S: str from, str client_msg_id · S→C: str to, str client_msg_id
	Location       byte = 4 // C→S: bin payload
	LocationUpdate byte = 5 // S→C: str from, bin payload
	LocationOff    byte = 6 // C→S: vazio · S→C: str from
	Error          byte = 7 // S→C: str code
)

const (
	MaxToken           = 2048
	MaxMsgID           = 64
	MaxEnvelopePayload = 32 << 10
	MaxLocationPayload = 1 << 10
	uuidLen            = 36
)

var (
	ErrBadFrame = errors.New("bad_frame")
	ErrTooLarge = errors.New("too_large") // bem formado, payload acima do limite do tipo
)

// Frame: os campos FATIAM o buffer de entrada e valem só enquanto ele não for reutilizado.
type Frame struct {
	Type    byte
	Peer    []byte // Auth: token; Envelope/Ack/LocationUpdate/LocationOff: o outro usuário (UUID); nil se não houver
	MsgID   []byte // Envelope, Ack
	Code    []byte // Error
	Payload []byte // Envelope, Location, LocationUpdate
}

// Decode aceita as duas direções; recusar o tipo errado para a direção é de quem chama.
func Decode(b []byte) (Frame, error) {
	r := reader{b: b}
	f := Frame{Type: r.u8()}
	switch f.Type {
	case Auth:
		f.Peer = r.str()
		if r.ok && (len(f.Peer) == 0 || len(f.Peer) > MaxToken) {
			return f, ErrBadFrame
		}
	case Envelope:
		f.Peer, f.MsgID, f.Payload = r.uuid(), r.msgID(), r.bin()
	case Ack:
		f.Peer, f.MsgID = r.uuid(), r.msgID()
	case Location:
		f.Payload = r.bin()
	case LocationUpdate:
		f.Peer, f.Payload = r.uuid(), r.bin()
	case LocationOff:
		if len(r.b) > r.i { // C→S é vazio
			f.Peer = r.uuid()
		}
	case Error:
		f.Code = r.str()
	default:
		return f, ErrBadFrame
	}
	if !r.ok || r.i != len(b) {
		return f, ErrBadFrame
	}
	if f.Payload != nil && len(f.Payload) == 0 { // o app nunca manda; envelope vazio viraria linha inútil no banco
		return f, ErrBadFrame
	}
	if (f.Type == Envelope && len(f.Payload) > MaxEnvelopePayload) ||
		((f.Type == Location || f.Type == LocationUpdate) && len(f.Payload) > MaxLocationPayload) {
		return f, ErrTooLarge
	}
	return f, nil
}

// reader: depois do primeiro erro (ok = false), tudo devolve nil; o Decode confere uma vez só no fim.
type reader struct {
	b  []byte
	i  int
	ok bool
}

func (r *reader) u8() byte {
	if len(r.b) == 0 {
		return 0
	}
	r.ok = true
	r.i = 1
	return r.b[0]
}

func (r *reader) take(n int) []byte {
	if !r.ok || n > len(r.b)-r.i {
		r.ok = false
		return nil
	}
	v := r.b[r.i : r.i+n : r.i+n] // cap fechado: append no campo nunca escreve no frame vizinho
	r.i += n
	return v
}

func (r *reader) str() []byte {
	h := r.take(2)
	if h == nil {
		return nil
	}
	return r.take(int(binary.BigEndian.Uint16(h)))
}

func (r *reader) bin() []byte {
	h := r.take(4)
	if h == nil {
		return nil
	}
	n := int64(binary.BigEndian.Uint32(h))
	if n > int64(len(r.b)-r.i) { // compara antes de converter: u32 não cabe em int de 32 bits
		r.ok = false
		return nil
	}
	return r.take(int(n))
}

func (r *reader) msgID() []byte {
	v := r.str()
	if r.ok && (len(v) == 0 || len(v) > MaxMsgID) {
		r.ok = false
	}
	return v
}

// uuid: só a forma canônica minúscula, para o mesmo usuário ser sempre a mesma chave de mapa.
func (r *reader) uuid() []byte {
	v := r.str()
	if r.ok && !isUUID(v) {
		r.ok = false
	}
	return v
}

func isUUID(v []byte) bool {
	if len(v) != uuidLen {
		return false
	}
	for i, c := range v {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
				return false
			}
		}
	}
	return true
}

// Os Append* recebem campos já validados: tamanho acima do formato é bug de quem chama e dá panic.

func AppendAuth(dst []byte, token []byte) []byte { return appendStr(append(dst, Auth), token) }

func AppendEnvelope(dst, peer, msgID, payload []byte) []byte {
	return appendBin(appendStr(appendStr(append(dst, Envelope), peer), msgID), payload)
}

func AppendAck(dst, peer, msgID []byte) []byte {
	return appendStr(appendStr(append(dst, Ack), peer), msgID)
}

func AppendLocation(dst, payload []byte) []byte { return appendBin(append(dst, Location), payload) }

func AppendLocationUpdate(dst, from, payload []byte) []byte {
	return appendBin(appendStr(append(dst, LocationUpdate), from), payload)
}

// AppendLocationOff: from nil = o frame vazio do cliente.
func AppendLocationOff(dst, from []byte) []byte {
	dst = append(dst, LocationOff)
	if from == nil {
		return dst
	}
	return appendStr(dst, from)
}

func AppendError(dst []byte, code string) []byte {
	n := len(code)
	if n < 0 || n > math.MaxUint16 {
		panic("wire: str > 65535 bytes")
	}
	return append(binary.BigEndian.AppendUint16(append(dst, Error), uint16(n)), code...)
}

func appendStr(dst, v []byte) []byte {
	n := len(v)
	if n < 0 || n > math.MaxUint16 {
		panic("wire: str > 65535 bytes")
	}
	return append(binary.BigEndian.AppendUint16(dst, uint16(n)), v...)
}

func appendBin(dst, v []byte) []byte {
	n := len(v)
	if n < 0 || int64(n) > math.MaxUint32 {
		panic("wire: bin > 4 GiB")
	}
	return append(binary.BigEndian.AppendUint32(dst, uint32(n)), v...)
}
