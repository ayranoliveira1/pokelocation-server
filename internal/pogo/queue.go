package pogo

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Fila dos alertas: um membro por usuário no ZSET dueKey, com score = próxima execução (unix ms). Quem reivindica
// grava um token em leaseKey (fencing): agendar de novo e marcar como visto só valem com o token ainda igual.
const dueKey = "pogo:due"

func leaseKey(user string) string { return "pogo:lease:" + user }

// KEYS: due. ARGV: agora, lease ms, n, prefixo do token. Devolve trios {user, token, score}; sem nenhum vencido,
// {"", "", score do mais antigo ou "" se a fila está vazia}.
var claimScript = redis.NewScript(`
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'WITHSCORES', 'LIMIT', 0, tonumber(ARGV[3]))
local out = {}
for i = 1, #due, 2 do
	local tok = ARGV[4] .. ':' .. i
	redis.call('SET', 'pogo:lease:' .. due[i], tok, 'PX', ARGV[2])
	redis.call('ZADD', KEYS[1], 'XX', tonumber(ARGV[1]) + tonumber(ARGV[2]), due[i])
	out[#out + 1] = due[i]
	out[#out + 1] = tok
	out[#out + 1] = due[i + 1]
end
if #out == 0 then
	local first = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
	return {'', '', first[2] or ''}
end
return out`)

// KEYS: due, lease. ARGV: user, token, próximo score ("" = sai da fila). 0 = o lease já não era deste worker.
var finishScript = redis.NewScript(`
if redis.call('GET', KEYS[2]) ~= ARGV[2] then return 0 end
redis.call('DEL', KEYS[2])
if ARGV[3] == '' then redis.call('ZREM', KEYS[1], ARGV[1]) else redis.call('ZADD', KEYS[1], 'XX', ARGV[3], ARGV[1]) end
return 1`)

// KEYS: due, lease. ARGV: user, agora. Fora da fila: entra vencido. Em processamento: o lease cai e o usuário volta
// vencido, para o worker atual não tirá-lo da fila com dados velhos (ex.: "sem regras" logo antes de uma regra nova).
var enqueueScript = redis.NewScript(`
if not redis.call('ZSCORE', KEYS[1], ARGV[1]) or redis.call('DEL', KEYS[2]) == 1 then
	redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
end
return 1`)

// Dedup: um ZSET por usuário, membro = encounter_id e score = quando o aviso pode repetir (ms no relógio do worker).
// Até 128 membros fica em listpack, ~22 B por item contra ~150 B de uma chave por item.
//
// KEYS: lease, seen. ARGV: token, limite por regra, agora ms, TTL da chave ms, e um texto por regra com pares
// "encounter ttl_ms" separados por espaço. Marca até o limite de cada regra e devolve as posições (1..) marcadas; nil
// se o lease já não é deste worker.
var seenScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return false end
local limit, now = tonumber(ARGV[2]), tonumber(ARGV[3])
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
local out, pos = {}, 0
for g = 5, #ARGV do
	local got = 0
	for id, ttl in string.gmatch(ARGV[g], '(%S+) (%d+)') do
		pos = pos + 1
		if got < limit and not redis.call('ZSCORE', KEYS[2], id) then
			redis.call('ZADD', KEYS[2], now + tonumber(ttl), id)
			got = got + 1
			out[#out + 1] = pos
		end
	end
end
if #out > 0 then redis.call('PEXPIRE', KEYS[2], ARGV[4]) end
return out`)

func seenKey(user string) string { return "pogo:seen:" + user }

type queue struct {
	rdb  *redis.Client
	wake chan struct{} // ZADD local: o dispatcher acorda sem esperar o próximo sono
}

func newQueue(rdb *redis.Client) *queue { return &queue{rdb: rdb, wake: make(chan struct{}, 1)} }

func (q *queue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (q *queue) enqueue(ctx context.Context, user string, now time.Time) error {
	err := enqueueScript.Run(ctx, q.rdb, []string{dueKey, leaseKey(user)}, user, now.UnixMilli()).Err()
	q.signal()
	return err
}

func (q *queue) remove(ctx context.Context, user string) error {
	_, err := q.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.ZRem(ctx, dueKey, user)
		p.Del(ctx, leaseKey(user))
		return nil
	})
	return err
}

type claim struct {
	user, token string
	due         time.Time
}

// claim: next zero = fila vazia.
func (q *queue) claim(ctx context.Context, now time.Time, lease time.Duration, n int) ([]claim, time.Time, error) {
	var b [8]byte
	_, _ = rand.Read(b[:])
	res, err := claimScript.Run(ctx, q.rdb, []string{dueKey}, now.UnixMilli(), lease.Milliseconds(), n,
		hex.EncodeToString(b[:])).StringSlice()
	if err != nil {
		return nil, time.Time{}, err
	}
	if len(res) == 3 && res[0] == "" {
		return nil, scoreTime(res[2]), nil
	}
	cs := make([]claim, 0, len(res)/3)
	for i := 0; i+2 < len(res); i += 3 {
		cs = append(cs, claim{user: res[i], token: res[i+1], due: scoreTime(res[i+2])})
	}
	return cs, time.Time{}, nil
}

func scoreTime(s string) time.Time {
	ms, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return time.Time{}
	}
	return time.UnixMilli(int64(ms))
}

// finish: next zero = sai da fila. false = o lease venceu e outro worker já tem o usuário.
func (q *queue) finish(ctx context.Context, c claim, next time.Time) (bool, error) {
	arg := ""
	if !next.IsZero() {
		arg = strconv.FormatInt(next.UnixMilli(), 10)
	}
	n, err := finishScript.Run(ctx, q.rdb, []string{dueKey, leaseKey(c.user)}, c.user, c.token, arg).Int()
	return n == 1, err
}

var errFenced = errors.New("pogo: lease perdido")

// markSeen: uma ida ao Redis para todas as regras do usuário; groups[i] é o texto da regra i (ver seenScript).
// Devolve as posições (0..) marcadas, contando os pares de todas as regras em ordem.
func (q *queue) markSeen(ctx context.Context, c claim, limit int, now time.Time, groups []string) ([]int, error) {
	args := make([]any, 0, 4+len(groups))
	args = append(args, c.token, limit, now.UnixMilli(), maxSeen.Milliseconds())
	for _, g := range groups {
		args = append(args, g)
	}
	res, err := seenScript.Run(ctx, q.rdb, []string{leaseKey(c.user), seenKey(c.user)}, args...).Int64Slice()
	if errors.Is(err, redis.Nil) {
		return nil, errFenced
	}
	if err != nil {
		return nil, err
	}
	out := make([]int, len(res))
	for i, p := range res {
		out[i] = int(p) - 1
	}
	return out, nil
}
