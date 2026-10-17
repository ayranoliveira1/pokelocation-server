// Package pogo: proxy do PoGo Coordinates com o token de cada usuário, guardado cifrado (contrato em
// pokelocation/docs/pogo/api-servidor.md).
package pogo

import (
	"bytes"
	"cmp"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	TokenKey string // 32 bytes em base64 padrão; vazio = integração desligada
	Upstream string // vazio = DefaultUpstream
	Workers  string // POGO_WORKERS: alertas em paralelo por instância, 1–64; vazio = 32
	// POGO_ALERT_INTERVAL: de quanto em quanto tempo cada usuário roda (2m–10m); vazio = 3m
	AlertInterval string
	// POGO_SHARED_TOKENS: tokens do servidor separados por vírgula, em rodízio no que não depende da conta do usuário;
	// vazio = tudo com o token do usuário
	SharedTokens string
}

var (
	ErrRelogin      = errors.New("pogo_relogin")
	ErrInvalidGrant = errors.New("invalid_grant")
	ErrUnavailable  = errors.New("upstream_unavailable")
	ErrTimeout      = errors.New("upstream_timeout")
	// todos os tokens do servidor recusados (401, 402, 403): o detail é da conta do servidor, não do usuário
	errSharedRefused = fmt.Errorf("%w: tokens do servidor recusados", ErrUnavailable)
)

const (
	maxToken   = 8 << 10
	maxProfile = 64 << 10
	dbWait     = 5 * time.Second
)

type Service struct {
	store  *Store
	c      *client
	aead   cipher.AEAD
	shared []sharedToken
	next   atomic.Uint32
}

// sharedToken: um token de POGO_SHARED_TOKENS. Recusado ou no limite fica fora do rodízio por um tempo, senão cada
// usuário da fila pagaria uma ida ao PoGo nele (e no 429 prolongaria o limite, que é do token e não do usuário).
type sharedToken struct {
	token   string
	until   atomic.Int64 // unix ms
	limited atomic.Bool  // until veio de um 429
}

const (
	refusedCool = 5 * time.Minute
	limitedCool = time.Minute // 429 sem retry_after
)

func newService(cfg Config, pool *pgxpool.Pool) (*Service, error) {
	key, err := base64.StdEncoding.DecodeString(cfg.TokenKey)
	if err != nil || len(key) != 32 {
		return nil, errors.New("pogo: POGO_TOKEN_KEY deve ter 32 bytes em base64")
	}
	block, _ := aes.NewCipher(key) // 32 bytes nunca falha
	aead, _ := cipher.NewGCM(block)
	if cfg.Upstream == "" {
		cfg.Upstream = DefaultUpstream
	}
	var toks []string
	for t := range strings.SplitSeq(cfg.SharedTokens, ",") {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		if len(t) > maxToken || strings.ContainsFunc(t, func(r rune) bool { return r <= ' ' || r >= 0x7f }) {
			return nil, errors.New("pogo: POGO_SHARED_TOKENS tem um token inválido")
		}
		toks = append(toks, t)
	}
	shared := make([]sharedToken, len(toks))
	for i, t := range toks {
		shared[i].token = t
	}
	return &Service{store: &Store{pool}, c: newClient(cfg.Upstream), aead: aead, shared: shared}, nil
}

// O user_id como AAD: um token copiado para a linha de outro usuário não abre.
func (s *Service) seal(user, token string) []byte {
	nonce := make([]byte, s.aead.NonceSize(), s.aead.NonceSize()+len(token)+s.aead.Overhead())
	_, _ = rand.Read(nonce)
	return s.aead.Seal(nonce, nonce, []byte(token), []byte(user))
}

func (s *Service) open(user string, enc []byte) (string, error) {
	n := s.aead.NonceSize()
	if len(enc) < n {
		return "", errors.New("pogo: token cifrado curto")
	}
	b, err := s.aead.Open(nil, enc[:n], enc[n:], []byte(user))
	return string(b), err
}

// Profile: o que sai do /users/me deles para o app; o resto (email, ids, assinatura) fica no servidor.
type Profile struct {
	Provider    string            `json:"provider"`
	DisplayName *string           `json:"display_name"`
	Trainers    []json.RawMessage `json:"trainers"`
	TrainerCap  int               `json:"trainer_cap"`
	VipTier     *string           `json:"vip_tier"`
}

func parseProfile(provider string, body []byte) (*Profile, error) {
	var me struct {
		DisplayName     *string           `json:"display_name"`
		DiscordUsername *string           `json:"discord_username"`
		Trainers        []json.RawMessage `json:"trainers"`
		Trainer         json.RawMessage   `json:"trainer"`
		TrainerCap      int               `json:"trainer_cap"`
		VipTier         *string           `json:"vip_tier"`
	}
	if err := json.Unmarshal(body, &me); err != nil {
		return nil, ErrUnavailable
	}
	p := &Profile{Provider: provider, DisplayName: me.DisplayName, TrainerCap: me.TrainerCap, VipTier: me.VipTier}
	if p.DisplayName == nil || *p.DisplayName == "" {
		p.DisplayName = me.DiscordUsername
	}
	ts := me.Trainers
	if ts == nil && len(me.Trainer) > 0 && string(me.Trainer) != "null" {
		ts = []json.RawMessage{me.Trainer}
	}
	p.Trainers = activeTrainers(ts)
	return p, nil
}

// activeTrainers: como o site, só os com active != false; nunca nil.
func activeTrainers(ts []json.RawMessage) []json.RawMessage {
	out := []json.RawMessage{}
	for _, t := range ts {
		var a struct {
			Active *bool `json:"active"`
		}
		if json.Unmarshal(t, &a) == nil && (a.Active == nil || *a.Active) {
			out = append(out, t)
		}
	}
	return out
}

// Login troca o code no PoGo, confere o token com /users/me e grava o vínculo (substitui o anterior).
func (s *Service) Login(ctx context.Context, user, provider, code string) (*Profile, error) {
	buf := getBuf()
	defer putBuf(buf)
	body, _ := json.Marshal(map[string]string{"code": code})
	rep, err := s.c.do(ctx, "POST", "/auth/"+provider+"/callback", "", body, maxProfile, false, buf)
	if err != nil {
		return nil, upErr(err)
	}
	if err := classify(rep.status, rep.retryAfter, buf.Bytes()); err != nil {
		if rep.status == 400 || errors.Is(err, errUnauthorized) {
			return nil, ErrInvalidGrant
		}
		return nil, err
	}
	var out struct {
		Token string `json:"token"`
	}
	if json.Unmarshal(buf.Bytes(), &out) != nil || out.Token == "" || len(out.Token) > maxToken {
		return nil, ErrUnavailable
	}
	buf.Reset()
	rep, err = s.c.do(ctx, "GET", "/users/me", out.Token, nil, maxProfile, false, buf)
	if err != nil {
		return nil, upErr(err)
	}
	if err := classify(rep.status, rep.retryAfter, buf.Bytes()); err != nil {
		if errors.Is(err, errUnauthorized) { // token recém-emitido recusado: problema deles
			return nil, ErrUnavailable
		}
		return nil, err
	}
	p, err := parseProfile(provider, buf.Bytes())
	if err != nil {
		return nil, err
	}
	// o code já foi gasto: se o app desistir agora, o token ainda tem de ficar gravado
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dbWait)
	defer cancel()
	return p, s.store.upsert(dctx, user, provider, s.seal(user, out.Token), p)
}

// Me relê o /users/me e atualiza o cache do vínculo.
func (s *Service) Me(ctx context.Context, user string) (*Profile, error) {
	buf := getBuf()
	defer putBuf(buf)
	a, _, err := s.call(ctx, user, "GET", "/users/me", nil, maxProfile, false, buf)
	if err != nil {
		return nil, err
	}
	p, err := parseProfile(a.provider, buf.Bytes())
	if err != nil {
		return nil, err
	}
	return p, s.store.updateProfile(ctx, user, a.enc, p)
}

func (s *Service) Unlink(ctx context.Context, user string) error {
	return s.store.deleteAccount(ctx, user)
}

// call faz o pedido com o token do usuário e deixa a resposta (2xx) em buf, comprimida se gzipped. 401 do PoGo, ou
// token que não abre (POGO_TOKEN_KEY trocada), apaga o vínculo e vira ErrRelogin.
func (s *Service) call(ctx context.Context, user, method, path string, body []byte, max int64, gz bool, buf *bytes.Buffer) (a account, gzipped bool, err error) {
	a, err = s.store.account(ctx, user)
	if errors.Is(err, errNoAccount) {
		return a, false, ErrRelogin
	}
	if err != nil {
		return a, false, err
	}
	token, err := s.open(user, a.enc)
	if err != nil {
		return a, false, s.drop(ctx, user, a.enc)
	}
	gzipped, err = s.callToken(ctx, user, a.enc, token, method, path, body, max, gz, buf)
	return a, gzipped, err
}

func (s *Service) callToken(ctx context.Context, user string, enc []byte, token, method, path string, body []byte, max int64, gz bool, buf *bytes.Buffer) (bool, error) {
	rep, err := s.c.do(ctx, method, path, token, body, max, gz, buf)
	if err != nil {
		return false, upErr(err)
	}
	if err := classify(rep.status, rep.retryAfter, buf.Bytes()); err != nil {
		if errors.Is(err, errUnauthorized) {
			return false, s.drop(ctx, user, enc)
		}
		return false, err
	}
	return rep.gzip, nil
}

// search: lookup, search e reveal. Com POGO_SHARED_TOKENS vão pelos tokens do servidor, sem exigir vínculo; filtro
// shiny só vale com o treinador do dono do token, então esse segue com o token do usuário. Servidor todo recusado e
// usuário vinculado: tenta com o token dele.
func (s *Service) search(ctx context.Context, user, method, path string, body []byte, max int64, gz bool, buf *bytes.Buffer) (bool, error) {
	if len(s.shared) > 0 && !wantsShiny(body) {
		gzipped, err := s.callShared(ctx, method, path, body, max, gz, buf)
		if err != errSharedRefused {
			return gzipped, err
		}
		buf.Reset()
		a, gzipped, err := s.call(ctx, user, method, path, body, max, gz, buf)
		if errors.Is(err, ErrRelogin) && a.enc == nil { // sem vínculo: nada a relogar, o erro é do servidor
			return false, errSharedRefused
		}
		return gzipped, err
	}
	_, gzipped, err := s.call(ctx, user, method, path, body, max, gz, buf)
	return gzipped, err
}

// callShared roda os tokens do servidor a partir do próximo da vez, pulando os que esfriam; 401, 402 ou 403 (conta do
// servidor sem acesso) e 429 passam para o seguinte; o 403 de filtro (vale para qualquer token) volta na hora. Nenhum
// serviu: 429 com o menor tempo até um token voltar, se algum está no limite, senão errSharedRefused.
func (s *Service) callShared(ctx context.Context, method, path string, body []byte, max int64, gz bool, buf *bytes.Buffer) (bool, error) {
	n := len(s.shared)
	first := int(s.next.Add(1) - 1)
	var wait time.Duration // menor espera de um token no limite
	for i := range n {
		k := (first + i) % n
		t := &s.shared[k]
		if d := time.Until(time.UnixMilli(t.until.Load())); d > 0 {
			if t.limited.Load() && (wait == 0 || d < wait) {
				wait = d
			}
			continue
		}
		buf.Reset()
		rep, err := s.c.do(ctx, method, path, t.token, body, max, gz, buf)
		if err != nil {
			return false, upErr(err)
		}
		err = classify(rep.status, rep.retryAfter, buf.Bytes())
		var ue *UpstreamError
		switch {
		case err == nil:
			return rep.gzip, nil
		case errors.Is(err, errUnauthorized) || errors.As(err, &ue) && (ue.Status == 402 || ue.Status == 403 && !filterDenied(ue)):
			// a posição em POGO_SHARED_TOKENS, nunca o token
			slog.Warn("pogo: token do servidor recusado", "pos", k, "status", rep.status)
			t.cool(refusedCool, false)
		case ue != nil && ue.Status == 429:
			d := cmp.Or(time.Duration(ue.RetryAfter)*time.Second, limitedCool)
			t.cool(d, true)
			if wait == 0 || d < wait {
				wait = d
			}
		default:
			return false, err
		}
	}
	if wait > 0 {
		sec := int(math.Ceil(wait.Seconds()))
		return false, &UpstreamError{Status: 429, RetryAfter: sec,
			Detail: fmt.Appendf(nil, `{"error":"rate_limited","retry_after":%d}`, sec)}
	}
	return false, errSharedRefused
}

// filterDenied: 403 pelo que o pedido pede (spec do PoGo), não pela conta do token.
func filterDenied(ue *UpstreamError) bool {
	switch ue.code() {
	case "trainer_not_linked", "shiny_account_unavailable", "vip_required":
		return true
	}
	return false
}

func (t *sharedToken) cool(d time.Duration, limited bool) {
	t.limited.Store(limited)
	t.until.Store(time.Now().Add(d).UnixMilli())
}

// shinyFilters: os filtros que dependem do treinador do dono do token.
type shinyFilters struct {
	ShinyOnly      bool   `json:"shiny_only"`
	BackgroundOnly bool   `json:"background_only"`
	ShinyAccount   string `json:"shiny_account"`
}

func (f *shinyFilters) any() bool { return f.ShinyOnly || f.BackgroundOnly || f.ShinyAccount != "" }

// wantsShiny: no /search os filtros vêm no topo, no /map/lookup dentro de filters. Corpo que não lê como esperado
// conta como shiny: vai com o token do usuário.
func wantsShiny(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	var b struct {
		shinyFilters
		Filters *shinyFilters `json:"filters"`
	}
	return json.Unmarshal(body, &b) != nil || b.any() || b.Filters != nil && b.Filters.any()
}

// upErr: prazo estourado vira ErrTimeout (504); conexão, TLS ou resposta grande demais, ErrUnavailable (502).
func upErr(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrTimeout
	}
	return ErrUnavailable
}

// drop roda mesmo com o pedido cancelado: o token já se provou inválido.
func (s *Service) drop(ctx context.Context, user string, enc []byte) error {
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dbWait)
	defer cancel()
	if err := s.store.deleteToken(dctx, user, enc); err != nil {
		return err
	}
	return ErrRelogin
}
