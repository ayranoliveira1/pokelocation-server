// Package pogo: proxy do PoGo Coordinates com o token de cada usuário, guardado cifrado (contrato em
// pokelocation/docs/pogo/api-servidor.md).
package pogo

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	TokenKey string // 32 bytes em base64 padrão; vazio = integração desligada
	Upstream string // vazio = DefaultUpstream
	Workers  string // POGO_WORKERS: alertas em paralelo por instância, 1–64; vazio = 8
}

var (
	ErrRelogin      = errors.New("pogo_relogin")
	ErrInvalidGrant = errors.New("invalid_grant")
	ErrUnavailable  = errors.New("upstream_unavailable")
)

const (
	maxToken   = 8 << 10
	maxProfile = 64 << 10
	dbWait     = 5 * time.Second
)

type Service struct {
	store *Store
	c     *client
	aead  cipher.AEAD
}

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
	return &Service{store: &Store{pool}, c: newClient(cfg.Upstream), aead: aead}, nil
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
		return nil, ErrUnavailable
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
		return nil, ErrUnavailable
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
		return false, ErrUnavailable
	}
	if err := classify(rep.status, rep.retryAfter, buf.Bytes()); err != nil {
		if errors.Is(err, errUnauthorized) {
			return false, s.drop(ctx, user, enc)
		}
		return false, err
	}
	return rep.gzip, nil
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
