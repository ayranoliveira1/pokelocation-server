package auth

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/argon2"
	"google.golang.org/api/idtoken"

	"github.com/ayranoliveira1/pokelocation-server/internal/groups"
)

const (
	accessTTL  = 15 * time.Minute
	refreshTTL = 30 * 24 * time.Hour
	stateTTL   = 10 * time.Minute // o usuário está no navegador, escolhendo a conta
	codeTTL    = 60 * time.Second

	// argon2id com os parâmetros da OWASP.
	argonM, argonT, argonP, argonLen = 19 * 1024, 2, 1, 32
	argonWait                        = 2 * time.Second

	googleAuthURL  = "https://accounts.google.com/o/oauth2/v2/auth"
	googleTokenURL = "https://oauth2.googleapis.com/token" // #nosec G101 -- URL pública do OAuth do Google
)

// apiError: falha esperada, respondida como {"error": code}.
type apiError struct {
	status int
	code   string
}

func (e *apiError) Error() string { return e.code }

var (
	errBadRequest   = &apiError{400, "bad_request"}
	errInvalidEmail = &apiError{400, "invalid_email"}
	errWeakPassword = &apiError{400, "weak_password"}
	errEmailTaken   = &apiError{409, "email_taken"}
	errInvalidCreds = &apiError{401, "invalid_credentials"}
	errUnauthorized = &apiError{401, "unauthorized"}
	errInvalidGrant = &apiError{400, "invalid_grant"}
	errBadState     = errors.New("auth: state desconhecido")
	errArgonBusy    = errors.New("auth: argon2 sem vaga")

	ErrUnauthorized error = errUnauthorized
)

// claims: tv = users.token_version na emissão. Revogar incrementa a versão, e todo access com tv antigo morre na hora
// (sem janela de relógio).
type claims struct {
	jwt.RegisteredClaims
	TV *int `json:"tv,omitempty"`
}

type tokens struct {
	Access    string `json:"access_token"`
	Refresh   string `json:"refresh_token"`
	ExpiresIn int    `json:"expires_in"`
}

type Service struct {
	key    ed25519.PrivateKey
	now    func() time.Time
	store  *Store
	rdb    *redis.Client
	cfg    Config
	dummy  string        // login sem conta compara com este hash: o tempo de resposta não revela se o email existe
	argon  chan struct{} // 19 MiB por hash: o teto de hashes simultâneos impede uma rajada de logins de estourar a RAM
	notify func(user string, exFriends []string, groupChanges []groups.Change)
	// google é campo para o teste trocar a ida ao Google.
	google func(ctx context.Context, code string) (sub, email string, err error)
}

func normEmail(raw string) (string, bool) {
	e := strings.ToLower(strings.TrimSpace(raw))
	local, domain, _ := strings.Cut(e, "@")
	return e, len(e) >= 3 && len(e) <= 254 && local != "" && domain != "" && !strings.Contains(domain, "@")
}

func (s *Service) register(ctx context.Context, email, password string) (tokens, error) {
	e, ok := normEmail(email)
	if !ok {
		return tokens{}, errInvalidEmail
	}
	if n := utf8.RuneCountInString(password); n < 8 || n > 128 {
		return tokens{}, errWeakPassword
	}
	hash, err := s.hashPassword(ctx, password)
	if err != nil {
		return tokens{}, err
	}
	id, err := s.store.createUser(ctx, &e, &hash, nil)
	if err != nil {
		return tokens{}, err
	}
	return s.issue(ctx, id)
}

// login: sem conta, conta só-Google e senha errada dão o mesmo erro no mesmo tempo.
func (s *Service) login(ctx context.Context, email, password string) (tokens, error) {
	e, _ := normEmail(email)
	id, hash, _, err := s.store.userByEmail(ctx, e)
	if err != nil {
		return tokens{}, err
	}
	phc := s.dummy
	if hash != nil {
		phc = *hash
	}
	ok, err := s.checkPassword(ctx, phc, password)
	if err != nil {
		return tokens{}, err
	}
	if !ok || hash == nil {
		return tokens{}, errInvalidCreds
	}
	return s.issue(ctx, id)
}

func (s *Service) refresh(ctx context.Context, token string) (tokens, error) {
	next := randomToken()
	now := s.now()
	id, tv, revoked, err := s.store.rotate(ctx, sha(token), sha(next), now, now.Add(refreshTTL))
	if err != nil {
		return tokens{}, err
	}
	if revoked != "" {
		s.notify(revoked, nil, nil)
	}
	if id == "" {
		return tokens{}, errUnauthorized
	}
	return s.withAccess(id, tv, next)
}

func (s *Service) logout(ctx context.Context, token string) error {
	user, err := s.store.revoke(ctx, sha(token))
	if user != "" {
		s.notify(user, nil, nil)
	}
	return err
}

func (s *Service) issue(ctx context.Context, userID string) (tokens, error) {
	r := randomToken()
	tv, err := s.store.newRefresh(ctx, userID, sha(r), s.now().Add(refreshTTL))
	if err != nil {
		return tokens{}, err
	}
	return s.withAccess(userID, tv, r)
}

func (s *Service) withAccess(userID string, tv int, refresh string) (tokens, error) {
	a, err := s.accessToken(userID, tv)
	return tokens{Access: a, Refresh: refresh, ExpiresIn: int(accessTTL / time.Second)}, err
}

func (s *Service) accessToken(userID string, tv int) (string, error) {
	now := s.now()
	return jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims{jwt.RegisteredClaims{
		Subject:   userID,
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(accessTTL)),
	}, &tv}).SignedString(s.key)
}

// parse não lê o banco: a revogação (tv ≠ token_version) fica para quem chama.
func (s *Service) parse(token string) (string, time.Time, int, error) {
	var c claims
	_, err := jwt.ParseWithClaims(token, &c, func(*jwt.Token) (any, error) { return s.key.Public(), nil },
		jwt.WithValidMethods([]string{"EdDSA"}), jwt.WithExpirationRequired(), jwt.WithTimeFunc(s.now))
	if err != nil || c.Subject == "" || c.TV == nil {
		return "", time.Time{}, 0, errUnauthorized
	}
	return c.Subject, c.ExpiresAt.Time, *c.TV, nil
}

func (s *Service) verify(ctx context.Context, token string) (string, time.Time, error) {
	id, exp, tv, err := s.parse(token)
	if err != nil {
		return "", time.Time{}, err
	}
	v, err := s.store.version(ctx, id)
	if errors.Is(err, errNotFound) || err == nil && v != tv {
		return "", time.Time{}, errUnauthorized
	}
	if err != nil {
		return "", time.Time{}, err
	}
	return id, exp, nil
}

var b64 = base64.RawStdEncoding

func (s *Service) hashPassword(ctx context.Context, pw string) (string, error) {
	salt := make([]byte, 16)
	_, _ = rand.Read(salt)
	k, err := s.argon2(ctx, pw, salt, argonM, argonT, argonP, argonLen)
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s", argon2.Version, argonM, argonT, argonP,
		b64.EncodeToString(salt), b64.EncodeToString(k)), err
}

// checkPassword lê os parâmetros do próprio hash: trocar os parâmetros não invalida as senhas antigas.
func (s *Service) checkPassword(ctx context.Context, phc, pw string) (bool, error) {
	f := strings.Split(phc, "$")
	if len(f) != 6 || f[1] != "argon2id" || f[2] != fmt.Sprintf("v=%d", argon2.Version) {
		return false, nil
	}
	var m, t uint32
	var p uint8
	if _, err := fmt.Sscanf(f[3], "m=%d,t=%d,p=%d", &m, &t, &p); err != nil {
		return false, nil
	}
	salt, err1 := b64.DecodeString(f[4])
	want, err2 := b64.DecodeString(f[5])
	if err1 != nil || err2 != nil || len(want) != argonLen {
		return false, nil
	}
	got, err := s.argon2(ctx, pw, salt, m, t, p, argonLen)
	return err == nil && subtle.ConstantTimeCompare(got, want) == 1, err
}

func (s *Service) argon2(ctx context.Context, pw string, salt []byte, m, t uint32, p uint8, n uint32) ([]byte, error) {
	wait := time.NewTimer(argonWait)
	defer wait.Stop()
	select {
	case s.argon <- struct{}{}:
	case <-wait.C:
		return nil, errArgonBusy
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-s.argon }()
	return argon2.IDKey([]byte(pw), salt, t, m, p, n), nil
}

func validB64URL(s string) bool {
	for _, c := range []byte(s) {
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

func (s *Service) redirectURI() string {
	return strings.TrimRight(s.cfg.PublicURL, "/") + "/v1/auth/google/callback"
}

// googleStart guarda o challenge e o state do app sob um state nosso: o do app nunca vai ao Google.
func (s *Service) googleStart(ctx context.Context, challenge, method, appState string) (string, error) {
	if len(challenge) != 43 || !validB64URL(challenge) || method != "S256" ||
		len(appState) < 16 || len(appState) > 128 || !validB64URL(appState) {
		return "", errBadRequest
	}
	state := randomToken()
	if err := s.rdb.Set(ctx, "gstate:"+state, challenge+appState, stateTTL).Err(); err != nil {
		return "", err
	}
	return googleAuthURL + "?" + url.Values{
		"client_id":     {s.cfg.GoogleClientID},
		"redirect_uri":  {s.redirectURI()},
		"response_type": {"code"},
		"scope":         {"openid email"},
		"state":         {state},
	}.Encode(), nil
}

func (s *Service) takeState(ctx context.Context, state string) (challenge, appState string, err error) {
	v, err := s.rdb.GetDel(ctx, "gstate:"+state).Result()
	if errors.Is(err, redis.Nil) || err == nil && len(v) < 43 {
		return "", "", errBadState
	}
	if err != nil {
		return "", "", err
	}
	return v[:43], v[43:], nil
}

func (s *Service) googleLogin(ctx context.Context, challenge, googleCode string) (string, error) {
	sub, raw, err := s.google(ctx, googleCode)
	if err != nil {
		return "", googleError{err}
	}
	email, ok := normEmail(raw)
	if !ok || sub == "" {
		return "", googleError{errors.New("identidade inválida")}
	}
	id, err := s.googleUser(ctx, sub, email)
	if err != nil {
		return "", err
	}
	code := randomToken()
	return code, s.rdb.Set(ctx, "gcode:"+code, challenge+id, codeTTL).Err()
}

// googleUser vincula pelo email verificado só uma conta sem sub, e a entrega limpa: o cadastro por senha não verifica
// email, então quem cadastrou o email de outro perde o acesso quando o dono entra (pre-account hijacking).
func (s *Service) googleUser(ctx context.Context, sub, email string) (string, error) {
	id, err := s.store.userBySub(ctx, sub)
	if err != nil || id != "" {
		return id, err
	}
	id, _, linked, err := s.store.userByEmail(ctx, email)
	if err != nil {
		return "", err
	}
	if id == "" {
		return s.store.createUser(ctx, &email, nil, &sub)
	}
	if linked != nil {
		return "", googleError{errors.New("email já vinculado a outra conta Google")}
	}
	ok, ex, changes, err := s.store.linkGoogle(ctx, id, sub)
	if err == nil && !ok {
		err = googleError{errors.New("vínculo concorrente")}
	}
	if ok {
		s.notify(id, ex, changes)
	}
	return id, err
}

func (s *Service) googleExchange(ctx context.Context, code, verifier string) (tokens, error) {
	if code == "" || verifier == "" {
		return tokens{}, errBadRequest
	}
	v, err := s.rdb.GetDel(ctx, "gcode:"+code).Result()
	if errors.Is(err, redis.Nil) {
		return tokens{}, errInvalidGrant
	}
	if err != nil {
		return tokens{}, err
	}
	sum := sha256.Sum256([]byte(verifier))
	if len(v) <= 43 || subtle.ConstantTimeCompare([]byte(base64.RawURLEncoding.EncodeToString(sum[:])), []byte(v[:43])) != 1 {
		return tokens{}, errInvalidGrant
	}
	return s.issue(ctx, v[43:])
}

var googleHTTP = &http.Client{Timeout: 10 * time.Second}

func (s *Service) googleIdentity(ctx context.Context, code string) (string, string, error) {
	form := url.Values{
		"code":          {code},
		"client_id":     {s.cfg.GoogleClientID},
		"client_secret": {s.cfg.GoogleClientSecret},
		"redirect_uri":  {s.redirectURI()},
		"grant_type":    {"authorization_code"},
	}
	req, err := http.NewRequestWithContext(ctx, "POST", googleTokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	res, err := googleHTTP.Do(req)
	if err != nil {
		return "", "", err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("auth: google token %d", res.StatusCode)
	}
	var out struct {
		IDToken string `json:"id_token"`
	}
	if err := json.NewDecoder(io.LimitReader(res.Body, 64<<10)).Decode(&out); err != nil {
		return "", "", errors.New("auth: resposta do google ilegível")
	}
	p, err := idtoken.Validate(ctx, out.IDToken, s.cfg.GoogleClientID)
	if err != nil {
		return "", "", err
	}
	email, _ := p.Claims["email"].(string)
	if verified, _ := p.Claims["email_verified"].(bool); !verified {
		return "", "", errors.New("auth: google sem email verificado")
	}
	return p.Subject, email, nil
}

// googleError: texto nosso ou da lib, sem dado do usuário: pode ir inteiro para o log.
type googleError struct{ err error }

func (e googleError) Error() string { return "google: " + e.err.Error() }
func (e googleError) Unwrap() error { return e.err }

func randomToken() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

func sha(s string) []byte {
	h := sha256.Sum256([]byte(s))
	return h[:]
}
