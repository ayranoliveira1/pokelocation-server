package pogo

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

const DefaultUpstream = "https://coordinates-api.pokemongopro.com"

const userAgent = "pokelocation-server"

type client struct {
	base string
	hc   *http.Client
}

func newClient(base string) *client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = 32 // um host só: o padrão (2) fecharia conexões a cada rajada
	return &client{base: strings.TrimRight(base, "/"), hc: &http.Client{
		Transport: tr,
		// redirect nunca é esperado; segui-lo levaria o token para outro lugar
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

var errTooLarge = errors.New("pogo: resposta acima do teto")

type reply struct {
	status     int
	retryAfter string // header Retry-After
	gzip       bool   // buf ficou comprimido (só com gz e 2xx)
}

// do lê a resposta inteira em buf, até max bytes abertos. token "" = anônimo. Com gz, pede gzip e, num 2xx, deixa
// os bytes comprimidos em buf para repassar ao app; o resto chega aberto.
func (c *client) do(ctx context.Context, method, path, token string, body []byte, max int64, gz bool, buf *bytes.Buffer) (reply, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return reply{}, err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "application/json")
	if gz {
		req.Header.Set("Accept-Encoding", "gzip") // explícito: o Transport deixa de abrir sozinho
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("pogo: upstream", "err_type", fmt.Sprintf("%T", err)) // a mensagem traz a URL
		}
		return reply{}, err
	}
	defer resp.Body.Close()
	if resp.ContentLength > max {
		return reply{}, errTooLarge
	}
	if resp.ContentLength > 0 {
		buf.Grow(int(resp.ContentLength))
	}
	n, err := buf.ReadFrom(io.LimitReader(resp.Body, max+1))
	if err != nil {
		return reply{}, err
	}
	if n > max {
		return reply{}, errTooLarge
	}
	r := reply{status: resp.StatusCode, retryAfter: resp.Header.Get("Retry-After")}
	if gz && resp.Header.Get("Content-Encoding") == "gzip" {
		if r.status >= 200 && r.status < 300 {
			r.gzip = true
			err = gunzip(buf.Bytes(), max, io.Discard) // só confere o teto aberto
		} else { // o corpo do erro é lido aqui
			tmp := getBuf()
			if err = gunzip(buf.Bytes(), max, tmp); err == nil {
				buf.Reset()
				buf.Write(tmp.Bytes())
			}
			putBuf(tmp)
		}
	}
	return r, err
}

var zreaders sync.Pool

func gunzip(src []byte, max int64, dst io.Writer) error {
	zr, _ := zreaders.Get().(*gzip.Reader)
	var err error
	if zr == nil {
		zr, err = gzip.NewReader(bytes.NewReader(src))
	} else {
		err = zr.Reset(bytes.NewReader(src))
	}
	if err != nil {
		return err
	}
	defer zreaders.Put(zr)
	n, err := io.Copy(dst, io.LimitReader(zr, max+1))
	if err == nil && n > max {
		err = errTooLarge
	}
	return err
}

// UpstreamError: um 4xx do PoGo (exceto 401), repassado ao app como veio.
type UpstreamError struct {
	Status     int
	Detail     json.RawMessage
	RetryAfter int // segundos; 0 = não veio
}

func (e *UpstreamError) Error() string { return "pogo: upstream " + strconv.Itoa(e.Status) }

// code: o detail.error do PoGo ("trainer_not_linked"…); "" se o detail não é um objeto.
func (e *UpstreamError) code() string {
	var d struct {
		Error string `json:"error"`
	}
	if len(e.Detail) == 0 || e.Detail[0] != '{' || json.Unmarshal(e.Detail, &d) != nil {
		return ""
	}
	return d.Error
}

var errUnauthorized = errors.New("pogo: upstream 401")

// classify: nil no 2xx, errUnauthorized no 401, *UpstreamError nos outros 4xx, ErrUnavailable no resto.
func classify(status int, retryAfter string, body []byte) error {
	switch {
	case status >= 200 && status < 300:
		return nil
	case status == http.StatusUnauthorized:
		return errUnauthorized
	case status < 400 || status >= 500:
		return ErrUnavailable
	}
	var e struct {
		Detail json.RawMessage `json:"detail"`
	}
	_ = json.Unmarshal(body, &e) // 4xx sem JSON (nginx) = detail null
	ue := &UpstreamError{Status: status, Detail: e.Detail}
	var d struct {
		RetryAfter float64 `json:"retry_after"`
	}
	if len(e.Detail) > 0 && e.Detail[0] == '{' && json.Unmarshal(e.Detail, &d) == nil && d.RetryAfter > 0 {
		ue.RetryAfter = int(math.Ceil(min(d.RetryAfter, maxRetryAfter)))
	} else if s, err := strconv.Atoi(strings.TrimSpace(retryAfter)); err == nil && s > 0 {
		ue.RetryAfter = min(s, maxRetryAfter)
	}
	return ue
}

const maxRetryAfter = 24 * 3600

// Buffers das respostas: o lookup chega a centenas de KiB por chamada. O GC esvazia o pool, então guardar até o
// maior teto não retém memória para sempre.
var bufs = sync.Pool{New: func() any { return new(bytes.Buffer) }}

func getBuf() *bytes.Buffer {
	b := bufs.Get().(*bytes.Buffer)
	b.Reset()
	return b
}

func putBuf(b *bytes.Buffer) {
	if b.Cap() <= maxPooledBuf {
		bufs.Put(b)
	}
}

const maxPooledBuf = 1 << 20
