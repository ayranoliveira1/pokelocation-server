// Package username: regras do @nome (docs/api.md §8).
package username

import (
	"errors"
	"strings"
)

var (
	ErrInvalid  = errors.New("invalid_username")
	ErrReserved = errors.New("username_reserved")
)

var reserved = map[string]bool{
	"admin": true, "administrador": true, "pokelocation": true, "suporte": true, "support": true, "root": true,
	"null": true, "moderador": true, "moderator": true, "staff": true, "oficial": true, "official": true,
	"sistema": true, "system": true,
}

// O CHECK da coluna users.username repete o formato: mudou aqui, mude lá (nova migração).
func Check(s string) (string, error) {
	n := strings.TrimSpace(s)
	for i := 0; i < len(n); i++ {
		if n[i] >= 0x80 { // antes do ToLower, que é Unicode: o sinal Kelvin (U+212A) viraria 'k'
			return "", ErrInvalid
		}
	}
	n = strings.ToLower(n)
	if !valid(n) {
		return "", ErrInvalid
	}
	if reserved[n] {
		return "", ErrReserved
	}
	return n, nil
}

func valid(n string) bool {
	if len(n) < 3 || len(n) > 20 || n[0] < 'a' || n[0] > 'z' || n[len(n)-1] == '.' || strings.Contains(n, "..") {
		return false
	}
	for i := 0; i < len(n); i++ {
		c := n[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '_' && c != '.' {
			return false
		}
	}
	return true
}
