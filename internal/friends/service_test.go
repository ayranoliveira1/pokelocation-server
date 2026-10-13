package friends

import (
	"regexp"
	"testing"
)

// Espelha Api.normalizeCode do app: os dois lados têm de concordar no mesmo código.
func TestNormalize(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"7K3M9QXA", "7K3M9QXA"},
		{"7k3m9qxa", "7K3M9QXA"},
		{" 7K3M-9QXA ", "7K3M9QXA"},
		{"7K3M 9QXA", "7K3M9QXA"},
		{"\t7K3M\n9QXA", "7K3M9QXA"},
		{"ILOo1234", "1100" + "1234"},
		{"il0o1234", "11001234"},
		{"7K3M9QX", ""},   // curto
		{"7K3M9QXAB", ""}, // longo
		{"7K3M9QXU", ""},  // U fora do Crockford
		{"7K3M9QX!", ""},
		{"7K3M9QXé", ""},
		{"", ""},
		{"--------", ""},
	} {
		if got := Normalize(tc.in); got != tc.want {
			t.Errorf("Normalize(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

var crockford8 = regexp.MustCompile(`^[0-9A-HJKMNP-TV-Z]{8}$`) // o CHECK de users.friend_code

func TestNewCode(t *testing.T) {
	seen := map[string]bool{}
	hit := map[rune]bool{}
	for range 2000 {
		c := NewCode()
		if !crockford8.MatchString(c) {
			t.Fatalf("NewCode() = %q: fora do Crockford 8", c)
		}
		if Normalize(c) != c {
			t.Fatalf("NewCode() = %q não é estável na normalização", c)
		}
		if seen[c] {
			t.Fatalf("NewCode() repetiu %q em 2000", c)
		}
		seen[c] = true
		for _, r := range c {
			hit[r] = true
		}
	}
	if len(hit) != 32 { // 16000 sorteios: todos os 32 símbolos aparecem (detecta alfabeto cortado/viés grosso)
		t.Fatalf("NewCode usou %d símbolos, want 32", len(hit))
	}
}

func BenchmarkNormalize(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		Normalize(" 7k3m-9qxa ")
	}
}
