package username

import "testing"

func TestCheck(t *testing.T) {
	for in, want := range map[string]string{
		"ana":                  "ana",
		"  Ana.Silva_9 ":       "ana.silva_9",
		"a1_":                  "a1_",
		"abcdefghijklmnopqrst": "abcdefghijklmnopqrst",
	} {
		if got, err := Check(in); err != nil || got != want {
			t.Errorf("%q: %q, %v; want %q", in, got, err, want)
		}
	}
	for in, want := range map[string]error{
		"ab":                    ErrInvalid,
		"abcdefghijklmnopqrstu": ErrInvalid,
		"9ana":                  ErrInvalid,
		"_ana":                  ErrInvalid,
		"ana..b":                ErrInvalid,
		"ana.":                  ErrInvalid,
		"ana-b":                 ErrInvalid,
		"joão":                  ErrInvalid,
		"ａｎａ":                   ErrInvalid,
		"\u212Aelvin":           ErrInvalid,
		"İstanbul":              ErrInvalid,
		"":                      ErrInvalid,
		"Admin":                 ErrReserved,
		"pokelocation":          ErrReserved,
	} {
		if _, err := Check(in); err != want {
			t.Errorf("%q: %v, want %v", in, err, want)
		}
	}
}
