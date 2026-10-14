package main

import (
	"testing"
	"time"
)

func TestParseCPU(t *testing.T) {
	for in, want := range map[string]time.Duration{
		"0:01.50":    1500 * time.Millisecond, // macOS
		"12:03.25":   12*time.Minute + 3250*time.Millisecond,
		"1:02:03.00": time.Hour + 2*time.Minute + 3*time.Second,
		"00:00:07":   7 * time.Second, // Linux
		"2-01:00:00": 49 * time.Hour,
	} {
		if got := parseCPU(in); got != want {
			t.Errorf("parseCPU(%q) = %v, esperado %v", in, got, want)
		}
	}
}
