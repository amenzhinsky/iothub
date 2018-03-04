package iotutil

import (
	"testing"
)

func TestRIDGenerator_Next(t *testing.T) {
	t.Parallel()
	g := NewRIDGenerator()
	s := make([]string, 10000)
	for i := 0; i < 10000; i++ {
		s[i] = g.Next()
		for j := 0; j < i; j++ {
			if s[i] == s[j] {
				t.Fatal("RID collision")
			}
		}
	}
}
