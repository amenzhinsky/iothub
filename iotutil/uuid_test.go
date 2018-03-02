package iotutil

import (
	"testing"
)

func TestUUID(t *testing.T) {
	t.Parallel()
	s := make([]string, 10000)
	for i := 0; i < 10000; i++ {
		s[i] = UUID()
		for j := 0; j < i; j++ {
			if s[i] == s[j] {
				t.Fatal("UUID collision")
			}
		}
	}
}
