package iotutil

import "testing"

func TestFormatPayload(t *testing.T) {
	t.Parallel()

	for _, s := range []struct {
		b []byte
		w string
	}{
		{[]byte(`{"a": "бла", "b": 1}`), `{"a": "бла", "b": 1}`},
		{[]byte{0x0, 0xa, 0xab}, "00 0a ab"},
	} {
		if g := FormatPayload(s.b); g != s.w {
			t.Errorf("FormatPayload(%v) = %q, want %q", s.b, g, s.w)
		}
	}
}
