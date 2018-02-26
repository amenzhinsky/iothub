package iotdevice

import "testing"

func TestPtreq(t *testing.T) {
	t.Parallel()

	f1 := func() {}
	f2 := func() {}
	var f3 func() = nil

	for _, v := range []struct {
		f1 func()
		f2 func()
		eq bool
	}{
		{f1, f1, true},
		{f2, f2, true},
		{f1, f2, false},
		{f1, f3, false},
	} {
		if g := ptreq(v.f1, v.f2); g != v.eq {
			t.Errorf("prteq(%p, %p) = %t, want %t", v.f1, v.f2, g, v.eq)
		}
	}
}
