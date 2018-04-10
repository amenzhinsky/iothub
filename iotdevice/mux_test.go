package iotdevice

import (
	"bytes"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/goautomotive/iothub/common"
)

func TestMessageMux(t *testing.T) {
	t.Parallel()

	var i uint32
	f1 := func(*common.Message) {
		atomic.AddUint32(&i, 1)
	}
	f2 := func(*common.Message) {
		atomic.AddUint32(&i, 1)
	}

	m := &messageMux{}

	m.add(f1)
	m.add(f1)
	m.add(f2)
	testRecvNum(t, m, &i, 3)

	m.remove(f1)
	testRecvNum(t, m, &i, 1)

	m.remove(f2)
	testRecvNum(t, m, &i, 0)
}

func TestMessageMux_Once(t *testing.T) {
	t.Parallel()

	m := &messageMux{}
	if err := m.once(func() error {
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// func has to be ignored for the second time
	if err := m.once(func() error {
		return errors.New("some error")
	}); err != nil {
		t.Fatal(err)
	}
}

func testRecvNum(t *testing.T, m *messageMux, i *uint32, w uint32) {
	atomic.StoreUint32(i, 0) // zero counter
	m.Dispatch(&common.Message{})

	if g := atomic.LoadUint32(i); g != w {
		t.Fatalf("recv num = %d, want %d", g, w)
	}
}

func TestMethodMux(t *testing.T) {
	t.Parallel()

	m := methodMux{}
	if err := m.handle("add", func(v map[string]interface{}) (map[string]interface{}, error) {
		v["b"] = 2
		return v, nil
	}); err != nil {
		t.Fatal(err)
	}
	defer m.remove("add")

	rc, data, err := m.Dispatch("add", []byte(`{"a":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if rc != 200 {
		t.Errorf("rc = %d, want %d", rc, 200)
	}
	w := []byte(`{"a":1,"b":2}`)
	if !bytes.Equal(data, w) {
		t.Errorf("data = %q, want %q", data, w)
	}
}
