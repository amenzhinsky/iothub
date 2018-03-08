package iotdevice

import (
	"bytes"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amenzhinsky/golang-iothub/common"
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

func testRecvNum(t *testing.T, m *messageMux, i *uint32, num uint32) {
	atomic.StoreUint32(i, 0) // zero counter
	m.Dispatch(&common.Message{})
	time.Sleep(time.Millisecond)
	if *i != num {
		t.Fatalf("recv num = %d, want %d", i, num)
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
