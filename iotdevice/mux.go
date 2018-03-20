package iotdevice

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"sync"

	"github.com/amenzhinsky/golang-iothub/common"
)

// messageMux messages router.
type messageMux struct {
	on bool
	mu sync.RWMutex
	s  []MessageHandler
}

// add adds the given handler to the handlers list.
func (m *messageMux) add(fn MessageHandler) {
	if fn == nil {
		panic("fn is nil")
	}
	m.mu.Lock()
	m.s = append(m.s, fn)
	m.mu.Unlock()
}

// remove removes all matched handlers from the handlers list.
func (m *messageMux) remove(fn MessageHandler) {
	m.mu.RLock()
	for i := len(m.s) - 1; i >= 0; i-- {
		if ptreq(m.s[i], fn) {
			m.s = append(m.s[:i], m.s[i+1:]...)
		}
	}
	m.mu.RUnlock()
}

func ptreq(v1, v2 interface{}) bool {
	return reflect.ValueOf(v1).Pointer() == reflect.ValueOf(v2).Pointer()
}

// Dispatch handles every handler in its own goroutine to prevent blocking.
func (m *messageMux) Dispatch(msg *common.Message) {
	m.mu.RLock()
	for _, fn := range m.s {
		fn(msg)
	}
	m.mu.RUnlock()
}

// methodMux is direct-methods dispatcher.
type methodMux struct {
	on bool
	mu sync.RWMutex
	m  map[string]DirectMethodHandler
}

// handle registers the given direct-method handler.
func (r *methodMux) handle(method string, fn DirectMethodHandler) error {
	if fn == nil {
		panic("fn is nil")
	}
	r.mu.Lock()
	if r.m == nil {
		r.m = map[string]DirectMethodHandler{}
	}
	if _, ok := r.m[method]; ok {
		r.mu.Unlock()
		return fmt.Errorf("method %q is already registered", method)
	}
	r.m[method] = fn
	r.mu.Unlock()
	return nil
}

// remove deregisters the named method.
func (r *methodMux) remove(method string) {
	r.mu.Lock()
	if r.m != nil {
		delete(r.m, method)
	}
	r.mu.Unlock()
}

// Dispatch dispatches the named method, error is not nil only when dispatching fails.
func (r *methodMux) Dispatch(method string, b []byte) (int, []byte, error) {
	r.mu.RLock()
	f, ok := r.m[method]
	r.mu.RUnlock()
	if !ok {
		return 0, nil, fmt.Errorf("method %q is not registered", method)
	}

	var v map[string]interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return jsonErr(err)
	}
	v, err := f(v)
	if err != nil {
		return jsonErr(err)
	}
	if v == nil {
		v = map[string]interface{}{}
	}
	b, err = json.Marshal(v)
	if err != nil {
		return jsonErr(err)
	}
	return 200, b, nil
}

func jsonErr(err error) (int, []byte, error) {
	return 500, []byte(fmt.Sprintf(`{"error":%q}`, err.Error())), nil
}

// mostly copy-paste of messageRouter
type stateMux struct {
	on bool
	mu sync.RWMutex
	s  []TwinUpdateHandler
}

func (m *stateMux) add(fn TwinUpdateHandler) {
	if fn == nil {
		panic("fn is nil")
	}
	m.mu.Lock()
	m.s = append(m.s, fn)
	m.mu.Unlock()
}

func (m *stateMux) remove(fn TwinUpdateHandler) {
	m.mu.RLock()
	for i := len(m.s) - 1; i >= 0; i-- {
		if ptreq(m.s[i], fn) {
			m.s = append(m.s[:i], m.s[i+1:]...)
		}
	}
	m.mu.RUnlock()
}

func (m *stateMux) Dispatch(b []byte) {
	var v TwinState
	if err := json.Unmarshal(b, &v); err != nil {
		log.Printf("unmarshal error: %s", err)
		return
	}

	m.mu.RLock()
	w := sync.WaitGroup{}
	w.Add(len(m.s))
	for _, fn := range m.s {
		go func(f TwinUpdateHandler) {
			f(v)
			w.Done()
		}(fn)
	}
	m.mu.RUnlock()
	w.Wait()
}
