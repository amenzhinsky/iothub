package iotdevice

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/goautomotive/iothub/common"
)

// messageMux messages router.
type messageMux struct {
	on uint32
	mu sync.RWMutex
	s  []MessageHandler
}

func (m *messageMux) once(fn func() error) error {
	return once(&m.on, &m.mu, fn)
}

func once(i *uint32, mu *sync.RWMutex, fn func() error) error {
	// make a quick check without locking the mutex
	if atomic.LoadUint32(i) == 1 {
		return nil
	}
	mu.Lock()
	defer mu.Unlock()
	// someone can run the given func and change the value
	// between atomic checking and lock acquiring
	if *i == 1 {
		return nil
	}
	if err := fn(); err != nil {
		return err
	}
	atomic.StoreUint32(i, 1)
	return nil
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
	on uint32
	mu sync.RWMutex
	m  map[string]DirectMethodHandler
}

func (m *methodMux) once(fn func() error) error {
	return once(&m.on, &m.mu, fn)
}

// handle registers the given direct-method handler.
func (m *methodMux) handle(method string, fn DirectMethodHandler) error {
	if fn == nil {
		panic("fn is nil")
	}
	m.mu.Lock()
	if m.m == nil {
		m.m = map[string]DirectMethodHandler{}
	}
	if _, ok := m.m[method]; ok {
		m.mu.Unlock()
		return fmt.Errorf("method %q is already registered", method)
	}
	m.m[method] = fn
	m.mu.Unlock()
	return nil
}

// remove deregisters the named method.
func (m *methodMux) remove(method string) {
	m.mu.Lock()
	if m.m != nil {
		delete(m.m, method)
	}
	m.mu.Unlock()
}

// Dispatch dispatches the named method, error is not nil only when dispatching fails.
func (m *methodMux) Dispatch(method string, b []byte) (int, []byte, error) {
	m.mu.RLock()
	f, ok := m.m[method]
	m.mu.RUnlock()
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
	on uint32
	mu sync.RWMutex
	s  []TwinUpdateHandler
}

func (m *stateMux) once(fn func() error) error {
	return once(&m.on, &m.mu, fn)
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

// blocks until all handlers return
func (m *stateMux) Dispatch(b []byte) {
	var v TwinState
	if err := json.Unmarshal(b, &v); err != nil {
		log.Printf("unmarshal error: %s", err)
		return
	}

	w := sync.WaitGroup{}
	m.mu.RLock()
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
