package iotutil

import (
	"sync"
	"testing"
)

func TestRIDGenerator_Next(t *testing.T) {
	t.Parallel()

	r := NewRIDGenerator()
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	wg.Add(10)
	rids := make([]string, 0, 10000)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 1000; j++ {
				mu.Lock()
				rids = append(rids, r.Next())
				mu.Unlock()
			}
			wg.Done()
		}()
	}
	wg.Wait()

	m := make(map[string]bool, 10000)
	for _, r := range rids {
		if _, ok := m[r]; ok {
			t.Fatal("sequence collision")
			m[r] = true
		}
	}
}
