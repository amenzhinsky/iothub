package mqtt

import (
	"sync"
	"testing"
)

func TestRID(t *testing.T) {
	t.Parallel()

	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	wg.Add(10)
	rids := make([]string, 0, 10000)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 1000; j++ {
				rid := GenRID()
				mu.Lock()
				rids = append(rids, rid)
				mu.Unlock()
			}
			wg.Done()
		}()
	}
	wg.Wait()

	m := make(map[string]bool, 10000)
	for _, r := range rids {
		if _, ok := m[r]; ok {
			t.Fatal("genRID sequence collision")
			m[r] = true
		}
	}
}
