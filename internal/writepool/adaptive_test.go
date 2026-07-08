package writepool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAdaptivePool_acquireRelease(t *testing.T) {
	p := NewAdaptivePool(WithMaxLimit(2))
	defer p.Close()
	ctx := context.Background()
	if err := p.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	if p.InFlight() != 1 {
		t.Fatalf("inFlight = %d", p.InFlight())
	}
	p.Release(100)
	if p.InFlight() != 0 {
		t.Fatalf("inFlight = %d after release", p.InFlight())
	}
}

func TestAdaptivePool_blocksAtLimit(t *testing.T) {
	p := NewAdaptivePool(WithMaxLimit(1))
	defer p.Close()
	ctx := context.Background()
	if err := p.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		_ = p.Acquire(ctx)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("second acquire should block")
	case <-time.After(50 * time.Millisecond):
	}
	p.Release(0)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("second acquire did not unblock")
	}
	p.Release(0)
}

func TestAdaptivePool_increasesOnFlatThroughput(t *testing.T) {
	p := NewAdaptivePool(WithMaxLimit(4), WithOnAdjust(func(old, new int, _ float64) {
		_ = old
		_ = new
	}))
	defer p.Close()

	now := time.Now()
	p.mu.Lock()
	for i := 0; i < 10; i++ {
		p.samples = append(p.samples, sample{at: now.Add(-time.Duration(50-i) * time.Second), rows: 1000})
	}
	p.mu.Unlock()

	p.adjust(180) // prev rate below current (~200 rows/s) → should increase
	if p.Limit() != 2 {
		t.Fatalf("limit = %d, want 2", p.Limit())
	}
}

func TestAdaptivePool_decreasesOnDrop(t *testing.T) {
	p := NewAdaptivePool(WithMaxLimit(8))
	defer p.Close()
	p.mu.Lock()
	p.limit = 4
	p.mu.Unlock()

	p.adjust(2000) // high prev, no samples → rate 0, no change
	if p.Limit() != 4 {
		t.Fatalf("limit = %d with no samples", p.Limit())
	}

	now := time.Now()
	p.mu.Lock()
	p.samples = []sample{{at: now.Add(-30 * time.Second), rows: 500}}
	p.mu.Unlock()
	p.adjust(2000) // big drop
	if p.Limit() != 3 {
		t.Fatalf("limit = %d, want 3 after throughput drop", p.Limit())
	}
}

func TestAdaptivePool_parallelWrites(t *testing.T) {
	p := NewAdaptivePool(WithMaxLimit(4))
	defer p.Close()
	p.mu.Lock()
	p.limit = 4
	p.mu.Unlock()

	ctx := context.Background()
	var peak atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.Acquire(ctx); err != nil {
				return
			}
			cur := int32(p.InFlight())
			for {
				old := peak.Load()
				if cur <= old || peak.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			p.Release(10)
		}()
	}
	wg.Wait()
	if peak.Load() < 2 {
		t.Fatalf("peak in-flight = %d, expected >1", peak.Load())
	}
}
