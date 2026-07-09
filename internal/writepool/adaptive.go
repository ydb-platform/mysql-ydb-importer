package writepool

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/mysql2ydb/mysql2ydb/internal/memory"
)

const (
	defaultWindow      = time.Minute
	defaultAdjustEvery = 30 * time.Second
	defaultMinLimit    = 1
	defaultMaxLimit    = maxLimitCeil
	increaseThreshold  = 0.98 // throughput flat or up → try more writers
	decreaseThreshold  = 0.95 // throughput down → fewer writers
)

// DefaultMaxLimit is the default upper bound on concurrent chunk writes.
func DefaultMaxLimit() int { return defaultMaxLimit }

// AdaptivePool limits concurrent chunk writes and tunes the limit from observed throughput.
type AdaptivePool struct {
	mu         sync.Mutex
	limit      int
	maxLimit   int
	memReserve uint64 // keep at least this much RAM free; 0 disables memory checks
	inFlight   int
	samples    []sample
	window     time.Duration
	stopCh     chan struct{}
	stopOnce   sync.Once
	onAdjust   func(oldLimit, newLimit int, rowsPerSec float64)
	waiters    []chan struct{}
}

type sample struct {
	at   time.Time
	rows int
}

// PoolOption configures an AdaptivePool.
type PoolOption func(*AdaptivePool)

// WithMaxLimit sets the upper bound on concurrent writes (default 32).
func WithMaxLimit(n int) PoolOption {
	return func(p *AdaptivePool) {
		if n > 0 {
			p.maxLimit = n
		}
	}
}

// WithMemoryReserve sets a minimum free-RAM threshold. The pool will not grow
// while MemAvailable is below 2× reserve and will shrink when below reserve.
func WithMemoryReserve(bytes uint64) PoolOption {
	return func(p *AdaptivePool) { p.memReserve = bytes }
}

// WithOnAdjust is called when the pool changes its concurrency limit (optional).
func WithOnAdjust(fn func(oldLimit, newLimit int, rowsPerSec float64)) PoolOption {
	return func(p *AdaptivePool) { p.onAdjust = fn }
}

// NewAdaptivePool starts a pool with one writer and a background tuner.
func NewAdaptivePool(opts ...PoolOption) *AdaptivePool {
	p := &AdaptivePool{
		limit:    defaultMinLimit,
		maxLimit: defaultMaxLimit,
		window:   defaultWindow,
		stopCh:   make(chan struct{}),
	}
	for _, opt := range opts {
		opt(p)
	}
	go p.adjustLoop()
	return p
}

// Limit returns the current concurrency limit.
func (p *AdaptivePool) Limit() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.limit
}

// InFlight returns the number of writes currently running.
func (p *AdaptivePool) InFlight() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.inFlight
}

// Acquire blocks until a write slot is available or ctx is done.
func (p *AdaptivePool) Acquire(ctx context.Context) error {
	for {
		p.mu.Lock()
		if p.inFlight < p.limit {
			p.inFlight++
			p.mu.Unlock()
			return nil
		}
		ch := make(chan struct{})
		p.waiters = append(p.waiters, ch)
		p.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			p.removeWaiter(ch)
			return ctx.Err()
		}
	}
}

func (p *AdaptivePool) removeWaiter(ch chan struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, w := range p.waiters {
		if w == ch {
			p.waiters = append(p.waiters[:i], p.waiters[i+1:]...)
			return
		}
	}
}

// Release records completed rows and frees a slot.
func (p *AdaptivePool) Release(rows int) {
	p.mu.Lock()
	if rows > 0 {
		p.samples = append(p.samples, sample{at: time.Now(), rows: rows})
		p.pruneSamplesLocked(time.Now())
	}
	p.inFlight--
	p.wakeWaitersLocked()
	p.mu.Unlock()
}

func (p *AdaptivePool) wakeWaitersLocked() {
	for p.inFlight < p.limit && len(p.waiters) > 0 {
		ch := p.waiters[0]
		p.waiters = p.waiters[1:]
		close(ch)
	}
}

// Close stops the background tuner.
func (p *AdaptivePool) Close() {
	p.stopOnce.Do(func() { close(p.stopCh) })
}

func (p *AdaptivePool) pruneSamplesLocked(now time.Time) {
	cutoff := now.Add(-p.window)
	i := 0
	for i < len(p.samples) && p.samples[i].at.Before(cutoff) {
		i++
	}
	if i > 0 {
		p.samples = append([]sample(nil), p.samples[i:]...)
	}
}

func (p *AdaptivePool) rowsPerSecLocked(now time.Time) float64 {
	p.pruneSamplesLocked(now)
	if len(p.samples) == 0 {
		return 0
	}
	oldest := p.samples[0].at
	span := now.Sub(oldest)
	if span < time.Second {
		span = time.Second
	}
	var total int
	for _, s := range p.samples {
		total += s.rows
	}
	return float64(total) / span.Seconds()
}

func (p *AdaptivePool) adjustLoop() {
	ticker := time.NewTicker(defaultAdjustEvery)
	defer ticker.Stop()
	var prevRate float64
	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.adjust(prevRate)
			prevRate = p.snapshotRate()
		}
	}
}

func (p *AdaptivePool) snapshotRate() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rowsPerSecLocked(time.Now())
}

func (p *AdaptivePool) adjust(prevRate float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	rate := p.rowsPerSecLocked(now)
	old := p.limit

	memTight := false
	if free := memory.FreeBytes(); p.memReserve > 0 && free > 0 {
		if free < p.memReserve && p.limit > defaultMinLimit {
			p.limit--
			p.logAdjust(old, rate, "memory pressure (%.1f GiB free, reserve %.1f GiB)",
				float64(free)/(1<<30), float64(p.memReserve)/(1<<30))
			p.wakeWaitersLocked()
			return
		}
		memTight = free < p.memReserve*2
	}

	if prevRate > 0 && rate > 0 {
		ratio := rate / prevRate
		switch {
		case ratio >= increaseThreshold && p.limit < p.maxLimit && !memTight:
			p.limit++
		case ratio < decreaseThreshold && p.limit > defaultMinLimit:
			p.limit--
		}
	} else if rate > 0 && p.limit < p.maxLimit && !memTight {
		p.limit++
	}
	if p.limit != old {
		p.logAdjust(old, rate, "")
		p.wakeWaitersLocked()
	}
}

func (p *AdaptivePool) logAdjust(old int, rate float64, extra string, args ...interface{}) {
	if p.onAdjust != nil {
		p.onAdjust(old, p.limit, rate)
		return
	}
	if extra != "" {
		all := append([]interface{}{old, p.limit, rate}, args...)
		log.Printf("write pool: concurrency %d -> %d (%.0f rows/s; "+extra+")", all...)
		return
	}
	log.Printf("write pool: concurrency %d -> %d (%.0f rows/s over last minute)", old, p.limit, rate)
}
