package writepool

import (
	"context"
	"sync"
)

// SaveFunc persists progress after a contiguous prefix of chunks has been written.
type SaveFunc func(ctx context.Context, nextCursor []interface{}, rowsSoFar int) error

// OrderedTracker checkpoints chunks in ascending index order even when writes
// finish out of order.
type OrderedTracker struct {
	mu          sync.Mutex
	baseRows    int
	flushedRows int
	nextIdx     int
	pending     map[int]chunkDone
	onSave      SaveFunc
}

type chunkDone struct {
	nextCursor []interface{}
	chunkRows  int
}

// NewOrderedTracker creates a tracker. baseRows is rows already migrated before this run.
func NewOrderedTracker(baseRows int, onSave SaveFunc) *OrderedTracker {
	return &OrderedTracker{
		baseRows: baseRows,
		pending:  make(map[int]chunkDone),
		onSave:   onSave,
	}
}

// Complete records that chunk idx was written and flushes any contiguous prefix.
func (t *OrderedTracker) Complete(ctx context.Context, idx int, nextCursor []interface{}, chunkRows int) error {
	t.mu.Lock()
	t.pending[idx] = chunkDone{nextCursor: nextCursor, chunkRows: chunkRows}
	var err error
	for {
		cp, ok := t.pending[t.nextIdx]
		if !ok {
			break
		}
		delete(t.pending, t.nextIdx)
		t.flushedRows += cp.chunkRows
		rowsSoFar := t.baseRows + t.flushedRows
		next := cp.nextCursor
		t.nextIdx++
		t.mu.Unlock()
		if saveErr := t.onSave(ctx, next, rowsSoFar); saveErr != nil {
			err = saveErr
			t.mu.Lock()
			break
		}
		t.mu.Lock()
	}
	t.mu.Unlock()
	return err
}
