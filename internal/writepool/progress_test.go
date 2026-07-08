package writepool

import (
	"context"
	"testing"
)

func TestOrderedTracker_inOrder(t *testing.T) {
	var saved []int
	tr := NewOrderedTracker(100, func(_ context.Context, _ []interface{}, rowsSoFar int) error {
		saved = append(saved, rowsSoFar)
		return nil
	})
	ctx := context.Background()
	if err := tr.Complete(ctx, 0, []interface{}{1}, 10); err != nil {
		t.Fatal(err)
	}
	if len(saved) != 1 || saved[0] != 110 {
		t.Fatalf("saved = %v, want [110]", saved)
	}
	if err := tr.Complete(ctx, 1, []interface{}{2}, 5); err != nil {
		t.Fatal(err)
	}
	if len(saved) != 2 || saved[1] != 115 {
		t.Fatalf("saved = %v, want [110 115]", saved)
	}
}

func TestOrderedTracker_outOfOrder(t *testing.T) {
	var saved []int
	tr := NewOrderedTracker(0, func(_ context.Context, _ []interface{}, rowsSoFar int) error {
		saved = append(saved, rowsSoFar)
		return nil
	})
	ctx := context.Background()
	if err := tr.Complete(ctx, 2, nil, 3); err != nil {
		t.Fatal(err)
	}
	if len(saved) != 0 {
		t.Fatalf("chunk 2 first: saved = %v, want none", saved)
	}
	if err := tr.Complete(ctx, 0, []interface{}{"a"}, 10); err != nil {
		t.Fatal(err)
	}
	if len(saved) != 1 || saved[0] != 10 {
		t.Fatalf("after chunk 0: saved = %v", saved)
	}
	if err := tr.Complete(ctx, 1, []interface{}{"b"}, 7); err != nil {
		t.Fatal(err)
	}
	if len(saved) != 3 {
		t.Fatalf("saved = %v, want 3 checkpoints", saved)
	}
	if saved[2] != 20 {
		t.Fatalf("final rows = %d, want 20", saved[2])
	}
}
