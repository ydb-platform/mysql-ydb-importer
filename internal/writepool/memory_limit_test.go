package writepool

import "testing"

func TestMaxLimitForMemory(t *testing.T) {
	if got := MaxLimitForMemory(0, 1, 10_000, 256); got != 8 {
		t.Fatalf("no meminfo: got %d want 8", got)
	}
	// 8 GiB, one table, 10k rows × 256 B × 3 ≈ 7.5 MiB/chunk → many slots, capped at 32
	got := MaxLimitForMemory(8<<30, 1, 10_000, 256)
	if got != maxLimitCeil {
		t.Fatalf("large RAM: got %d want %d", got, maxLimitCeil)
	}
	// 512 MiB, 4 parallel tables → tight budget
	got = MaxLimitForMemory(512<<20, 4, 10_000, 1024)
	if got < maxLimitFloor || got > maxLimitCeil {
		t.Fatalf("tight RAM: got %d", got)
	}
}
