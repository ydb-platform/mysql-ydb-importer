package writepool

import "testing"

func TestMaxLimitForMemory(t *testing.T) {
	if got := MaxLimitForMemory(0, 1, 10_000, 256); got != 8 {
		t.Fatalf("no meminfo: got %d want 8", got)
	}
	got := MaxLimitForMemory(8<<30, 1, 10_000, 256)
	if got < 32 || got > maxLimitCeil {
		t.Fatalf("large RAM: got %d want in [32, %d]", got, maxLimitCeil)
	}
	got = MaxLimitForMemory(512<<20, 4, 10_000, 1024)
	if got < maxLimitFloor || got > maxLimitCeil {
		t.Fatalf("tight RAM: got %d", got)
	}
}

func TestMemoryReserve(t *testing.T) {
	if got := MemoryReserve(0); got != minMemoryReserve {
		t.Fatalf("got %d want min %d", got, minMemoryReserve)
	}
	if got := MemoryReserve(8 << 30); got != (8<<30)/10 {
		t.Fatalf("got %d", got)
	}
}
