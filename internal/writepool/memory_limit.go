package writepool

const (
	maxLimitFloor        = 1
	maxLimitCeil         = 1000
	chunkChannelBuf      = 4
	chunkMemoryOverhead  = 3
	writePoolRAMFraction = 0.4
	minMemoryReserve     = 256 << 20 // 256 MiB
)

// ChunkChannelBuf is the per-table channel capacity between MySQL read and YDB write.
func ChunkChannelBuf() int { return chunkChannelBuf }

// MemoryReserve returns how much free RAM the write pool should try to keep.
func MemoryReserve(freeBytes uint64) uint64 {
	if freeBytes == 0 {
		return minMemoryReserve
	}
	reserve := freeBytes / 10
	if reserve < minMemoryReserve {
		return minMemoryReserve
	}
	return reserve
}

// MaxLimitForMemory estimates a safe upper bound on concurrent chunk writes.
func MaxLimitForMemory(freeBytes uint64, parallelTables, batchRows int, avgRowBytes uint64) int {
	if batchRows <= 0 {
		batchRows = 10_000
	}
	if avgRowBytes == 0 {
		avgRowBytes = 256
	}
	tables := parallelTables
	if tables < 1 {
		tables = 1
	}
	if freeBytes == 0 {
		return 8
	}
	chunkBytes := uint64(batchRows) * avgRowBytes * chunkMemoryOverhead
	if chunkBytes == 0 {
		return 8
	}
	budget := uint64(float64(freeBytes) * writePoolRAMFraction)
	perTable := budget / uint64(tables)
	slots := perTable / chunkBytes
	if slots <= uint64(chunkChannelBuf) {
		return maxLimitFloor
	}
	limit := int(slots) - chunkChannelBuf
	if limit < maxLimitFloor {
		return maxLimitFloor
	}
	if limit > maxLimitCeil {
		return maxLimitCeil
	}
	return limit
}
