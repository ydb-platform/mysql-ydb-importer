// Package writepool provides adaptive parallel YDB chunk writes.
//
// AdaptivePool limits how many chunks may be written to YDB at once. It starts
// with one in-flight write and periodically compares rows/sec over a rolling
// one-minute window: when throughput rises or stays flat it increases the limit,
// when throughput falls it decreases the limit. The upper bound is derived from
// available RAM and -parallel-tables so the process is not OOM-killed.
// tables when -parallel-tables > 1 so chunks from different tables can write
// concurrently.
//
// OrderedTracker saves migration progress (resume cursor) only after chunks
// complete in read order, so parallel writes remain safe to resume after interrupt.

package writepool
