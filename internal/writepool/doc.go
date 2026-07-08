// Package writepool provides adaptive parallel YDB chunk writes.
//
// AdaptivePool limits how many chunks may be written to YDB at once. It starts
// with one in-flight write and periodically compares rows/sec over a rolling
// one-minute window: when throughput rises or stays flat it increases the limit
// (up to 1000), when throughput falls it decreases the limit. Growth is blocked
// and the limit is reduced when free RAM drops below a reserve derived from
// MemAvailable at startup. The pool is shared across all tables when
// -parallel-tables > 1 so chunks from different tables can write concurrently.
//
// OrderedTracker saves migration progress (resume cursor) only after chunks
// complete in read order, so parallel writes remain safe to resume after interrupt.

package writepool
