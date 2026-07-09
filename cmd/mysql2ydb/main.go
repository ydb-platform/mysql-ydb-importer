// mysql2ydb: create YDB schema from MySQL and migrate data in chunks via idempotent BulkUpsert.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/mysql2ydb/mysql2ydb/internal/config"
	"github.com/mysql2ydb/mysql2ydb/internal/memory"
	"github.com/mysql2ydb/mysql2ydb/internal/mysql"
	"github.com/mysql2ydb/mysql2ydb/internal/progress"
	"github.com/mysql2ydb/mysql2ydb/internal/schema"
	"github.com/mysql2ydb/mysql2ydb/internal/shutdown"
	"github.com/mysql2ydb/mysql2ydb/internal/writepool"
	"github.com/mysql2ydb/mysql2ydb/internal/ydb"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"
	ydblog "github.com/ydb-platform/ydb-go-sdk/v3/log"
	ydbquery "github.com/ydb-platform/ydb-go-sdk/v3/query"
	ydbtrace "github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"golang.org/x/sync/errgroup"
)

// queryIssueOpts returns query execute options that log YQL issues from successful queries when debug is on.
func queryIssueOpts(debug bool) []ydbquery.ExecuteOption {
	if !debug {
		return nil
	}
	return []ydbquery.ExecuteOption{ydb.IssuesHandler()}
}

func main() {
	cfg, err := config.Parse()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	log.Printf("ydb endpoint: %s", cfg.YDBEndpoint)

	ctx := context.Background()

	mysqldb, err := openMySQL(cfg.MySQLDSN)
	if err != nil {
		log.Fatalf("mysql: %v", err)
	}
	defer func() { _ = mysqldb.Close() }()
	// One MySQL reader per table; pool size follows table parallelism, not YDB write slots.
	mysqlConns := 8
	if cfg.ParallelTables > 1 {
		mysqlConns = cfg.ParallelTables * 2
		if mysqlConns < 8 {
			mysqlConns = 8
		}
	}
	mysqldb.SetMaxOpenConns(mysqlConns)
	idle := cfg.ParallelTables
	if idle < 4 {
		idle = 4
	}
	mysqldb.SetMaxIdleConns(idle)
	// Drop idle connections before MySQL wait_timeout; parallel tables leave pool connections idle longer between chunk reads.
	mysqldb.SetConnMaxLifetime(5 * time.Minute)
	mysqldb.SetConnMaxIdleTime(90 * time.Second)

	ydbOpts := []ydbsdk.Option{}
	if cfg.YDBDebug {
		ydbOpts = append(ydbOpts, ydbsdk.WithLogger(
			ydblog.Default(os.Stdout, ydblog.WithMinLevel(ydblog.DEBUG), ydblog.WithLogQuery()),
			ydbtrace.DetailsAll,
		))
		log.Println("YDB SDK trace logging enabled (-ydb-debug)")
	} else if cfg.YDBWarn {
		ydbOpts = append(ydbOpts, ydbsdk.WithLogger(
			ydblog.Default(os.Stdout, ydblog.WithMinLevel(ydblog.WARN)),
			ydbtrace.DetailsAll,
		))
		log.Println("YDB SDK WARN+ logging enabled (-ydb-warn)")
	}
	if cfg.YDBDebug || cfg.YDBDumpFailedChunks != "" {
		// Surface YQL issues from unary operations (e.g. BulkUpsert), which the default SDK logger
		// does not print and which have no per-call issues callback. Optionally dump chunks on issues.
		ydbOpts = append(ydbOpts, ydb.IssueLoggingDriver(ydb.IssueLoggingDriverOpts{
			LogIssues: cfg.YDBDebug,
			DumpDir:   cfg.YDBDumpFailedChunks,
		}))
	}
	ydbdb, err := ydb.Open(ctx, cfg, ydbOpts...)
	if err != nil {
		fatalYDB(cfg, err, "ydb: %v", err)
	}
	defer func() { _ = ydbdb.Close(ctx) }()

	// Use the driver's database path so Table/Query use the same path (fixes state table write visibility).
	dbPath := ydbdb.Name()
	if !strings.HasPrefix(dbPath, "/") {
		dbPath = "/" + dbPath
	}

	tables, err := resolveTables(mysqldb, cfg.Tables)
	if err != nil {
		log.Fatalf("tables: %v", err)
	}

	queryExec := ydb.QueryExecFunc(func(ctx context.Context, q string) error {
		var opts []ydbquery.ExecuteOption
		if cfg.YDBDebug {
			opts = append(opts, ydb.IssuesHandler())
		}
		err := ydbdb.Query().Exec(ctx, q, opts...)
		if err != nil {
			ydb.LogIssuesIfDebug(cfg.YDBDebug, err)
		}
		return err
	})
	stateTableExists, err := ydb.StateTableExists(ctx, ydbdb, dbPath)
	if err != nil {
		fatalYDB(cfg, err, "check state table: %v", err)
	}
	if cfg.ForceRecreate && !cfg.DataOnly {
		log.Println("Force recreate: dropping all tables in YDB...")
		for _, name := range tables {
			if err := ydb.DropTable(ctx, queryExec, dbPath, name); err != nil {
				fatalYDB(cfg, err, "drop table %s: %v", name, err)
			}
			log.Printf("  dropped table %s", name)
		}
		if err := ydb.DropTable(ctx, queryExec, dbPath, ydb.StateTableName); err != nil {
			fatalYDB(cfg, err, "drop state table: %v", err)
		}
		log.Printf("  dropped state table %s", ydb.StateTablePath(dbPath))
		stateTableExists = false
	}
	// If state table exists, schema was already created in a previous run — skip all DDL.
	if !cfg.DataOnly && !stateTableExists {
		log.Println("Creating schema in YDB...")
		for _, name := range tables {
			meta, err := schema.LoadTableMeta(mysqldb, name)
			if err != nil {
				log.Fatalf("schema %s: %v", name, err)
			}
			if err := ydb.CreateTable(ctx, queryExec, dbPath, meta); err != nil {
				fatalYDB(cfg, err, "create table %s: %v", name, err)
			}
			log.Printf("  created table %s", name)
		}
		// Single state table (progress + completion) after all user tables.
		if err := ydb.CreateStateTable(ctx, queryExec, dbPath); err != nil {
			fatalYDB(cfg, err, "state table: %v", err)
		}
		log.Printf("state table: %s", ydb.StateTablePath(dbPath))
	} else if !cfg.DataOnly && stateTableExists {
		log.Println("State table exists, skipping DDL.")
	}

	if cfg.SchemaOnly {
		log.Println("Schema only, done.")
		os.Exit(0)
	}

	// Ensure state table exists (e.g. when -data-only).
	if err := ydb.CreateStateTable(ctx, queryExec, dbPath); err != nil {
		fatalYDB(cfg, err, "state table: %v", err)
	}
	completedSet := make(map[string]bool)
	if !cfg.Force {
		completed, err := ydb.GetCompletedTables(ctx, ydbdb, dbPath, queryIssueOpts(cfg.YDBDebug)...)
		if err != nil {
			fatalYDB(cfg, err, "read migration status: %v", err)
		}
		for _, n := range completed {
			completedSet[n] = true
		}
	} else {
		log.Println("Force mode: re-transfer all tables (ignoring completed state).")
	}
	var pending []string
	for _, n := range tables {
		if !completedSet[n] {
			pending = append(pending, n)
		} else {
			log.Printf("  %s: skip (already migrated)", n)
		}
	}
	if len(pending) == 0 {
		log.Println("All tables already migrated, nothing to do.")
		return
	}

	writerOpts := []ydb.BulkUpsertWriterOption{}
	if cfg.YDBDebug {
		writerOpts = append(writerOpts, ydb.WithDebugIssues(true))
	}
	if cfg.ForceTxUpsert {
		writerOpts = append(writerOpts, ydb.WithForceTxUpsert(true))
		log.Println("Using transactional UPSERT (-ydb-force-tx-upsert)")
	}
	if cfg.YDBDumpFailedChunks != "" {
		writerOpts = append(writerOpts, ydb.WithDumpFailedChunks(cfg.YDBDumpFailedChunks))
		log.Printf("BulkUpsert issue dumps enabled (-ydb-dump-failed-chunks=%s)", cfg.YDBDumpFailedChunks)
	}
	writer := ydb.NewBulkUpsertWriter(ydbdb, dbPath, writerOpts...)
	freeRAM := memory.FreeBytes()
	if freeRAM > 0 {
		log.Printf("Available RAM for chunks: %.1f GiB", float64(freeRAM)/(1<<30))
	}
	log.Println("Transferring data (chunked BulkUpsert, idempotent, adaptive parallel writes)...")
	transferCtx, stopSignals := shutdown.NotifyContext(ctx)
	defer stopSignals()

	writeReserve := writepool.MemoryReserve(freeRAM)
	writeMax := writepool.MaxLimitForMemory(freeRAM, cfg.ParallelTables, cfg.BatchSize, 256)
	writePool := writepool.NewAdaptivePool(
		writepool.WithMaxLimit(writeMax),
		writepool.WithMemoryReserve(writeReserve),
	)
	log.Printf("Write pool: max concurrency %d, RAM reserve %.1f GiB (-parallel-tables=%d)",
		writeMax, float64(writeReserve)/(1<<30), cfg.ParallelTables)
	defer writePool.Close()

	prog := progress.NewDisplay(pending)
	prog.ReserveLines()
	progressCtx, progressCancel := context.WithCancel(transferCtx)
	defer progressCancel()
	go prog.Run(progressCtx)

	var transferErr error
	transferTable := func(ctx context.Context, name string) error {
		meta, err := schema.LoadTableMeta(mysqldb, name)
		if err != nil {
			return fmt.Errorf("schema %s: %w", name, err)
		}
		initialCursor, initialTotal, _, err := ydb.GetProgress(ctx, ydbdb, dbPath, name, queryIssueOpts(cfg.YDBDebug)...)
		if err != nil {
			ydb.LogIssuesIfDebug(cfg.YDBDebug, err)
			return fmt.Errorf("progress %s: %w", name, err)
		}
		batchSize := batchSizeForTable(cfg.BatchSize, cfg.MaxChunkRows, freeRAM, cfg.ParallelTables, mysqldb, name)
		reader := mysql.NewChunkReader(mysqldb, meta, batchSize)
		type chunkMsg struct {
			rows []map[string]interface{}
			next []interface{}
		}
		ch := make(chan chunkMsg, writepool.ChunkChannelBuf())
		var rowsSoFar atomic.Int64
		rowsSoFar.Store(int64(initialTotal))
		tracker := writepool.NewOrderedTracker(initialTotal, func(saveCtx context.Context, nextCursor []interface{}, total int) error {
			rowsSoFar.Store(int64(total))
			prog.Update(name, total)
			if err := ydb.SaveProgressFlushing(saveCtx, ydbdb, dbPath, name, nextCursor, total); err != nil {
				ydb.LogIssuesIfDebug(cfg.YDBDebug, err)
				return fmt.Errorf("save progress to %s: %w", ydb.StateTablePath(dbPath), err)
			}
			return nil
		})
		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error {
			defer close(ch)
			cursor := initialCursor
			for {
				rows, next, hasMore, err := reader.ReadChunk(gCtx, cursor)
				if err != nil {
					return fmt.Errorf("mysql read: %w", err)
				}
				if len(rows) > 0 {
					var nextSend []interface{}
					if hasMore {
						nextSend = next
					}
					select {
					case ch <- chunkMsg{rows: rows, next: nextSend}:
					case <-gCtx.Done():
						return gCtx.Err()
					}
				}
				if !hasMore {
					break
				}
				cursor = next
			}
			return nil
		})
		g.Go(func() error {
			writeG, writeCtx := errgroup.WithContext(gCtx)
			chunkIdx := 0
			for msg := range ch {
				msg := msg
				idx := chunkIdx
				chunkIdx++
				if err := writePool.Acquire(writeCtx); err != nil {
					return err
				}
				writeG.Go(func() error {
					defer writePool.Release(len(msg.rows))
					written, err := writer.WriteChunk(writeCtx, meta, msg.rows)
					if err != nil {
						return fmt.Errorf("ydb write: %w", err)
					}
					if err := tracker.Complete(writeCtx, idx, msg.next, written); err != nil {
						return fmt.Errorf("progress: %w", err)
					}
					return nil
				})
			}
			return writeG.Wait()
		})
		if err := g.Wait(); err != nil {
			return fmt.Errorf("table %s: %w", name, err)
		}
		total := int(rowsSoFar.Load())
		if err := ydb.MarkTableCompleted(ctx, ydbdb, dbPath, name, total); err != nil {
			ydb.LogIssuesIfDebug(cfg.YDBDebug, err)
			return fmt.Errorf("mark %s completed: %w", name, err)
		}
		prog.SetDone(name, total)
		return nil
	}

	if cfg.ParallelTables <= 1 {
		for _, name := range pending {
			if err := transferTable(transferCtx, name); err != nil {
				transferErr = err
				break
			}
		}
	} else {
		g, gCtx := errgroup.WithContext(transferCtx)
		g.SetLimit(cfg.ParallelTables)
		for _, name := range pending {
			name := name
			g.Go(func() error {
				return transferTable(gCtx, name)
			})
		}
		transferErr = g.Wait()
	}
	prog.Stop()
	if transferErr != nil {
		if shutdown.IsInterrupt(transferErr) {
			shutdown.LogInterrupt()
			os.Exit(130)
		}
		fatalYDB(cfg, transferErr, "transfer: %v", transferErr)
	}
	log.Println("Done.")
}

// fatalYDB logs any YQL/operation issues carried by err (only when -ydb-debug is on), then exits.
func fatalYDB(cfg *config.Config, err error, format string, args ...interface{}) {
	ydb.LogIssuesIfDebug(cfg.YDBDebug, err)
	log.Fatalf(format, args...)
}

func openMySQL(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// batchSizeForTable caps batch size so that:
// - one chunk fits in a fraction of available RAM;
// - one chunk is downloaded from MySQL in ≤10s at 100 Mbit/s (125 MB);
// - never exceeds maxChunkRows.
func batchSizeForTable(configBatch, maxChunkRows int, freeRAM uint64, parallelTables int, db *sql.DB, tableName string) int {
	batch := configBatch
	if maxChunkRows > 0 && batch > maxChunkRows {
		batch = maxChunkRows
	}
	if parallelTables > 1 && freeRAM > 0 {
		freeRAM /= uint64(parallelTables)
	}
	dataLen, rowCount, err := schema.TableSize(db, tableName)
	var avgRowBytes uint64
	if err == nil && dataLen > 0 && rowCount > 0 {
		avgRowBytes = dataLen / rowCount
		if avgRowBytes == 0 {
			avgRowBytes = 256
		}
		// 100 Mbit/s × 10s = 125 MB — limit chunk so fetch takes ≤10s
		const maxChunkBytes = 125 * 1024 * 1024
		if networkBatch := maxChunkBytes / avgRowBytes; networkBatch > 0 && int(networkBatch) < batch {
			batch = int(networkBatch)
		}
	}
	if freeRAM == 0 {
		if maxChunkRows > 0 && batch > maxChunkRows {
			batch = maxChunkRows
		}
		return batch
	}
	if avgRowBytes == 0 {
		avgRowBytes = 256
	}
	const ramFraction = 0.25
	const overheadFactor = 3
	chunkMemLimit := uint64(ramFraction * float64(freeRAM))
	memSafeBatch := chunkMemLimit / (avgRowBytes * overheadFactor)
	const minBatch = 1000
	if memSafeBatch < minBatch {
		memSafeBatch = minBatch
	}
	if int(memSafeBatch) < batch {
		batch = int(memSafeBatch)
	}
	if maxChunkRows > 0 && batch > maxChunkRows {
		batch = maxChunkRows
	}
	return batch
}

func resolveTables(db *sql.DB, filter []string) ([]string, error) {
	all, err := schema.TableNames(db)
	if err != nil {
		return nil, err
	}
	if len(filter) == 0 {
		return all, nil
	}
	set := make(map[string]bool)
	for _, t := range filter {
		set[t] = true
	}
	var out []string
	for _, t := range all {
		if set[t] {
			out = append(out, t)
		}
	}
	return out, nil
}
