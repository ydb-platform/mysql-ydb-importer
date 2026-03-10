package config

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

// Config holds MySQL, YDB and migration settings.
type Config struct {
	// MySQL
	MySQLDSN string

	// YDB
	YDBEndpoint   string
	YDBDatabase   string
	YDBTokenFile  string // path to file with access token (optional)
	YDBDebug      bool   // enable YDB SDK trace logs (driver, table, query, retry)
	YDBWarn       bool   // enable YDB SDK logs at WARN level and above only

	// Migration
	SchemaOnly     bool
	DataOnly       bool
	BatchSize       int
	MaxChunkRows    int    // cap per-chunk rows so read/write alternate often (avoid "hang")
	ParallelTables  int    // max number of tables to transfer in parallel (1 = sequential)
	ForceTxUpsert   bool   // use transactional UPSERT instead of BulkUpsert (workaround if BulkUpsert hangs)
	Force          bool   // re-transfer all tables, ignore completed state in _mysql2ydb_state
	ForceRecreate   bool   // drop all tables in YDB (including state), then create schema from scratch
	TablesStr      string // comma-separated; empty = all tables
	Tables         []string
}

// Parse reads config from flags. MySQL connection is taken from -mysql or from ~/.my.cnf ([client]).
func Parse() (*Config, error) {
	cfg := &Config{}
	flag.StringVar(&cfg.MySQLDSN, "mysql", "", "MySQL DSN (overrides ~/.my.cnf if set)")
	flag.StringVar(&cfg.YDBEndpoint, "ydb", "", "YDB endpoint (e.g. grpc://localhost:2136)")
	flag.StringVar(&cfg.YDBEndpoint, "ydb-endpoint", "", "YDB endpoint (alias for -ydb)")
	flag.StringVar(&cfg.YDBDatabase, "ydb-database", "local", "YDB database path (e.g. /local)")
	flag.StringVar(&cfg.YDBTokenFile, "ydb-token-file", "", "Path to file with YDB access token (optional)")
	flag.BoolVar(&cfg.YDBDebug, "ydb-debug", false, "Enable YDB SDK trace logs (driver, table, query, retry) for debugging hangs")
	flag.BoolVar(&cfg.YDBWarn, "ydb-warn", false, "Enable YDB SDK logs at WARN level and above only")
	flag.BoolVar(&cfg.SchemaOnly, "schema-only", false, "Only create schema in YDB, do not transfer data")
	flag.BoolVar(&cfg.DataOnly, "data-only", false, "Only transfer data (schema must already exist)")
	flag.IntVar(&cfg.BatchSize, "batch-size", 10_000, "Number of rows per chunk for large tables")
	flag.IntVar(&cfg.MaxChunkRows, "max-chunk-rows", 25_000, "Max rows per chunk (avoids long read before first write)")
	flag.IntVar(&cfg.ParallelTables, "parallel-tables", 1, "Max number of tables to transfer in parallel (1 = sequential)")
	flag.BoolVar(&cfg.ForceTxUpsert, "ydb-force-tx-upsert", false, "Use transactional UPSERT instead of BulkUpsert (slower but works if BulkUpsert hangs)")
	flag.BoolVar(&cfg.Force, "force", false, "Re-transfer all tables, ignore completed state (use if data is missing but state says done)")
	flag.BoolVar(&cfg.ForceRecreate, "force-recreate", false, "Drop all tables in YDB (data + state), then create schema from scratch")
	flag.StringVar(&cfg.TablesStr, "tables", "", "Comma-separated table names (default: all)")
	flag.Parse()

	if cfg.TablesStr != "" {
		for _, s := range strings.Split(cfg.TablesStr, ",") {
			if t := strings.TrimSpace(s); t != "" {
				cfg.Tables = append(cfg.Tables, t)
			}
		}
	}

	if cfg.MySQLDSN == "" {
		opts, err := ReadMyCnf()
		if err != nil {
			return nil, fmt.Errorf("mysql: set -mysql or create ~/.my.cnf with [client] (user, password, host, port, database): %w", err)
		}
		cfg.MySQLDSN = opts.DSN()
		if opts.Database == "" {
			return nil, fmt.Errorf("mysql: set -mysql or set database= in ~/.my.cnf [client]")
		}
	}
	if cfg.YDBEndpoint == "" {
		cfg.YDBEndpoint = os.Getenv("YDB_ENDPOINT")
	}
	if cfg.YDBEndpoint == "" {
		return nil, fmt.Errorf("ydb endpoint is required: set -ydb-endpoint (or -ydb) or YDB_ENDPOINT env")
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 10_000
	}
	if cfg.MaxChunkRows <= 0 {
		cfg.MaxChunkRows = 25_000
	}
	if cfg.ParallelTables <= 0 {
		cfg.ParallelTables = 1
	}
	return cfg, nil
}
