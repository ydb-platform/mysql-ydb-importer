package ydb

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mysql2ydb/mysql2ydb/internal/schema"
)

type bulkUpsertDumpContextKey struct{}

// BulkUpsertDumpContext carries the in-flight chunk for dumping when BulkUpsert emits YQL issues.
type BulkUpsertDumpContext struct {
	Meta      *schema.TableMeta
	TablePath string
	Rows      []map[string]interface{}

	mu     sync.Mutex
	dumped bool
}

// WithBulkUpsertDumpContext attaches chunk metadata to ctx for the BulkUpsert driver trace hook.
func WithBulkUpsertDumpContext(ctx context.Context, meta *schema.TableMeta, tablePath string, rows []map[string]interface{}) context.Context {
	if meta == nil || len(rows) == 0 {
		return ctx
	}
	return context.WithValue(ctx, bulkUpsertDumpContextKey{}, &BulkUpsertDumpContext{
		Meta:      meta,
		TablePath: tablePath,
		Rows:      rows,
	})
}

func bulkUpsertDumpContextFrom(ctx context.Context) *BulkUpsertDumpContext {
	if ctx == nil {
		return nil
	}
	dc, _ := ctx.Value(bulkUpsertDumpContextKey{}).(*BulkUpsertDumpContext)
	return dc
}

// tryDumpBulkUpsertOnIssues writes the chunk once when BulkUpsert emits YQL issues.
func tryDumpBulkUpsertOnIssues(ctx context.Context, dir, issues string, err error) {
	if dir == "" || issues == "" {
		return
	}
	dc := bulkUpsertDumpContextFrom(ctx)
	if dc == nil {
		return
	}
	dc.mu.Lock()
	defer dc.mu.Unlock()
	if dc.dumped {
		return
	}
	path, dumpErr := DumpBulkUpsertChunk(dir, dc.TablePath, dc.Meta, dc.Rows, err, issues)
	if dumpErr != nil {
		log.Printf("  [ydb] %s: failed to dump BulkUpsert chunk on issues: %v", dc.Meta.Name, dumpErr)
		return
	}
	dc.dumped = true
	log.Printf("  [ydb] %s: dumped BulkUpsert chunk on issues (%d rows) to %s", dc.Meta.Name, len(dc.Rows), path)
}

// bulkUpsertFailureDump is written to disk when BulkUpsert emits YQL issues so the chunk can be inspected offline.
type bulkUpsertFailureDump struct {
	Table     string                   `json:"table"`
	TablePath string                   `json:"table_path"`
	RowCount  int                      `json:"row_count"`
	Error     string                   `json:"error,omitempty"`
	Issues    string                   `json:"issues,omitempty"`
	Columns   []schema.Column          `json:"columns"`
	Rows      []map[string]interface{} `json:"rows"`
	DumpedAt  time.Time                `json:"dumped_at"`
}

// DumpBulkUpsertChunk writes the chunk and issue details to dir as JSON. Returns the path of the created file.
func DumpBulkUpsertChunk(dir, tablePath string, meta *schema.TableMeta, rows []map[string]interface{}, err error, issues string) (string, error) {
	if dir == "" {
		return "", fmt.Errorf("dump directory is empty")
	}
	if meta == nil {
		return "", fmt.Errorf("table meta is nil")
	}
	if issues == "" && err == nil {
		return "", fmt.Errorf("issues and err are both empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create dump directory: %w", err)
	}
	dump := bulkUpsertFailureDump{
		Table:     meta.Name,
		TablePath: tablePath,
		RowCount:  len(rows),
		Issues:    issues,
		Columns:   meta.Columns,
		Rows:      rows,
		DumpedAt:  time.Now().UTC(),
	}
	if err != nil {
		dump.Error = err.Error()
		if dump.Issues == "" {
			dump.Issues = IssuesString(err)
		}
	}
	data, err := json.MarshalIndent(dump, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal dump: %w", err)
	}
	name := fmt.Sprintf("bulkupsert-issues-%s-%d.json", sanitizeDumpFileComponent(meta.Name), time.Now().UnixNano())
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return "", fmt.Errorf("write dump file: %w", err)
	}
	return path, nil
}

func sanitizeDumpFileComponent(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "table"
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

func isBulkUpsertDriverMethod(method string) bool {
	return strings.Contains(method, "BulkUpsert")
}
