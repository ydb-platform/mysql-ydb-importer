package ydb

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mysql2ydb/mysql2ydb/internal/schema"
)

// bulkUpsertFailureDump is written to disk when BulkUpsert fails so the chunk can be inspected offline.
type bulkUpsertFailureDump struct {
	Table     string                   `json:"table"`
	TablePath string                   `json:"table_path"`
	RowCount  int                      `json:"row_count"`
	Error     string                   `json:"error"`
	Issues    string                   `json:"issues,omitempty"`
	Columns   []schema.Column          `json:"columns"`
	Rows      []map[string]interface{} `json:"rows"`
	DumpedAt  time.Time                `json:"dumped_at"`
}

// DumpBulkUpsertFailure writes the failed chunk and error details to dir as JSON.
// Returns the path of the created file.
func DumpBulkUpsertFailure(dir, tablePath string, meta *schema.TableMeta, rows []map[string]interface{}, err error) (string, error) {
	if dir == "" {
		return "", fmt.Errorf("dump directory is empty")
	}
	if meta == nil {
		return "", fmt.Errorf("table meta is nil")
	}
	if err == nil {
		return "", fmt.Errorf("err is nil")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create dump directory: %w", err)
	}
	dump := bulkUpsertFailureDump{
		Table:     meta.Name,
		TablePath: tablePath,
		RowCount:  len(rows),
		Error:     err.Error(),
		Issues:    IssuesString(err),
		Columns:   meta.Columns,
		Rows:      rows,
		DumpedAt:  time.Now().UTC(),
	}
	data, err := json.MarshalIndent(dump, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal dump: %w", err)
	}
	name := fmt.Sprintf("bulkupsert-failure-%s-%d.json", sanitizeDumpFileComponent(meta.Name), time.Now().UnixNano())
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
