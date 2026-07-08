package ydb

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mysql2ydb/mysql2ydb/internal/schema"
)

func TestSanitizeDumpFileComponent(t *testing.T) {
	cases := map[string]string{
		"users":      "users",
		"my/table":   "my_table",
		"  spaced  ": "spaced",
		"":           "table",
		"foo-bar_1":  "foo-bar_1",
	}
	for in, want := range cases {
		if got := sanitizeDumpFileComponent(in); got != want {
			t.Errorf("sanitizeDumpFileComponent(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestIsBulkUpsertDriverMethod(t *testing.T) {
	if !isBulkUpsertDriverMethod("/Ydb.Table.V1.TableService/BulkUpsert") {
		t.Fatal("expected BulkUpsert grpc method to match")
	}
	if isBulkUpsertDriverMethod("/Ydb.Table.V1.TableService/DescribeTable") {
		t.Fatal("DescribeTable must not match")
	}
}

func TestDumpBulkUpsertChunk(t *testing.T) {
	dir := t.TempDir()
	meta := &schema.TableMeta{
		Name: "orders",
		Columns: []schema.Column{
			{Name: "id", DataType: "bigint", Nullable: false, PrimaryKey: true},
			{Name: "note", DataType: "text", Nullable: true},
		},
	}
	rows := []map[string]interface{}{
		{"id": int64(1), "note": "hello"},
		{"id": int64(2), "note": []byte("binary")},
	}
	issues := "  [ERROR] (code=0) Bulk upsert to table '/local/orders' Failed to connect to shard"

	path, err := DumpBulkUpsertChunk(dir, "/local/orders", meta, rows, errors.New("transport"), issues)
	if err != nil {
		t.Fatalf("DumpBulkUpsertChunk: %v", err)
	}
	if !strings.HasPrefix(filepath.Base(path), "bulkupsert-issues-orders-") {
		t.Fatalf("unexpected dump file name: %s", filepath.Base(path))
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read dump: %v", err)
	}
	var got bulkUpsertFailureDump
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal dump: %v", err)
	}
	if got.Table != "orders" || got.TablePath != "/local/orders" || got.RowCount != 2 {
		t.Fatalf("unexpected dump metadata: %+v", got)
	}
	if got.Issues != issues {
		t.Fatalf("dump issues = %q, want %q", got.Issues, issues)
	}
	if len(got.Columns) != 2 || len(got.Rows) != 2 {
		t.Fatalf("unexpected columns/rows in dump: columns=%d rows=%d", len(got.Columns), len(got.Rows))
	}
	if got.DumpedAt.IsZero() || time.Since(got.DumpedAt) > time.Minute {
		t.Fatalf("unexpected dumped_at: %v", got.DumpedAt)
	}
}

func TestTryDumpBulkUpsertOnIssues_OncePerChunk(t *testing.T) {
	dir := t.TempDir()
	meta := &schema.TableMeta{Name: "orders", Columns: []schema.Column{{Name: "id", DataType: "bigint"}}}
	rows := []map[string]interface{}{{"id": int64(1)}}
	ctx := WithBulkUpsertDumpContext(context.Background(), meta, "/local/orders", rows)
	issues := "  [ERROR] (code=0) Bulk upsert failed"

	tryDumpBulkUpsertOnIssues(ctx, dir, issues, errors.New("fail"))
	tryDumpBulkUpsertOnIssues(ctx, dir, issues, errors.New("fail again"))

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dump dir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected one dump file, got %d", len(entries))
	}
}

func TestDumpBulkUpsertChunk_Validation(t *testing.T) {
	meta := &schema.TableMeta{Name: "t"}
	rows := []map[string]interface{}{{"id": 1}}
	issues := "  [ERROR] (code=0) issue"

	if _, err := DumpBulkUpsertChunk("", "/t", meta, rows, nil, issues); err == nil {
		t.Fatal("expected error for empty dir")
	}
	if _, err := DumpBulkUpsertChunk(t.TempDir(), "/t", nil, rows, nil, issues); err == nil {
		t.Fatal("expected error for nil meta")
	}
	if _, err := DumpBulkUpsertChunk(t.TempDir(), "/t", meta, rows, nil, ""); err == nil {
		t.Fatal("expected error for empty issues and nil err")
	}
}
