package ydb

import (
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
		"users":       "users",
		"my/table":    "my_table",
		"  spaced  ":  "spaced",
		"":            "table",
		"foo-bar_1":   "foo-bar_1",
	}
	for in, want := range cases {
		if got := sanitizeDumpFileComponent(in); got != want {
			t.Errorf("sanitizeDumpFileComponent(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestDumpBulkUpsertFailure(t *testing.T) {
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
	dumpErr := errors.New("BulkUpsert failed: BAD_REQUEST")

	path, err := DumpBulkUpsertFailure(dir, "/local/orders", meta, rows, dumpErr)
	if err != nil {
		t.Fatalf("DumpBulkUpsertFailure: %v", err)
	}
	if !strings.HasPrefix(filepath.Base(path), "bulkupsert-failure-orders-") {
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
	if got.Error != dumpErr.Error() {
		t.Fatalf("dump error = %q, want %q", got.Error, dumpErr.Error())
	}
	if len(got.Columns) != 2 || len(got.Rows) != 2 {
		t.Fatalf("unexpected columns/rows in dump: columns=%d rows=%d", len(got.Columns), len(got.Rows))
	}
	if got.DumpedAt.IsZero() || time.Since(got.DumpedAt) > time.Minute {
		t.Fatalf("unexpected dumped_at: %v", got.DumpedAt)
	}
}

func TestDumpBulkUpsertFailure_Validation(t *testing.T) {
	meta := &schema.TableMeta{Name: "t"}
	rows := []map[string]interface{}{{"id": 1}}
	err := errors.New("fail")

	if _, err := DumpBulkUpsertFailure("", "/t", meta, rows, err); err == nil {
		t.Fatal("expected error for empty dir")
	}
	if _, err := DumpBulkUpsertFailure(t.TempDir(), "/t", nil, rows, err); err == nil {
		t.Fatal("expected error for nil meta")
	}
	if _, err := DumpBulkUpsertFailure(t.TempDir(), "/t", meta, rows, nil); err == nil {
		t.Fatal("expected error for nil err")
	}
}
