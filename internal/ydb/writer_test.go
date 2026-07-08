package ydb

import (
	"strings"
	"testing"

	"github.com/mysql2ydb/mysql2ydb/internal/schema"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestPartitionRowsBySize(t *testing.T) {
	meta := &schema.TableMeta{
		Columns: []schema.Column{
			{Name: "id", DataType: "bigint", Nullable: false},
			{Name: "payload", DataType: "text", Nullable: false},
		},
	}
	row := map[string]interface{}{
		"id":      int64(1),
		"payload": strings.Repeat("x", 1024),
	}
	rows := make([]map[string]interface{}, 100)
	for i := range rows {
		rows[i] = row
	}

	single := partitionRowsBySize(rows[:1], meta, maxChunkPayloadBytes)
	if len(single) != 1 || len(single[0]) != 1 {
		t.Fatalf("expected single chunk for one row, got %#v", single)
	}

	chunks := partitionRowsBySize(rows, meta, estimateRowSizeBytes(row, meta)*10)
	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(chunks))
	}
	totalRows := 0
	for _, chunk := range chunks {
		totalRows += len(chunk)
		var size int
		for _, r := range chunk {
			size += estimateRowSizeBytes(r, meta)
		}
		if size > estimateRowSizeBytes(row, meta)*10 {
			t.Fatalf("chunk size %d exceeds limit", size)
		}
	}
	if totalRows != len(rows) {
		t.Fatalf("expected %d rows across chunks, got %d", len(rows), totalRows)
	}
}

func TestMysqlValueToYDB(t *testing.T) {
	meta := &schema.TableMeta{
		Columns: []schema.Column{
			{Name: "id", DataType: "bigint", Nullable: false},
			{Name: "name", DataType: "text", Nullable: true},
		},
	}
	_ = meta
	tests := []struct {
		raw      interface{}
		dataType string
		nullable bool
		wantErr  bool
	}{
		{nil, "bigint", true, false},
		{int64(42), "bigint", false, false},
		{"hello", "text", false, false},
		{[]byte("bytes"), "text", false, false},
		{float64(3.14), "double", false, false},
	}
	for _, tt := range tests {
		v, err := mysqlValueToYDB(tt.raw, tt.dataType, tt.nullable)
		if (err != nil) != tt.wantErr {
			t.Errorf("mysqlValueToYDB(%v, %q) err = %v", tt.raw, tt.dataType, err)
			continue
		}
		if !tt.wantErr && v == nil && tt.raw != nil {
			t.Errorf("mysqlValueToYDB(%v) returned nil value", tt.raw)
		}
	}
}

func TestYdbTypeFromMySQL(t *testing.T) {
	if types.TypeInt64 != ydbTypeFromMySQL("bigint") {
		t.Error("bigint should map to TypeInt64")
	}
	if types.TypeText != ydbTypeFromMySQL("varchar") {
		t.Error("varchar should map to TypeText")
	}
}
