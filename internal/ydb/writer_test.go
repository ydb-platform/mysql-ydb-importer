package ydb

import (
	"testing"

	"github.com/mysql2ydb/mysql2ydb/internal/schema"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

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
