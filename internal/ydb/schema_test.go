package ydb

import (
	"strings"
	"testing"

	"github.com/mysql2ydb/mysql2ydb/internal/schema"
)

func TestBuildCreateTableDDL_NoIndexes(t *testing.T) {
	meta := &schema.TableMeta{
		Name: "users",
		Columns: []schema.Column{
			{Name: "id", DataType: "bigint", Nullable: false},
			{Name: "name", DataType: "text", Nullable: true},
		},
		PKCols: []string{"id"},
	}
	ddl := buildCreateTableDDL("/db/users", meta)
	if !strings.Contains(ddl, "PRIMARY KEY (`id`)") {
		t.Errorf("DDL should contain PRIMARY KEY: %s", ddl)
	}
	if strings.Contains(ddl, "INDEX ") {
		t.Errorf("DDL should not contain INDEX when no indexes: %s", ddl)
	}
}

func TestBuildCreateTableDDL_WithAsyncIndex(t *testing.T) {
	meta := &schema.TableMeta{
		Name: "users",
		Columns: []schema.Column{
			{Name: "id", DataType: "bigint", Nullable: false},
			{Name: "email", DataType: "text", Nullable: false},
		},
		PKCols: []string{"id"},
		Indexes: []schema.IndexInfo{
			{Name: "idx_email", Columns: []string{"email"}, Unique: false},
		},
	}
	ddl := buildCreateTableDDL("/db/users", meta)
	if !strings.Contains(ddl, "INDEX `idx_email` GLOBAL ASYNC ON (`email`)") {
		t.Errorf("DDL should contain ASYNC index on email: %s", ddl)
	}
	if !strings.Contains(ddl, "PRIMARY KEY (`id`)") {
		t.Errorf("DDL should contain PRIMARY KEY: %s", ddl)
	}
}

func TestBuildCreateTableDDL_WithUniqueIndex(t *testing.T) {
	meta := &schema.TableMeta{
		Name: "users",
		Columns: []schema.Column{
			{Name: "id", DataType: "bigint", Nullable: false},
			{Name: "login", DataType: "text", Nullable: false},
		},
		PKCols: []string{"id"},
		Indexes: []schema.IndexInfo{
			{Name: "uniq_login", Columns: []string{"login"}, Unique: true},
		},
	}
	ddl := buildCreateTableDDL("/db/users", meta)
	if !strings.Contains(ddl, "INDEX `uniq_login` GLOBAL UNIQUE SYNC ON (`login`)") {
		t.Errorf("DDL should contain UNIQUE SYNC index: %s", ddl)
	}
}

func TestBuildCreateTableDDL_IndexDuplicateOfPK_Skipped(t *testing.T) {
	meta := &schema.TableMeta{
		Name: "users",
		Columns: []schema.Column{
			{Name: "id", DataType: "bigint", Nullable: false},
		},
		PKCols: []string{"id"},
		Indexes: []schema.IndexInfo{
			{Name: "PRIMARY", Columns: []string{"id"}, Unique: true}, // same as PK — should be skipped
		},
	}
	ddl := buildCreateTableDDL("/db/users", meta)
	// Only one occurrence of "id" in key definition should be PRIMARY KEY
	if strings.Count(ddl, "INDEX ") > 0 {
		t.Errorf("index that equals PK should be skipped, DDL: %s", ddl)
	}
	if !strings.Contains(ddl, "PRIMARY KEY (`id`)") {
		t.Errorf("DDL should contain PRIMARY KEY: %s", ddl)
	}
}

func TestBuildCreateTableDDL_MultipleIndexes(t *testing.T) {
	meta := &schema.TableMeta{
		Name: "orders",
		Columns: []schema.Column{
			{Name: "id", DataType: "bigint", Nullable: false},
			{Name: "user_id", DataType: "bigint", Nullable: false},
			{Name: "created_at", DataType: "datetime", Nullable: false},
		},
		PKCols: []string{"id"},
		Indexes: []schema.IndexInfo{
			{Name: "idx_user", Columns: []string{"user_id"}, Unique: false},
			{Name: "idx_user_created", Columns: []string{"user_id", "created_at"}, Unique: false},
		},
	}
	ddl := buildCreateTableDDL("/db/orders", meta)
	if !strings.Contains(ddl, "INDEX `idx_user` GLOBAL ASYNC ON (`user_id`)") {
		t.Errorf("DDL should contain idx_user: %s", ddl)
	}
	if !strings.Contains(ddl, "INDEX `idx_user_created` GLOBAL ASYNC ON (`user_id`, `created_at`)") {
		t.Errorf("DDL should contain idx_user_created: %s", ddl)
	}
}
