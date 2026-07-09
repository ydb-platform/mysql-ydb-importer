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
	ddl := buildCreateTableDDL("/db/users", meta, true)
	if !strings.Contains(ddl, "PRIMARY KEY (`id`)") {
		t.Errorf("DDL should contain PRIMARY KEY: %s", ddl)
	}
	if strings.Contains(ddl, "INDEX ") {
		t.Errorf("DDL should not contain INDEX when no indexes: %s", ddl)
	}
}

func TestBuildCreateTableDDL_DeferredIndexes(t *testing.T) {
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
	ddl := buildCreateTableDDL("/db/users", meta, false)
	if strings.Contains(ddl, "INDEX ") {
		t.Errorf("DDL should not contain INDEX when includeIndexes=false: %s", ddl)
	}
	addIdx := buildAddIndexDDL("/db/users", meta.Indexes[0])
	if !strings.Contains(addIdx, "ALTER TABLE `/db/users` ADD INDEX `idx_email` GLOBAL ASYNC ON (`email`);") {
		t.Errorf("unexpected ADD INDEX DDL: %s", addIdx)
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
	ddl := buildCreateTableDDL("/db/users", meta, true)
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
	ddl := buildCreateTableDDL("/db/users", meta, true)
	if !strings.Contains(ddl, "INDEX `uniq_login` GLOBAL UNIQUE SYNC ON (`login`)") {
		t.Errorf("DDL should contain UNIQUE SYNC index: %s", ddl)
	}
	addIdx := buildAddIndexDDL("/db/users", meta.Indexes[0])
	if !strings.Contains(addIdx, "GLOBAL UNIQUE SYNC") {
		t.Errorf("ADD INDEX should be UNIQUE SYNC: %s", addIdx)
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
	ddl := buildCreateTableDDL("/db/users", meta, true)
	if strings.Count(ddl, "INDEX ") > 0 {
		t.Errorf("index that equals PK should be skipped, DDL: %s", ddl)
	}
	if !strings.Contains(ddl, "PRIMARY KEY (`id`)") {
		t.Errorf("DDL should contain PRIMARY KEY: %s", ddl)
	}
	if len(SecondaryIndexes(meta)) != 0 {
		t.Error("SecondaryIndexes should skip PK duplicate")
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
	ddl := buildCreateTableDDL("/db/orders", meta, true)
	if !strings.Contains(ddl, "INDEX `idx_user` GLOBAL ASYNC ON (`user_id`)") {
		t.Errorf("DDL should contain idx_user: %s", ddl)
	}
	if !strings.Contains(ddl, "INDEX `idx_user_created` GLOBAL ASYNC ON (`user_id`, `created_at`)") {
		t.Errorf("DDL should contain idx_user_created: %s", ddl)
	}
}
