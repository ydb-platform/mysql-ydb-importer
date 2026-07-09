package ydb

import (
	"context"
	"fmt"
	"log"
	"path"
	"strings"

	"github.com/mysql2ydb/mysql2ydb/internal/schema"
)

// QueryExecer runs YQL (DDL/DML) via Query service. Used for CREATE/DROP TABLE.
type QueryExecer interface {
	Exec(ctx context.Context, query string) error
}

// QueryExecFunc adapts a function to QueryExecer (e.g. driver.Query().Exec ignoring opts).
type QueryExecFunc func(ctx context.Context, query string) error

func (f QueryExecFunc) Exec(ctx context.Context, query string) error { return f(ctx, query) }

// DropTable drops a table in YDB via Query service (DROP TABLE IF EXISTS). Idempotent.
func DropTable(ctx context.Context, q QueryExecer, database string, tableName string) error {
	tablePath := path.Join(database, tableName)
	quotedPath := quoteYQLPath(tablePath)
	return q.Exec(ctx, "DROP TABLE IF EXISTS "+quotedPath)
}

// CreateTable creates a table in YDB via Query service (CREATE TABLE ... WITH AUTO_PARTITIONING_BY_LOAD = ENABLED).
// When includeIndexes is false, only columns and PRIMARY KEY are created; secondary indexes are added later via CreateSecondaryIndexes.
func CreateTable(ctx context.Context, q QueryExecer, database string, meta *schema.TableMeta, includeIndexes bool) error {
	tablePath := path.Join(database, meta.Name)
	if err := q.Exec(ctx, buildCreateTableDDL(tablePath, meta, includeIndexes)); err != nil {
		return err
	}
	if meta.AutoIncrementNext > 0 {
		return AlterSequence(ctx, q, database, meta.Name, meta.AutoIncrementNext)
	}
	return nil
}

// CreateSecondaryIndexes adds secondary indexes from MySQL metadata via ALTER TABLE ADD INDEX.
// Skips indexes that duplicate the primary key. No-op when there are no secondary indexes.
func CreateSecondaryIndexes(ctx context.Context, q QueryExecer, database string, meta *schema.TableMeta) error {
	tablePath := path.Join(database, meta.Name)
	indexes := SecondaryIndexes(meta)
	if len(indexes) == 0 {
		return nil
	}
	for _, idx := range indexes {
		ddl := buildAddIndexDDL(tablePath, idx)
		log.Printf("  [ydb] %s: %s", meta.Name, strings.TrimSpace(ddl))
		if err := q.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("add index %s on %s: %w", idx.Name, meta.Name, err)
		}
	}
	return nil
}

// SecondaryIndexes returns MySQL secondary indexes that should be created in YDB (excludes PK duplicates).
func SecondaryIndexes(meta *schema.TableMeta) []schema.IndexInfo {
	var out []schema.IndexInfo
	for _, idx := range meta.Indexes {
		if indexEqualsPK(idx.Columns, meta.PKCols) {
			continue
		}
		out = append(out, idx)
	}
	return out
}

// AlterSequence sets the next value for a table's serial column (YDB sequence).
// Path format as in mysql-to-ydb-converter: <database>/<table>/_serial_column_id.
func AlterSequence(ctx context.Context, q QueryExecer, database string, tableName string, startWith uint64) error {
	seqPath := path.Join(database, tableName, "_serial_column_id")
	quotedPath := quoteYQLPath(seqPath)
	return q.Exec(ctx, fmt.Sprintf("ALTER SEQUENCE %s START WITH %d RESTART", quotedPath, startWith))
}

// buildCreateTableDDL returns YQL CREATE TABLE statement with optional columns and AUTO_PARTITIONING_BY_LOAD.
// AUTO_INCREMENT columns from MySQL become BigSerial NOT NULL in YDB (sequence per table).
func buildCreateTableDDL(tablePath string, meta *schema.TableMeta, includeIndexes bool) string {
	quotedPath := quoteYQLPath(tablePath)
	var parts []string
	for _, col := range meta.Columns {
		var line string
		if col.AutoIncrement {
			line = fmt.Sprintf("%s BigSerial NOT NULL", quoteYQLName(col.Name))
		} else {
			yqlType := mysqlDataTypeToYQL(col.DataType, col.Nullable)
			line = fmt.Sprintf("%s %s", quoteYQLName(col.Name), yqlType)
		}
		parts = append(parts, line)
	}
	if includeIndexes {
		for _, idx := range SecondaryIndexes(meta) {
			parts = append(parts, buildInlineIndexDef(idx))
		}
	}
	pkList := make([]string, len(meta.PKCols))
	for i, pk := range meta.PKCols {
		pkList[i] = quoteYQLName(pk)
	}
	parts = append(parts, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pkList, ", ")))
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n\t%s\n)\nWITH (\n\tAUTO_PARTITIONING_BY_LOAD = ENABLED\n)",
		quotedPath, strings.Join(parts, ",\n\t"))
}

func buildInlineIndexDef(idx schema.IndexInfo) string {
	quotedCols := make([]string, len(idx.Columns))
	for i, c := range idx.Columns {
		quotedCols[i] = quoteYQLName(c)
	}
	return fmt.Sprintf("INDEX %s GLOBAL %s ON (%s)", quoteYQLName(idx.Name), indexKind(idx.Unique), strings.Join(quotedCols, ", "))
}

func buildAddIndexDDL(tablePath string, idx schema.IndexInfo) string {
	quotedCols := make([]string, len(idx.Columns))
	for i, c := range idx.Columns {
		quotedCols[i] = quoteYQLName(c)
	}
	return fmt.Sprintf("ALTER TABLE %s ADD INDEX %s GLOBAL %s ON (%s);",
		quoteYQLPath(tablePath), quoteYQLName(idx.Name), indexKind(idx.Unique), strings.Join(quotedCols, ", "))
}

func quoteYQLPath(p string) string {
	return "`" + strings.ReplaceAll(p, "`", "``") + "`"
}

func quoteYQLName(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func indexKind(unique bool) string {
	if unique {
		return "UNIQUE SYNC"
	}
	return "ASYNC"
}

func indexEqualsPK(cols, pkCols []string) bool {
	if len(cols) != len(pkCols) {
		return false
	}
	for i, c := range cols {
		if i >= len(pkCols) || c != pkCols[i] {
			return false
		}
	}
	return true
}

// mysqlDataTypeToYQL returns YQL type name (e.g. Optional<Int32>, Utf8).
func mysqlDataTypeToYQL(dataType string, nullable bool) string {
	base := mysqlTypeToYQLBase(dataType)
	if nullable {
		return "Optional<" + base + ">"
	}
	return base
}

func mysqlTypeToYQLBase(dataType string) string {
	switch dataType {
	case "bool":
		return "Bool"
	case "int", "mediumint", "smallint", "tinyint":
		return "Int32"
	case "bigint":
		return "Int64"
	case "int unsigned", "mediumint unsigned", "smallint unsigned", "tinyint unsigned":
		return "Uint32"
	case "bigint unsigned":
		return "Uint64"
	case "float":
		return "Float"
	case "double", "real":
		return "Double"
	case "date":
		return "Date"
	case "datetime", "timestamp":
		return "Timestamp"
	case "year":
		return "Uint16"
	// Text and Utf8 are the same in YDB.
	case "char", "varchar", "text", "mediumtext", "longtext", "json", "enum", "set":
		return "Text"
	// All MySQL binary/blob types map to YDB String (bytes). Schema uses "bytes" from normalizeMySQLType for any blob/binary.
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob", "bytes":
		return "String"
	case "bit":
		return "Uint64"
	case "decimal", "numeric":
		return "Decimal(22, 9)"
	default:
		return "Text"
	}
}
