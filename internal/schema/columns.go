package schema

import (
	"database/sql"
	"fmt"
	"strings"
)

// Column describes a table column (MySQL and YDB).
type Column struct {
	Name         string
	DataType     string // MySQL: INT, BIGINT, VARCHAR, etc.
	Nullable     bool
	PrimaryKey   bool
	AutoIncrement bool // MySQL AUTO_INCREMENT; in YDB only documented in DDL comment
}

// IndexInfo describes a secondary index (KEY or UNIQUE KEY), not PRIMARY.
type IndexInfo struct {
	Name    string
	Columns []string // ordered column names
	Unique  bool
}

// TableMeta describes table structure for migration.
type TableMeta struct {
	Name               string
	Columns            []Column
	PKCols             []string   // ordered primary key column names for cursor pagination
	Indexes            []IndexInfo // secondary indexes from STATISTICS (excl. PRIMARY)
	AutoIncrementNext  uint64
}

// LoadTableMeta reads table structure from MySQL information_schema.
func LoadTableMeta(db *sql.DB, tableName string) (*TableMeta, error) {
	// COLUMN_TYPE gives e.g. "bigint unsigned", "int(11)"; EXTRA contains "auto_increment"
	rows, err := db.Query(`
		SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, EXTRA
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`, tableName)
	if err != nil {
		return nil, fmt.Errorf("columns query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var cols []Column
	var pkCols []string
	for rows.Next() {
		var name, columnType, nullable, key, extra string
		if err := rows.Scan(&name, &columnType, &nullable, &key, &extra); err != nil {
			return nil, err
		}
		dataType := normalizeMySQLType(columnType)
		col := Column{
			Name:          name,
			DataType:      dataType,
			Nullable:      nullable == "YES",
			PrimaryKey:    key == "PRI",
			AutoIncrement: strings.Contains(strings.ToLower(extra), "auto_increment"),
		}
		cols = append(cols, col)
		if col.PrimaryKey {
			pkCols = append(pkCols, name)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	meta := &TableMeta{Name: tableName, Columns: cols, PKCols: pkCols}
	// Load secondary indexes from information_schema.STATISTICS (exclude PRIMARY)
	idxRows, err := db.Query(`
		SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX, NON_UNIQUE
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME != 'PRIMARY'
		ORDER BY INDEX_NAME, SEQ_IN_INDEX
	`, tableName)
	if err == nil {
		defer func() { _ = idxRows.Close() }()
		indexMap := make(map[string]*IndexInfo)
		for idxRows.Next() {
			var idxName, colName string
			var seq int
			var nonUnique int
			if idxRows.Scan(&idxName, &colName, &seq, &nonUnique) != nil {
				break
			}
			if indexMap[idxName] == nil {
				indexMap[idxName] = &IndexInfo{Name: idxName, Unique: nonUnique == 0}
			}
			indexMap[idxName].Columns = append(indexMap[idxName].Columns, colName)
		}
		for _, idx := range indexMap {
			meta.Indexes = append(meta.Indexes, *idx)
		}
	}
	// Load AUTO_INCREMENT next value from information_schema.TABLES (for ALTER SEQUENCE in YDB)
	var ai sql.NullInt64
	err = db.QueryRow(`
		SELECT AUTO_INCREMENT FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
	`, tableName).Scan(&ai)
	if err == nil && ai.Valid && ai.Int64 > 0 {
		meta.AutoIncrementNext = uint64(ai.Int64)
	}
	return meta, nil
}

// TableSize returns approximate data length (bytes) and row count for the table from information_schema.
// Values are estimates; TABLE_ROWS can be NULL for some engines.
func TableSize(db *sql.DB, tableName string) (dataLength uint64, rowCount uint64, err error) {
	var dl, rc sql.NullInt64
	err = db.QueryRow(`
		SELECT COALESCE(DATA_LENGTH, 0), TABLE_ROWS
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
	`, tableName).Scan(&dl, &rc)
	if err != nil {
		return 0, 0, err
	}
	if dl.Valid {
		dataLength = uint64(dl.Int64)
	}
	if rc.Valid && rc.Int64 > 0 {
		rowCount = uint64(rc.Int64)
	}
	return dataLength, rowCount, nil
}

// TableNames returns list of table names in the current database.
func TableNames(db *sql.DB) ([]string, error) {
	rows, err := db.Query(`
		SELECT TABLE_NAME FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'
		ORDER BY TABLE_NAME
	`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		names = append(names, n)
	}
	return names, rows.Err()
}

// normalizeMySQLType maps COLUMN_TYPE to a short type key for YDB mapping.
func normalizeMySQLType(columnType string) string {
	// COLUMN_TYPE is e.g. "bigint unsigned", "int(11)", "varchar(255)"
	switch {
	case strings.Contains(columnType, "bigint unsigned"):
		return "bigint unsigned"
	case strings.Contains(columnType, "int unsigned"), strings.Contains(columnType, "mediumint unsigned"),
		strings.Contains(columnType, "smallint unsigned"), strings.Contains(columnType, "tinyint unsigned"):
		return "int unsigned"
	case strings.HasPrefix(columnType, "bigint"):
		return "bigint"
	case columnType == "tinyint(1)" || strings.HasPrefix(columnType, "tinyint(1)"):
		return "bool" // MySQL convention: TINYINT(1) = boolean
	case strings.HasPrefix(columnType, "int"), strings.HasPrefix(columnType, "mediumint"),
		strings.HasPrefix(columnType, "smallint"), strings.HasPrefix(columnType, "tinyint"):
		return "int"
	case strings.HasPrefix(columnType, "float"):
		return "float"
	case strings.HasPrefix(columnType, "double"), strings.HasPrefix(columnType, "real"):
		return "double"
	case strings.HasPrefix(columnType, "datetime"):
		return "datetime"
	case strings.HasPrefix(columnType, "timestamp"):
		return "timestamp"
	case strings.HasPrefix(columnType, "date"):
		return "date"
	case strings.HasPrefix(columnType, "year"):
		return "year"
	case strings.HasPrefix(columnType, "char"), strings.HasPrefix(columnType, "varchar"),
		strings.HasPrefix(columnType, "text"), strings.HasPrefix(columnType, "json"),
		strings.HasPrefix(columnType, "enum"), strings.HasPrefix(columnType, "set"):
		return "text"
	case strings.HasPrefix(columnType, "binary"), strings.HasPrefix(columnType, "varbinary"),
		strings.HasPrefix(columnType, "blob"):
		return "bytes"
	case strings.HasPrefix(columnType, "bit"):
		return "bit"
	case strings.HasPrefix(columnType, "decimal"), strings.HasPrefix(columnType, "numeric"):
		return "decimal"
	default:
		return "text"
	}
}
