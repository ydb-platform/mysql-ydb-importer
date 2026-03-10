package ydb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"
)

// StateTableName is the single YDB table for migration state: progress (resume cursor, rows_so_far) and completion (completed_at, row_count).
const StateTableName = "_mysql2ydb_state"

// StateTablePath returns the full YDB path for the state table (same for DDL and DML).
func StateTablePath(database string) string {
	return path.Join(database, StateTableName)
}

// StateTableExists returns true if the state table already exists (schema was created in a previous run).
func StateTableExists(ctx context.Context, db TableClient, database string) (bool, error) {
	tablePath := StateTablePath(database)
	var err error
	err = db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		_, err = s.DescribeTable(ctx, tablePath)
		return err
	}, table.WithIdempotent())
	if err == nil {
		return true, nil
	}
	// Table does not exist: path not found, SCHEME_ERROR (400070), or similar.
	errStr := err.Error()
	if strings.Contains(errStr, "not found") || strings.Contains(errStr, "NotFound") ||
		strings.Contains(errStr, "Path does not exist") || strings.Contains(errStr, "SCHEME_ERROR") {
		return false, nil
	}
	return false, err
}

// CreateStateTable creates the migration state table via Query (DDL) if it does not exist. Idempotent.
// One table holds both in-progress state (resume_cursor, rows_so_far) and completion (completed_at != 0).
func CreateStateTable(ctx context.Context, q QueryExecer, database string) error {
	tablePath := StateTablePath(database)
	quotedPath := "`" + strings.ReplaceAll(tablePath, "`", "``") + "`"
	ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n"+
		"  table_name Utf8 NOT NULL,\n"+
		"  resume_cursor Utf8 NOT NULL,\n"+
		"  rows_so_far Uint64 NOT NULL,\n"+
		"  updated_at Datetime NOT NULL,\n"+
		"  completed_at Datetime NOT NULL,\n"+
		"  PRIMARY KEY (table_name)\n"+
		")\nWITH (\n  AUTO_PARTITIONING_BY_LOAD = ENABLED\n)", quotedPath)
	return q.Exec(ctx, ddl)
}

// epochZero is the sentinel for "in progress" (completed_at = 0 in Datetime).
var epochZero = time.Unix(0, 0).UTC()

// GetCompletedTables returns table names that are already recorded as migrated (completed_at > epoch).
func GetCompletedTables(ctx context.Context, db Client, database string) ([]string, error) {
	tablePath := StateTablePath(database)
	queryText := fmt.Sprintf(`
		DECLARE $min_completed AS Datetime;
		SELECT table_name FROM %s WHERE completed_at > $min_completed
	`, quoteTablePath(tablePath))
	var out []string
	err := db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		res, err := tx.Query(ctx, queryText,
			query.WithParameters(ydbsdk.ParamsBuilder().Param("$min_completed").Datetime(epochZero).Build()),
		)
		if err != nil {
			return err
		}
		defer func() { _ = res.Close(ctx) }()
		for rs, err := range res.ResultSets(ctx) {
			if err != nil {
				return err
			}
			for row, err := range rs.Rows(ctx) {
				if err != nil {
					return err
				}
				var name string
				if err := row.ScanNamed(query.Named("table_name", &name)); err != nil {
					return err
				}
				out = append(out, name)
			}
		}
		return nil
	}, query.WithIdempotent(), query.WithTxSettings(query.TxSettings(query.WithSnapshotReadOnly())))
	return out, err
}

// MarkTableCompleted records that the table was successfully migrated (BulkUpsert one row, completed_at = now).
func MarkTableCompleted(ctx context.Context, db TableClient, database string, tableName string, rowCount int) error {
	now := time.Now().UTC()
	row := types.StructValue(
		types.StructFieldValue("table_name", types.TextValue(tableName)),
		types.StructFieldValue("resume_cursor", types.TextValue("")),
		types.StructFieldValue("rows_so_far", types.Uint64Value(uint64(rowCount))),
		types.StructFieldValue("updated_at", types.DatetimeValueFromTime(now)),
		types.StructFieldValue("completed_at", types.DatetimeValueFromTime(now)),
	)
	tablePath := StateTablePath(database)
	list := types.ListValue(row)
	return db.Table().BulkUpsert(ctx, tablePath, table.BulkUpsertDataRows(list), table.WithIdempotent())
}

func quoteTablePath(p string) string {
	return "`" + strings.ReplaceAll(p, "`", "``") + "`"
}

// SaveProgress records progress after a chunk in the state table via BulkUpsert (same API as user tables, ensures visibility).
func SaveProgress(ctx context.Context, db TableClient, database string, tableName string, nextCursor []interface{}, rowsSoFar int) error {
	cursorJSON := "[]"
	if len(nextCursor) > 0 {
		raw, err := json.Marshal(cursorToJSON(nextCursor))
		if err != nil {
			return err
		}
		cursorJSON = string(raw)
	}
	now := time.Now().UTC()
	completedAtZero := time.Unix(0, 0).UTC()
	row := types.StructValue(
		types.StructFieldValue("table_name", types.TextValue(tableName)),
		types.StructFieldValue("resume_cursor", types.TextValue(cursorJSON)),
		types.StructFieldValue("rows_so_far", types.Uint64Value(uint64(rowsSoFar))),
		types.StructFieldValue("updated_at", types.DatetimeValueFromTime(now)),
		types.StructFieldValue("completed_at", types.DatetimeValueFromTime(completedAtZero)),
	)
	tablePath := StateTablePath(database)
	list := types.ListValue(row)
	return db.Table().BulkUpsert(ctx, tablePath, table.BulkUpsertDataRows(list), table.WithIdempotent())
}

// GetProgress returns saved progress for the table (cursor for next read, rows_so_far). ok=false if none or table already completed.
func GetProgress(ctx context.Context, db Client, database string, tableName string) (cursor []interface{}, rowsSoFar int, ok bool, err error) {
	tablePath := StateTablePath(database)
	queryText := fmt.Sprintf("SELECT resume_cursor, rows_so_far, completed_at FROM %s WHERE table_name = $name", quoteTablePath(tablePath))
	var cursorJSON string
	var rows uint64
	var completedAt time.Time
	err = db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		row, err := tx.QueryRow(ctx, queryText,
			query.WithParameters(ydbsdk.ParamsBuilder().Param("$name").Text(tableName).Build()),
		)
		if err != nil {
			return err
		}
		return row.ScanNamed(
			query.Named("resume_cursor", &cursorJSON),
			query.Named("rows_so_far", &rows),
			query.Named("completed_at", &completedAt),
		)
	}, query.WithIdempotent(), query.WithTxSettings(query.TxSettings(query.WithSnapshotReadOnly())))
	if err != nil {
		if errors.Is(err, query.ErrNoRows) {
			return nil, 0, false, nil
		}
		return nil, 0, false, err
	}
	if cursorJSON == "" {
		return nil, 0, false, nil
	}
	// Do not resume if already completed
	if !completedAt.IsZero() && completedAt.Unix() > 0 {
		return nil, 0, false, nil
	}
	var arr []interface{}
	if err := json.Unmarshal([]byte(cursorJSON), &arr); err != nil {
		return nil, 0, false, err
	}
	cursor = jsonToCursor(arr)
	return cursor, int(rows), true, nil
}

// cursorToJSON converts []interface{} to a JSON-serializable slice (e.g. int64->float for numbers).
func cursorToJSON(c []interface{}) []interface{} {
	out := make([]interface{}, len(c))
	for i, v := range c {
		switch x := v.(type) {
		case []byte:
			out[i] = string(x)
		case int:
			out[i] = float64(x)
		case int64:
			out[i] = float64(x)
		case uint64:
			out[i] = float64(x)
		default:
			out[i] = v
		}
	}
	return out
}

// jsonToCursor converts JSON-unmarshaled []interface{} back to types suitable for MySQL (float64->int64).
func jsonToCursor(arr []interface{}) []interface{} {
	out := make([]interface{}, len(arr))
	for i, v := range arr {
		switch x := v.(type) {
		case float64:
			out[i] = int64(x)
		case string:
			out[i] = x
		default:
			out[i] = v
		}
	}
	return out
}
