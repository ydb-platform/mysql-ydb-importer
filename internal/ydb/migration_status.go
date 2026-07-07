package ydb

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
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
// extraOpts are appended to the query execution (e.g. IssuesHandler() to log YQL issues under -ydb-debug).
func GetCompletedTables(ctx context.Context, db Client, database string, extraOpts ...query.ExecuteOption) ([]string, error) {
	tablePath := StateTablePath(database)
	queryText := fmt.Sprintf(`
		SELECT table_name FROM %s WHERE completed_at > $min_completed
	`, quoteTablePath(tablePath))
	// Client-level Query (not tx.Query inside DoTx): the YDB SDK only wires WithIssuesHandler
	// on Client.Query/Client.Exec, so IssuesHandler() in extraOpts is silently ignored on
	// transaction-scoped calls. A snapshot-RO tx control gives the same read isolation as DoTx.
	queryOpts := append([]query.ExecuteOption{
		query.WithParameters(ydbsdk.ParamsBuilder().Param("$min_completed").Datetime(epochZero).Build()),
		query.WithTxControl(query.SnapshotReadOnlyTxControl()),
		query.WithIdempotent(),
	}, extraOpts...)
	res, err := db.Query().Query(ctx, queryText, queryOpts...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Close(ctx) }()
	var out []string
	for rs, err := range res.ResultSets(ctx) {
		if err != nil {
			return nil, err
		}
		for row, err := range rs.Rows(ctx) {
			if err != nil {
				return nil, err
			}
			var name string
			if err := row.ScanNamed(query.Named("table_name", &name)); err != nil {
				return nil, err
			}
			out = append(out, name)
		}
	}
	return out, nil
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
// extraOpts are appended to the query execution (e.g. IssuesHandler() to log YQL issues under -ydb-debug).
//
// Uses an aggregate (COUNT(*) + MAX) so the query always returns exactly one row, even when no progress is
// saved. This avoids query.ErrNoRows, which the YDB SDK trace logger surfaces as a scary (but harmless)
// "query pool with failed" DEBUG line under -ydb-debug. table_name is the primary key, so at most one row
// matches and MAX() returns that row's value (or NULL when the set is empty).
func GetProgress(ctx context.Context, db Client, database string, tableName string, extraOpts ...query.ExecuteOption) (cursor []interface{}, rowsSoFar int, ok bool, err error) {
	tablePath := StateTablePath(database)
	queryText := fmt.Sprintf(`
		SELECT
			COUNT(*) AS cnt,
			MAX(resume_cursor) AS resume_cursor,
			MAX(rows_so_far) AS rows_so_far,
			MAX(completed_at) AS completed_at
		FROM %s
		WHERE table_name = $name
	`, quoteTablePath(tablePath))
	var (
		cnt         uint64
		cursorJSON  *string
		rows        *uint64
		completedAt *time.Time
	)
	// Client-level Query (not tx.QueryRow inside DoTx) so IssuesHandler() in extraOpts actually
	// fires: the YDB SDK only wires WithIssuesHandler on Client.Query/Client.Exec, not on
	// transaction- or QueryRow-scoped calls. Snapshot-RO tx control matches the DoTx isolation.
	queryOpts := append([]query.ExecuteOption{
		query.WithParameters(ydbsdk.ParamsBuilder().Param("$name").Text(tableName).Build()),
		query.WithTxControl(query.SnapshotReadOnlyTxControl()),
		query.WithIdempotent(),
	}, extraOpts...)
	res, err := db.Query().Query(ctx, queryText, queryOpts...)
	if err != nil {
		return nil, 0, false, err
	}
	defer func() { _ = res.Close(ctx) }()
	// The aggregate query always returns exactly one row; scan it from the first result set.
	scanned := false
	for rs, err := range res.ResultSets(ctx) {
		if err != nil {
			return nil, 0, false, err
		}
		for row, err := range rs.Rows(ctx) {
			if err != nil {
				return nil, 0, false, err
			}
			if err := row.ScanNamed(
				query.Named("cnt", &cnt),
				query.Named("resume_cursor", &cursorJSON),
				query.Named("rows_so_far", &rows),
				query.Named("completed_at", &completedAt),
			); err != nil {
				return nil, 0, false, err
			}
			scanned = true
		}
	}
	if !scanned {
		return nil, 0, false, nil
	}
	// No saved progress for this table.
	if cnt == 0 || cursorJSON == nil || *cursorJSON == "" {
		return nil, 0, false, nil
	}
	// Do not resume if already completed.
	if completedAt != nil && !completedAt.IsZero() && completedAt.Unix() > 0 {
		return nil, 0, false, nil
	}
	var arr []interface{}
	if err := json.Unmarshal([]byte(*cursorJSON), &arr); err != nil {
		return nil, 0, false, err
	}
	cursor = jsonToCursor(arr)
	if rows != nil {
		rowsSoFar = int(*rows)
	}
	return cursor, rowsSoFar, true, nil
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
