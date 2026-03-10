package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/mysql2ydb/mysql2ydb/internal/schema"
)

// ChunkReader reads table data from MySQL in fixed-size chunks to avoid loading
// the entire table into memory.
type ChunkReader struct {
	db        *sql.DB
	meta      *schema.TableMeta
	batchSize int
}

// NewChunkReader creates a reader that yields batchSize rows per ReadChunk.
func NewChunkReader(db *sql.DB, meta *schema.TableMeta, batchSize int) *ChunkReader {
	return &ChunkReader{db: db, meta: meta, batchSize: batchSize}
}

// ReadChunk reads the next chunk of rows. It uses cursor-based pagination when
// the table has a primary key (WHERE pk > ? ORDER BY pk LIMIT n), otherwise
// LIMIT/OFFSET. Returns (rows, lastCursor, hasMore, error). lastCursor is used
// for the next call when using cursor pagination.
func (r *ChunkReader) ReadChunk(ctx context.Context, cursor []interface{}) (rows []map[string]interface{}, nextCursor []interface{}, hasMore bool, err error) {
	colNames := make([]string, 0, len(r.meta.Columns))
	for _, c := range r.meta.Columns {
		colNames = append(colNames, "`"+c.Name+"`")
	}
	selectList := strings.Join(colNames, ", ")
	table := "`" + r.meta.Name + "`"

	var query string
	var args []interface{}
	var offset int

	if len(r.meta.PKCols) > 0 {
		// Cursor-based: WHERE (pk1, pk2, ...) > (?, ?, ...) ORDER BY pk1, pk2, ... LIMIT n
		orderClause := "ORDER BY " + strings.Join(colNamesForPK(r.meta.PKCols), ", ")
		limit := r.batchSize + 1 // fetch one extra to know if there's more
		if len(cursor) == 0 {
			query = fmt.Sprintf("SELECT %s FROM %s %s LIMIT %d", selectList, table, orderClause, limit)
			args = nil
		} else {
			// (pk1 > ?) OR (pk1 = ? AND pk2 > ?) ...
			whereCond := buildCursorCondition(r.meta.PKCols)
			query = fmt.Sprintf("SELECT %s FROM %s WHERE %s %s LIMIT %d", selectList, table, whereCond, orderClause, limit)
			args = cursor
		}
	} else {
		// No PK: LIMIT/OFFSET (warning: large offset can be slow)
		offset = 0
		if len(cursor) == 1 {
			switch v := cursor[0].(type) {
			case int:
				offset = v
			case int64:
				offset = int(v)
			}
		}
		query = fmt.Sprintf("SELECT %s FROM %s LIMIT %d OFFSET %d", selectList, table, r.batchSize+1, offset)
		args = nil
	}

	q, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, false, fmt.Errorf("query: %w", err)
	}
	defer func() { _ = q.Close() }()

	cols, _ := q.Columns()
	dest := makeDest(cols)
	var result []map[string]interface{}
	var lastRow []interface{}
	for q.Next() {
		if err := q.Scan(dest...); err != nil {
			return nil, nil, false, err
		}
		row := rowToMap(cols, dest)
		result = append(result, row)
		lastRow = copyDest(dest)
	}
	if err := q.Err(); err != nil {
		return nil, nil, false, err
	}

	hasMore = len(result) > r.batchSize
	if hasMore {
		result = result[:r.batchSize]
		// next cursor: primary key values of last row, or next offset
		if len(r.meta.PKCols) > 0 && lastRow != nil {
			nextCursor = cursorFromRow(cols, lastRow, r.meta.PKCols)
		} else {
			nextCursor = []interface{}{offset + r.batchSize}
		}
	}
	return result, nextCursor, hasMore, nil
}

func colNamesForPK(pk []string) []string {
	out := make([]string, len(pk))
	for i, n := range pk {
		out[i] = "`" + n + "`"
	}
	return out
}

// buildCursorCondition builds (a > ?) OR (a = ? AND b > ?) ... for composite PK.
func buildCursorCondition(pkCols []string) string {
	var parts []string
	for i := range pkCols {
		eqParts := make([]string, i)
		for j := 0; j < i; j++ {
			eqParts[j] = "`" + pkCols[j] + "` = ?"
		}
		gt := "`" + pkCols[i] + "` > ?"
		if i == 0 {
			parts = append(parts, gt)
		} else {
			parts = append(parts, "("+strings.Join(eqParts, " AND ")+" AND "+gt+")")
		}
	}
	return "(" + strings.Join(parts, " OR ") + ")"
}

func makeDest(cols []string) []interface{} {
	dest := make([]interface{}, len(cols))
	for i := range cols {
		dest[i] = new(interface{})
	}
	return dest
}

func copyDest(dest []interface{}) []interface{} {
	out := make([]interface{}, len(dest))
	for i, d := range dest {
		if p, ok := d.(*interface{}); ok && p != nil {
			out[i] = *p
		}
	}
	return out
}

func rowToMap(cols []string, dest []interface{}) map[string]interface{} {
	m := make(map[string]interface{}, len(cols))
	for i, c := range cols {
		if p, ok := dest[i].(*interface{}); ok && p != nil {
			m[c] = *p
		}
	}
	return m
}

func cursorFromRow(cols []string, row []interface{}, pkCols []string) []interface{} {
	idx := make(map[string]int)
	for i, c := range cols {
		idx[c] = i
	}
	out := make([]interface{}, len(pkCols))
	for i, name := range pkCols {
		if j, ok := idx[name]; ok && j < len(row) {
			out[i] = row[j]
		}
	}
	return out
}
