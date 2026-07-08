package ydb

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"path"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/mysql2ydb/mysql2ydb/internal/schema"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

// TableClient is the minimal YDB interface for Table API (DescribeTable, BulkUpsert).
type TableClient interface {
	Table() table.Client
}

// Client is the YDB interface with both Table and Query API (for DoTx).
type Client interface {
	Table() table.Client
	Query() query.Client
}

// maxTxExecBytes is the max estimated params size per tx.Exec (YDB limit 50MB). Use 40MB so estimation error stays under limit.
const maxTxExecBytes = 40 * 1024 * 1024

// BulkUpsertWriter writes rows to YDB using BulkUpsert (idempotent by default).
// mu protects only in-memory caches (checkedTables, txOnlyTables, ydbColumns, etc.).
// BulkUpsert, DoTx, and DescribeTable are never called under mu so concurrent WriteChunk calls can run in parallel.
type BulkUpsertWriter struct {
	mu              sync.Mutex
	db              Client
	database        string
	forceTxUpsert          bool // skip BulkUpsert, use writeChunkTx only (workaround when BulkUpsert hangs)
	debugIssues            bool // log YQL issues from YDB errors when -ydb-debug is on
	dumpOnErrorDir         string // when set, dump failed BulkUpsert chunks to this directory (-ydb-dump-failed-chunks)
	bulkUpsertNonIdempotent bool // pass table.WithIdempotent(false) to BulkUpsert (-ydb-bulkupsert-non-idempotent)
	checkedTables   map[string]struct{}
	txOnlyTables    map[string]struct{}          // tables that require tx path (BulkUpsert not supported), checked once
	ydbColumns      map[string][]string          // column names per table
	ydbColumnIsText map[string]map[string]bool   // tablePath -> columnName -> true if YDB type is Utf8/Text (not String)
	ydbColumnYQL    map[string]map[string]string // tablePath -> columnName -> YQL type (e.g. "Uint32", "Optional<Uint32>") for BulkUpsert type coercion
}

// BulkUpsertWriterOption configures the writer.
type BulkUpsertWriterOption func(*BulkUpsertWriter)

// WithForceTxUpsert makes the writer use transactional UPSERT instead of BulkUpsert.
func WithForceTxUpsert(force bool) BulkUpsertWriterOption {
	return func(w *BulkUpsertWriter) { w.forceTxUpsert = force }
}

// WithDebugIssues enables logging of YQL/operation issues from YDB errors (-ydb-debug).
func WithDebugIssues(debug bool) BulkUpsertWriterOption {
	return func(w *BulkUpsertWriter) { w.debugIssues = debug }
}

// WithDumpFailedChunks dumps BulkUpsert chunk data to dir when BulkUpsert fails (-ydb-dump-failed-chunks).
func WithDumpFailedChunks(dir string) BulkUpsertWriterOption {
	return func(w *BulkUpsertWriter) { w.dumpOnErrorDir = dir }
}

// WithBulkUpsertNonIdempotent disables SDK retries for BulkUpsert via table.WithIdempotent(false).
func WithBulkUpsertNonIdempotent(nonIdempotent bool) BulkUpsertWriterOption {
	return func(w *BulkUpsertWriter) { w.bulkUpsertNonIdempotent = nonIdempotent }
}

func (w *BulkUpsertWriter) bulkUpsertOpts() []table.Option {
	return []table.Option{table.WithIdempotent(!w.bulkUpsertNonIdempotent)}
}

// NewBulkUpsertWriter creates a writer that calls BulkUpsert (table.WithIdempotent() by default).
func NewBulkUpsertWriter(db Client, database string, opts ...BulkUpsertWriterOption) *BulkUpsertWriter {
	w := &BulkUpsertWriter{
		db:              db,
		database:        database,
		checkedTables:   make(map[string]struct{}),
		txOnlyTables:    make(map[string]struct{}),
		ydbColumns:      make(map[string][]string),
		ydbColumnIsText: make(map[string]map[string]bool),
		ydbColumnYQL:    make(map[string]map[string]string),
	}
	for _, o := range opts {
		o(w)
	}
	return w
}

// WriteChunk sends a batch of rows to YDB via BulkUpsert.
// If the table has synchronous indexes, falls back to transactional UPSERT (with dedupe by PK/unique).
// Returns the number of rows actually written to YDB (after dedupe in tx path).
func (w *BulkUpsertWriter) WriteChunk(ctx context.Context, meta *schema.TableMeta, rows []map[string]interface{}) (rowsWritten int, err error) {
	if len(rows) == 0 {
		return 0, nil
	}
	tablePath := path.Join(w.database, meta.Name)
	if err := w.checkYDBKeyColumns(ctx, tablePath, meta); err != nil {
		LogIssuesIfDebug(w.debugIssues, err)
		return 0, err
	}
	w.mu.Lock()
	forceTx := w.forceTxUpsert
	useTx := false
	if !forceTx {
		_, useTx = w.txOnlyTables[tablePath]
	}
	colNamesForRow := w.ydbColumns[tablePath]
	colYQL := w.ydbColumnYQL[tablePath]
	w.mu.Unlock()
	if forceTx {
		log.Printf("  [ydb] %s: force-tx-upsert %d rows", meta.Name, len(rows))
		return w.writeChunkTx(ctx, meta, rows, tablePath)
	}
	if useTx {
		return w.writeChunkTx(ctx, meta, rows, tablePath)
	}
	if colNamesForRow == nil {
		colNamesForRow = make([]string, len(meta.Columns))
		for i, c := range meta.Columns {
			colNamesForRow[i] = c.Name
		}
	}
	log.Printf("  [ydb] %s: BulkUpsert %d rows", meta.Name, len(rows))
	metaByNameLower := make(map[string]*schema.Column)
	for i := range meta.Columns {
		metaByNameLower[strings.ToLower(meta.Columns[i].Name)] = &meta.Columns[i]
	}
	ydbRows := make([]types.Value, 0, len(rows))
	for _, row := range rows {
		fields := make([]types.StructValueOption, 0, len(colNamesForRow))
		for _, ydbName := range colNamesForRow {
			col := metaByNameLower[strings.ToLower(ydbName)]
			if col == nil {
				continue
			}
			ydbYQL := colYQL[ydbName]
			v, err := mysqlValueToYDBForBulkUpsert(row[col.Name], col.DataType, col.Nullable, ydbYQL)
			if err != nil {
				return 0, fmt.Errorf("column %s: %w", col.Name, err)
			}
			fields = append(fields, types.StructFieldValue(ydbName, v))
		}
		ydbRows = append(ydbRows, types.StructValue(fields...))
	}
	list := types.ListValue(ydbRows...)
	err = w.db.Table().BulkUpsert(ctx, tablePath, table.BulkUpsertDataRows(list), w.bulkUpsertOpts()...)
	if err == nil {
		return len(rows), nil
	}
	LogIssuesIfDebug(w.debugIssues, err)
	// Fallback for tables with sync indexes: "Only async-indexed tables are supported by BulkUpsert"
	if strings.Contains(err.Error(), "Only async-indexed tables are supported") {
		w.mu.Lock()
		w.txOnlyTables[tablePath] = struct{}{}
		w.mu.Unlock()
		log.Printf("  [ydb] %s: BulkUpsert not supported (sync indexes), using tx upsert for this table", meta.Name)
		return w.writeChunkTx(ctx, meta, rows, tablePath)
	}
	if w.dumpOnErrorDir != "" {
		if dumpPath, dumpErr := DumpBulkUpsertFailure(w.dumpOnErrorDir, tablePath, meta, rows, err); dumpErr != nil {
			log.Printf("  [ydb] %s: failed to dump BulkUpsert chunk: %v", meta.Name, dumpErr)
		} else {
			log.Printf("  [ydb] %s: dumped failed BulkUpsert chunk (%d rows) to %s", meta.Name, len(rows), dumpPath)
		}
	}
	return 0, err
}

// checkYDBKeyColumns ensures all YDB table key columns are present in the MySQL schema,
// so we don't get "Missing key column in input" when writing. Result is cached per table.
// Comparison is case-insensitive (MySQL and YDB may differ in column name casing).
func (w *BulkUpsertWriter) checkYDBKeyColumns(ctx context.Context, tablePath string, meta *schema.TableMeta) error {
	w.mu.Lock()
	if _, ok := w.checkedTables[tablePath]; ok {
		w.mu.Unlock()
		return nil
	}
	w.mu.Unlock()
	log.Printf("  [ydb] %s: DescribeTable", meta.Name)
	ourColsLower := make(map[string]bool)
	for _, c := range meta.Columns {
		ourColsLower[strings.ToLower(c.Name)] = true
	}
	var ydbPK []string
	var desc options.Description
	err := w.db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		var err error
		desc, err = s.DescribeTable(ctx, tablePath)
		if err != nil {
			return err
		}
		ydbPK = desc.PrimaryKey
		return nil
	}, table.WithIdempotent())
	if err != nil {
		LogIssuesIfDebug(w.debugIssues, err)
		return err
	}
	var missing []string
	for _, pk := range ydbPK {
		if !ourColsLower[strings.ToLower(pk)] {
			missing = append(missing, pk)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("YDB table %s has key columns %v that are not in the MySQL table %s. Recreate the YDB table with --schema-only to match MySQL, or add columns %v to MySQL",
			tablePath, missing, meta.Name, missing)
	}
	// BulkUpsert supports only async indexes; sync indexes require transactional UPSERT.
	hasSyncIndex := false
	for _, idx := range desc.Indexes {
		if idx.Type != options.IndexTypeGlobalAsync {
			hasSyncIndex = true
			break
		}
	}
	ydbCols := make([]string, 0, len(desc.Columns))
	colIsText := make(map[string]bool)
	colYQL := make(map[string]string)
	ourColsSet := make(map[string]bool)
	for _, c := range meta.Columns {
		ourColsSet[strings.ToLower(c.Name)] = true
	}
	for _, c := range desc.Columns {
		if ourColsSet[strings.ToLower(c.Name)] {
			ydbCols = append(ydbCols, c.Name)
			yql := c.Type.Yql()
			colIsText[c.Name] = strings.Contains(yql, "Utf8") || strings.Contains(yql, "Text")
			colYQL[c.Name] = yql
		}
	}
	w.mu.Lock()
	w.checkedTables[tablePath] = struct{}{}
	w.ydbColumns[tablePath] = ydbCols
	w.ydbColumnIsText[tablePath] = colIsText
	w.ydbColumnYQL[tablePath] = colYQL
	if hasSyncIndex {
		w.txOnlyTables[tablePath] = struct{}{}
	}
	w.mu.Unlock()
	if hasSyncIndex {
		log.Printf("  [ydb] %s: table has sync index(es), using tx upsert (BulkUpsert not supported)", meta.Name)
	}
	return nil
}

// estimateRowSizeBytes returns approximate serialized size of a row for YDB params (for batch sizing).
// Uses 1.15x multiplier for string/bytes so we stay under YDB 50MB params limit when proto overhead is added.
func estimateRowSizeBytes(row map[string]interface{}, meta *schema.TableMeta) int {
	const fieldOverhead = 16
	const stringOverhead = 1.15 // proto encoding adds tags/lengths
	size := 64
	for _, col := range meta.Columns {
		v := row[col.Name]
		if v == nil {
			size += fieldOverhead + 1
			continue
		}
		size += fieldOverhead + len(col.Name)
		switch val := v.(type) {
		case []byte:
			size += int(float64(len(val)) * stringOverhead)
		case string:
			size += int(float64(len(val)) * stringOverhead)
		case int64, int32, int16, int8, uint64, uint32, uint16, uint8, float64, float32, bool:
			size += 8
		case time.Time:
			size += 16
		default:
			if _, ok := val.(*big.Int); ok {
				size += 32
			} else {
				size += 64
			}
		}
	}
	return size
}

// partitionRowsBySize splits rows into chunks each under maxBytes (estimated). Splits in half when over limit.
func partitionRowsBySize(rows []map[string]interface{}, meta *schema.TableMeta, maxBytes int) [][]map[string]interface{} {
	if len(rows) == 0 {
		return nil
	}
	var total int
	for _, row := range rows {
		total += estimateRowSizeBytes(row, meta)
	}
	if total <= maxBytes {
		return [][]map[string]interface{}{rows}
	}
	mid := len(rows) / 2
	left := partitionRowsBySize(rows[:mid], meta, maxBytes)
	right := partitionRowsBySize(rows[mid:], meta, maxBytes)
	return append(left, right...)
}

// writeChunkTx writes rows via transactional UPSERT with a single List parameter (AS_TABLE).
// Query text is fixed size, so no 128KB limit issue.
func (w *BulkUpsertWriter) writeChunkTx(ctx context.Context, meta *schema.TableMeta, rows []map[string]interface{}, tablePath string) (rowsWritten int, err error) {
	metaByNameLower := make(map[string]*schema.Column)
	for i := range meta.Columns {
		metaByNameLower[strings.ToLower(meta.Columns[i].Name)] = &meta.Columns[i]
	}
	var desc options.Description
	err = w.db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		var err error
		desc, err = s.DescribeTable(ctx, tablePath)
		return err
	}, table.WithIdempotent())
	if err != nil {
		return 0, err
	}
	ydbCols := make([]string, 0, len(desc.Columns))
	colYQL := make(map[string]string)
	for _, c := range desc.Columns {
		if metaByNameLower[strings.ToLower(c.Name)] != nil {
			ydbCols = append(ydbCols, c.Name)
			colYQL[c.Name] = c.Type.Yql()
		}
	}
	if len(ydbCols) == 0 {
		return 0, fmt.Errorf("no matching columns between YDB table %s and MySQL table %s", tablePath, meta.Name)
	}
	log.Printf("  [ydb] %s: writeChunkTx start (%d rows)", meta.Name, len(rows))
	// Parameter types (Optional vs required, Date/Datetime/Timestamp) are carried by the typed
	// $data value itself, so the query service infers them without an explicit DECLARE.
	quotedPath := "`" + strings.ReplaceAll(tablePath, "`", "``") + "`"
	queryText := fmt.Sprintf("UPSERT INTO %s SELECT * FROM AS_TABLE($data);", quotedPath)

	chunks := partitionRowsBySize(rows, meta, maxTxExecBytes)
	log.Printf("  [ydb] %s: writeChunkTx upserting %d rows in %d batch(es) (max %d MB/batch)", meta.Name, len(rows), len(chunks), maxTxExecBytes/(1024*1024))

	if err = w.db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		for i, chunk := range chunks {
			ydbRows := make([]types.Value, 0, len(chunk))
			for _, row := range chunk {
				fields := make([]types.StructValueOption, 0, len(ydbCols))
				for _, ydbName := range ydbCols {
					col := metaByNameLower[strings.ToLower(ydbName)]
					if col == nil {
						continue
					}
					ydbType := colYQL[ydbName]
					v, err := mysqlValueToYDBForBulkUpsert(row[col.Name], col.DataType, col.Nullable, ydbType)
					if err != nil {
						return fmt.Errorf("column %s: %w", col.Name, err)
					}
					fields = append(fields, types.StructFieldValue(ydbName, v))
				}
				ydbRows = append(ydbRows, types.StructValue(fields...))
			}
			list := types.ListValue(ydbRows...)
			opts := []query.ExecuteOption{query.WithParameters(ydbsdk.ParamsBuilder().Param("$data").Any(list).Build())}
			if i == len(chunks)-1 {
				opts = append(opts, query.WithCommit())
			}
			// NOTE: IssuesHandler() is intentionally not attached here. The YDB SDK only wires
			// WithIssuesHandler on client-level Query/Exec, not on transaction-scoped tx.Exec, so
			// it would be silently ignored. Issues from *failed* statements are still logged via
			// LogIssuesIfDebug on the error returned by DoTx below.
			if err := tx.Exec(ctx, queryText, opts...); err != nil {
				if isValueRepresentationError(err) {
					logParameterTypes(meta.Name, ydbCols, metaByNameLower)
				}
				return err
			}
		}
		return nil
	}, query.WithIdempotent()); err != nil {
		LogIssuesIfDebug(w.debugIssues, err)
		return 0, err
	}
	return len(rows), nil
}

// isValueRepresentationError returns true for BAD_REQUEST "Invalid value representation for type".
func isValueRepresentationError(err error) bool {
	s := err.Error()
	return strings.Contains(s, "BAD_REQUEST") &&
		strings.Contains(s, "Invalid value representation for type")
}

// logParameterTypes logs column names and their YDB/YQL types (for debugging type mismatch errors).
func logParameterTypes(tableName string, ydbCols []string, metaByNameLower map[string]*schema.Column) {
	var b strings.Builder
	b.WriteString("  [ydb] parameter types for table ")
	b.WriteString(tableName)
	b.WriteString(" (on value representation error):\n")
	for _, ydbName := range ydbCols {
		col := metaByNameLower[strings.ToLower(ydbName)]
		if col == nil {
			b.WriteString("    ")
			b.WriteString(ydbName)
			b.WriteString(": (no MySQL column)\n")
			continue
		}
		yqlType := ydbYQLTypeFromMySQL(col.DataType, col.Nullable)
		b.WriteString("    ")
		b.WriteString(ydbName)
		b.WriteString("  mysql=")
		b.WriteString(col.DataType)
		if col.Nullable {
			b.WriteString(" nullable")
		}
		b.WriteString("  yql=")
		b.WriteString(yqlType)
		b.WriteString("\n")
	}
	log.Print(b.String())
}

func isStringOrBinaryType(dataType string) bool {
	switch dataType {
	case "char", "varchar", "text", "mediumtext", "longtext", "json", "enum", "set",
		"binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob", "bytes":
		return true
	}
	return false
}

// textOrBytesValue returns TextValue for text types or BytesValue for binary.
// For text types, invalid UTF-8 is replaced with U+FFFD so YDB does not reject with "string field contains invalid UTF-8".
func textOrBytesValue(s string, dataType string) types.Value {
	switch dataType {
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob", "bytes":
		return types.BytesValueFromString(s)
	default:
		if !utf8.ValidString(s) {
			s = strings.ToValidUTF8(s, "\ufffd")
		}
		return types.TextValue(s)
	}
}

// textOrBytesValueWithHint returns TextValue or BytesValue based on YDB column type (BulkUpsert: table may have Utf8 or String).
func textOrBytesValueWithHint(s string, ydbWantsText bool) types.Value {
	if ydbWantsText {
		if !utf8.ValidString(s) {
			s = strings.ToValidUTF8(s, "\ufffd")
		}
		return types.TextValue(s)
	}
	return types.BytesValueFromString(s)
}

// nullValueForMySQLType returns YDB NULL (Optional empty) for the MySQL type. Used when raw is nil and column is nullable.
func nullValueForMySQLType(dataType string) types.Value {
	switch dataType {
	case "bool":
		return types.NullableBoolValue(nil)
	case "int", "mediumint", "smallint", "tinyint":
		return types.NullableInt32Value(nil)
	case "bigint":
		return types.NullableInt64Value(nil)
	case "int unsigned", "mediumint unsigned", "smallint unsigned", "tinyint unsigned":
		return types.NullableUint32Value(nil)
	case "bigint unsigned":
		return types.NullableUint64Value(nil)
	case "float":
		return types.NullableFloatValue(nil)
	case "double", "real":
		return types.NullableDoubleValue(nil)
	case "date":
		return types.NullableDateValueFromTime(nil)
	case "datetime", "timestamp":
		return types.NullableTimestampValueFromTime(nil)
	case "year":
		return types.NullableUint16Value(nil)
	case "bit":
		return types.NullableUint64Value(nil)
	case "decimal", "numeric":
		return types.NullableDecimalValueFromBigInt(nil, 22, 9)
	default:
		if isStringOrBinaryType(dataType) {
			if dataType == "binary" || dataType == "varbinary" || dataType == "tinyblob" || dataType == "blob" || dataType == "mediumblob" || dataType == "longblob" || dataType == "bytes" {
				return types.NullableBytesValueFromString(nil)
			}
			return types.NullableTextValue(nil)
		}
		return types.NullableTextValue(nil)
	}
}

// zeroValueForMySQLType returns a non-null zero value for the MySQL type. Used when raw is nil but column is NOT NULL (invalid data).
func zeroValueForMySQLType(dataType string) types.Value {
	switch dataType {
	case "bool":
		return types.BoolValue(false)
	case "int", "mediumint", "smallint", "tinyint":
		return types.Int32Value(0)
	case "bigint":
		return types.Int64Value(0)
	case "int unsigned", "mediumint unsigned", "smallint unsigned", "tinyint unsigned":
		return types.Uint32Value(0)
	case "bigint unsigned":
		return types.Uint64Value(0)
	case "float":
		return types.FloatValue(0)
	case "double", "real":
		return types.DoubleValue(0)
	case "date":
		return types.DateValueFromTime(time.Unix(0, 0).UTC())
	case "datetime", "timestamp":
		return types.TimestampValueFromTime(time.Unix(0, 0).UTC())
	case "year":
		return types.Uint16Value(0)
	case "bit":
		return types.Uint64Value(0)
	case "decimal", "numeric":
		return types.DecimalValueFromBigInt(big.NewInt(0), 22, 9)
	default:
		if isStringOrBinaryType(dataType) {
			return textOrBytesValue("", dataType)
		}
		return types.TextValue("")
	}
}

// wrapOptionalIfNullable wraps v in OptionalValue when column is nullable, so DECLARE Optional<T> matches actual type.
func wrapOptionalIfNullable(v types.Value, nullable bool) types.Value {
	if !nullable {
		return v
	}
	return types.OptionalValue(v)
}

// ydbColumnIsOptional returns true if the YDB column type is Optional<T> (so we must send OptionalValue or NullableXxx).
func ydbColumnIsOptional(ydbColumnYQL string) bool {
	return strings.Contains(ydbColumnYQL, "Optional<")
}

// wrapOptionalIfYDBOptional wraps v in OptionalValue only when the YDB column type is Optional. Used by BulkUpsert to match exact column type.
func wrapOptionalIfYDBOptional(v types.Value, ydbColumnYQL string) types.Value {
	if !ydbColumnIsOptional(ydbColumnYQL) {
		return v
	}
	return types.OptionalValue(v)
}

// isYDBDateType returns true if ydbColumnYQL is a date/time type (Date, Datetime, or Timestamp).
func isYDBDateType(ydbColumnYQL string) bool {
	return strings.Contains(ydbColumnYQL, "Timestamp") ||
		strings.Contains(ydbColumnYQL, "Datetime") ||
		strings.Contains(ydbColumnYQL, "Date")
}

// dateTimeToYDBForBulkUpsert converts a time (or nil) to YDB value matching ydbColumnYQL: Date, Datetime, or Timestamp; optional only if column is Optional.
// Used when ydbColumnYQL contains Date/Datetime/Timestamp so we don't send Optional<Timestamp> to a required Timestamp column.
func dateTimeToYDBForBulkUpsert(t time.Time, nullable bool, ydbColumnYQL string) (types.Value, error) {
	isOpt := ydbColumnIsOptional(ydbColumnYQL)
	if t.IsZero() || t.Year() < 1970 {
		if nullable || isOpt {
			switch {
			case strings.Contains(ydbColumnYQL, "Timestamp"):
				return types.NullableTimestampValueFromTime(nil), nil
			case strings.Contains(ydbColumnYQL, "Datetime"):
				return types.NullableDatetimeValueFromTime(nil), nil
			case strings.Contains(ydbColumnYQL, "Date"):
				return types.NullableDateValueFromTime(nil), nil
			default:
				return types.NullableTimestampValueFromTime(nil), nil
			}
		}
		t = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	var v types.Value
	switch {
	case strings.Contains(ydbColumnYQL, "Datetime"):
		v = types.DatetimeValueFromTime(t)
	case strings.Contains(ydbColumnYQL, "Date"):
		v = types.DateValueFromTime(t)
	default:
		v = types.TimestampValueFromTime(t)
	}
	return wrapOptionalIfYDBOptional(v, ydbColumnYQL), nil
}

// mysqlValueToYDBForBulkUpsert converts a MySQL value for BulkUpsert, coercing to match the existing YDB column type (ydbColumnYQL).
// When ydbColumnYQL is empty, delegates to mysqlValueToYDB. Otherwise coerces string/bytes (Text vs String) and integers (Uint32/Int32/etc).
func mysqlValueToYDBForBulkUpsert(raw interface{}, dataType string, nullable bool, ydbColumnYQL string) (types.Value, error) {
	if ydbColumnYQL == "" {
		return mysqlValueToYDB(raw, dataType, nullable)
	}
	ydbWantsText := strings.Contains(ydbColumnYQL, "Utf8") || strings.Contains(ydbColumnYQL, "Text")
	if raw == nil {
		if !nullable {
			return nil, fmt.Errorf("column is NOT NULL but got NULL from MySQL (dataType=%s)", dataType)
		}
		if (dataType == "date" || dataType == "datetime" || dataType == "timestamp") && isYDBDateType(ydbColumnYQL) {
			return dateTimeToYDBForBulkUpsert(time.Time{}, true, ydbColumnYQL)
		}
		if isStringOrBinaryType(dataType) {
			if ydbWantsText {
				return types.NullableTextValue(nil), nil
			}
			return types.NullableBytesValueFromString(nil), nil
		}
		return nullValueForMySQLType(dataType), nil
	}
	if dataType == "bool" {
		if v, ok := rawToBool(raw); ok {
			return wrapOptionalIfYDBOptional(types.BoolValue(v), ydbColumnYQL), nil
		}
	}
	switch v := raw.(type) {
	case []byte:
		if (dataType == "date" || dataType == "datetime" || dataType == "timestamp") && isYDBDateType(ydbColumnYQL) {
			if t, ok := parseDateTimeToTime(string(v), dataType); ok {
				return dateTimeToYDBForBulkUpsert(t, nullable, ydbColumnYQL)
			}
		}
		if val, ok := parseDateTime(string(v), dataType, nullable); ok {
			return wrapOptionalIfYDBOptional(val, ydbColumnYQL), nil
		}
		if isStringOrBinaryType(dataType) {
			return wrapOptionalIfYDBOptional(textOrBytesValueWithHint(string(v), ydbWantsText), ydbColumnYQL), nil
		}
		return wrapOptionalIfYDBOptional(textOrBytesValue(string(v), dataType), ydbColumnYQL), nil
	case string:
		if (dataType == "date" || dataType == "datetime" || dataType == "timestamp") && isYDBDateType(ydbColumnYQL) {
			if t, ok := parseDateTimeToTime(v, dataType); ok {
				return dateTimeToYDBForBulkUpsert(t, nullable, ydbColumnYQL)
			}
		}
		if val, ok := parseDateTime(v, dataType, nullable); ok {
			return wrapOptionalIfYDBOptional(val, ydbColumnYQL), nil
		}
		if isStringOrBinaryType(dataType) {
			return wrapOptionalIfYDBOptional(textOrBytesValueWithHint(v, ydbWantsText), ydbColumnYQL), nil
		}
		return wrapOptionalIfYDBOptional(textOrBytesValue(v, dataType), ydbColumnYQL), nil
	case int64:
		var val types.Value
		if c := coerceIntToYDBType(v, ydbColumnYQL); c != nil {
			val = c
		} else {
			val = int64ToYDB(v, dataType)
		}
		return wrapOptionalIfYDBOptional(val, ydbColumnYQL), nil
	case int32:
		var val types.Value
		if c := coerceIntToYDBType(int64(v), ydbColumnYQL); c != nil {
			val = c
		} else {
			val = int32ToYDB(v, dataType)
		}
		return wrapOptionalIfYDBOptional(val, ydbColumnYQL), nil
	case uint64:
		var val types.Value
		if c := coerceUintToYDBType(v, ydbColumnYQL); c != nil {
			val = c
		} else {
			val = uint64ToYDB(v, dataType)
		}
		return wrapOptionalIfYDBOptional(val, ydbColumnYQL), nil
	case uint32:
		var val types.Value
		if c := coerceUintToYDBType(uint64(v), ydbColumnYQL); c != nil {
			val = c
		} else {
			val = uint32ToYDB(v, dataType)
		}
		return wrapOptionalIfYDBOptional(val, ydbColumnYQL), nil
	case int16, int8, uint16, uint8, float64, float32, bool:
		// Get bare value (no Optional) then wrap to match YDB column type.
		bare, err := mysqlValueToYDB(raw, dataType, false)
		if err != nil {
			return nil, err
		}
		return wrapOptionalIfYDBOptional(bare, ydbColumnYQL), nil
	case time.Time:
		if (dataType == "date" || dataType == "datetime" || dataType == "timestamp") && isYDBDateType(ydbColumnYQL) {
			return dateTimeToYDBForBulkUpsert(v, nullable, ydbColumnYQL)
		}
		val, err := dateTimeToYDB(v, dataType, false)
		if err != nil {
			return nil, err
		}
		return wrapOptionalIfYDBOptional(val, ydbColumnYQL), nil
	default:
		bare, err := mysqlValueToYDB(raw, dataType, false)
		if err != nil {
			return nil, err
		}
		return wrapOptionalIfYDBOptional(bare, ydbColumnYQL), nil
	}
}

// coerceIntToYDBType returns a YDB value for an integer when the column type in YDB (yql) requires a specific int type; nil if no coercion needed.
func coerceIntToYDBType(v int64, yql string) types.Value {
	// Match longer names first (Uint64 before Uint32).
	switch {
	case strings.Contains(yql, "Uint64"):
		if v >= 0 {
			return types.Uint64Value(uint64(v))
		}
		return types.Uint64Value(0)
	case strings.Contains(yql, "Int64"):
		return types.Int64Value(v)
	case strings.Contains(yql, "Uint32"):
		if v >= 0 && v <= 0xFFFFFFFF {
			return types.Uint32Value(uint32(v))
		}
		if v < 0 {
			return types.Uint32Value(0)
		}
		return types.Uint32Value(0xFFFFFFFF)
	case strings.Contains(yql, "Int32"):
		if v >= -0x80000000 && v <= 0x7FFFFFFF {
			return types.Int32Value(int32(v))
		}
		return nil
	case strings.Contains(yql, "Uint16"):
		if v >= 0 && v <= 0xFFFF {
			return types.Uint16Value(uint16(v))
		}
		return nil
	}
	return nil
}

// coerceUintToYDBType returns a YDB value for an unsigned integer when the column type in YDB (yql) requires a specific type; nil if no coercion needed.
func coerceUintToYDBType(v uint64, yql string) types.Value {
	switch {
	case strings.Contains(yql, "Uint64"):
		return types.Uint64Value(v)
	case strings.Contains(yql, "Uint32"):
		if v <= 0xFFFFFFFF {
			return types.Uint32Value(uint32(v))
		}
		return types.Uint32Value(0xFFFFFFFF)
	case strings.Contains(yql, "Uint16"):
		if v <= 0xFFFF {
			return types.Uint16Value(uint16(v))
		}
		return nil
	case strings.Contains(yql, "Int64"):
		if v <= 0x7FFFFFFFFFFFFFFF {
			return types.Int64Value(int64(v))
		}
		return nil
	case strings.Contains(yql, "Int32"):
		if v <= 0x7FFFFFFF {
			return types.Int32Value(int32(v))
		}
		return nil
	}
	return nil
}

// mysqlValueToYDB converts a single MySQL value (from sql.Scan into interface{}) to YDB types.Value.
// For NOT NULL columns, nil from MySQL is an error. For nullable and nil we send YDB NULL (Optional empty).
func mysqlValueToYDB(raw interface{}, dataType string, nullable bool) (types.Value, error) {
	if raw == nil {
		if !nullable {
			return nil, fmt.Errorf("column is NOT NULL but got NULL from MySQL (dataType=%s)", dataType)
		}
		return nullValueForMySQLType(dataType), nil
	}

	// MySQL TINYINT(1) mapped to bool: convert numeric/string to Bool
	if dataType == "bool" {
		if v, ok := rawToBool(raw); ok {
			return wrapOptionalIfNullable(types.BoolValue(v), nullable), nil
		}
	}
	switch v := raw.(type) {
	case []byte:
		if val, ok := parseDateTime(string(v), dataType, nullable); ok {
			return wrapOptionalIfNullable(val, nullable), nil
		}
		return wrapOptionalIfNullable(textOrBytesValue(string(v), dataType), nullable), nil
	case string:
		if val, ok := parseDateTime(v, dataType, nullable); ok {
			return wrapOptionalIfNullable(val, nullable), nil
		}
		return wrapOptionalIfNullable(textOrBytesValue(v, dataType), nullable), nil
	case int64:
		return wrapOptionalIfNullable(int64ToYDB(v, dataType), nullable), nil
	case int32:
		return wrapOptionalIfNullable(int32ToYDB(v, dataType), nullable), nil
	case int16:
		return wrapOptionalIfNullable(int16ToYDB(v, dataType), nullable), nil
	case int8:
		return wrapOptionalIfNullable(int8ToYDB(v, dataType), nullable), nil
	case uint64:
		return wrapOptionalIfNullable(uint64ToYDB(v, dataType), nullable), nil
	case uint32:
		return wrapOptionalIfNullable(uint32ToYDB(v, dataType), nullable), nil
	case uint16:
		return wrapOptionalIfNullable(uint16ToYDB(v, dataType), nullable), nil
	case uint8:
		return wrapOptionalIfNullable(uint8ToYDB(v, dataType), nullable), nil
	case float64:
		return wrapOptionalIfNullable(types.DoubleValue(v), nullable), nil
	case float32:
		return wrapOptionalIfNullable(types.FloatValue(v), nullable), nil
	case bool:
		return wrapOptionalIfNullable(types.BoolValue(v), nullable), nil
	case time.Time:
		val, err := dateTimeToYDB(v, dataType, nullable)
		if err != nil {
			return nil, err
		}
		return wrapOptionalIfNullable(val, nullable), nil
	}
	return nil, fmt.Errorf("unsupported type %T for MySQL type %s", raw, dataType)
}

// isInvalidDate reports dates YDB rejects: zero or before 1970 (Unix epoch).
func isInvalidDate(t time.Time) bool {
	return t.IsZero() || t.Year() < 1970
}

// dateTimeToYDB converts time.Time to YDB Date/Datetime; invalid dates become NULL or 1970-01-01.
func dateTimeToYDB(t time.Time, dataType string, nullable bool) (types.Value, error) {
	if isInvalidDate(t) {
		if nullable {
			switch dataType {
			case "date":
				return types.NullableDateValueFromTime(nil), nil
			default:
				return types.NullableTimestampValueFromTime(nil), nil
			}
		}
		t = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	switch dataType {
	case "date":
		return types.DateValueFromTime(t), nil
	case "datetime", "timestamp":
		return types.TimestampValueFromTime(t), nil
	default:
		return types.TimestampValueFromTime(t), nil
	}
}

// parseDateTimeToTime parses date/datetime/timestamp string to time.Time. Returns (zero time, true) for empty/zero date.
func parseDateTimeToTime(s string, dataType string) (time.Time, bool) {
	s = trimSpace(s)
	if s == "" || s == "0000-00-00" || s == "0000-00-00 00:00:00" {
		if dataType == "date" || dataType == "datetime" || dataType == "timestamp" {
			return time.Time{}, true
		}
		return time.Time{}, false
	}
	switch dataType {
	case "date":
		t, err := time.Parse("2006-01-02", s)
		if err != nil {
			t, err = time.Parse("2006-01-02T15:04:05Z07:00", s)
		}
		if err != nil {
			t, err = time.Parse("2006-01-02 15:04:05", s)
		}
		if err != nil {
			return time.Time{}, false
		}
		return t, true
	case "datetime", "timestamp":
		t, err := time.Parse("2006-01-02 15:04:05", s)
		if err != nil {
			t, err = time.Parse("2006-01-02T15:04:05Z07:00", s)
		}
		if err != nil {
			t, err = time.Parse("2006-01-02 15:04:05.999999", s)
		}
		if err != nil {
			t, err = time.Parse(time.RFC3339Nano, s)
		}
		if err != nil {
			return time.Time{}, false
		}
		return t, true
	}
	return time.Time{}, false
}

// parseDateTime parses date/datetime/timestamp from string (MySQL often returns these as []byte).
// Returns (YDB value, true) on success; invalid/zero dates become NULL or 1970-01-01.
func parseDateTime(s string, dataType string, nullable bool) (types.Value, bool) {
	s = trimSpace(s)
	if s == "" || s == "0000-00-00" || s == "0000-00-00 00:00:00" {
		if dataType == "date" || dataType == "datetime" || dataType == "timestamp" {
			v, _ := dateTimeToYDB(time.Time{}, dataType, nullable)
			return v, true
		}
		return nil, false
	}
	switch dataType {
	case "date":
		t, err := time.Parse("2006-01-02", s)
		if err != nil {
			t, err = time.Parse("2006-01-02T15:04:05Z07:00", s)
		}
		if err != nil {
			t, err = time.Parse("2006-01-02 15:04:05", s)
		}
		if err != nil {
			return nil, false
		}
		val, _ := dateTimeToYDB(t, dataType, nullable)
		return val, true
	case "datetime", "timestamp":
		var t time.Time
		var err error
		t, err = time.Parse("2006-01-02 15:04:05", s)
		if err != nil {
			t, err = time.Parse("2006-01-02T15:04:05Z07:00", s)
		}
		if err != nil {
			t, err = time.Parse("2006-01-02 15:04:05.999999", s)
		}
		if err != nil {
			t, err = time.Parse(time.RFC3339Nano, s)
		}
		if err != nil {
			return nil, false
		}
		val, _ := dateTimeToYDB(t, dataType, nullable)
		return val, true
	}
	return nil, false
}

// rawToBool converts MySQL TINYINT(1) / 0|1 / "true"|"false" to bool for YDB Bool column.
func rawToBool(raw interface{}) (bool, bool) {
	switch v := raw.(type) {
	case bool:
		return v, true
	case int64:
		return v != 0, true
	case int32:
		return v != 0, true
	case int16:
		return v != 0, true
	case int8:
		return v != 0, true
	case uint64:
		return v != 0, true
	case uint32:
		return v != 0, true
	case uint16:
		return v != 0, true
	case uint8:
		return v != 0, true
	case []byte:
		s := trimSpace(string(v))
		return parseBoolString(s), true
	case string:
		return parseBoolString(trimSpace(v)), true
	}
	return false, false
}

func parseBoolString(s string) bool {
	switch s {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// int64ToYDB sends int64 as Int32 when MySQL column was INT/MEDIUMINT/SMALLINT/TINYINT, else Int64.
func int64ToYDB(v int64, dataType string) types.Value {
	switch dataType {
	case "int":
		return types.Int32Value(int32(v))
	case "year":
		return types.Uint16Value(uint16(v))
	}
	return types.Int64Value(v)
}

func int32ToYDB(v int32, dataType string) types.Value {
	switch dataType {
	case "year":
		return types.Uint16Value(uint16(v))
	}
	return types.Int32Value(v)
}

func int16ToYDB(v int16, dataType string) types.Value {
	if dataType == "year" {
		return types.Uint16Value(uint16(v))
	}
	return types.Int16Value(v)
}

func int8ToYDB(v int8, dataType string) types.Value {
	return types.Int8Value(v)
}

func uint64ToYDB(v uint64, dataType string) types.Value {
	switch dataType {
	case "int unsigned":
		return types.Uint32Value(uint32(v))
	case "year":
		return types.Uint16Value(uint16(v))
	}
	return types.Uint64Value(v)
}

func uint32ToYDB(v uint32, dataType string) types.Value {
	if dataType == "year" {
		return types.Uint16Value(uint16(v))
	}
	return types.Uint32Value(v)
}

func uint16ToYDB(v uint16, dataType string) types.Value {
	return types.Uint16Value(v)
}

func uint8ToYDB(v uint8, dataType string) types.Value {
	return types.Uint8Value(v)
}

func trimSpace(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}

func ydbTypeFromMySQL(dataType string) types.Type {
	switch dataType {
	case "bool":
		return types.TypeBool
	case "int", "mediumint", "smallint", "tinyint":
		return types.TypeInt32
	case "bigint":
		return types.TypeInt64
	case "int unsigned", "mediumint unsigned", "smallint unsigned", "tinyint unsigned":
		return types.TypeUint32
	case "bigint unsigned":
		return types.TypeUint64
	case "float":
		return types.TypeFloat
	case "double", "real":
		return types.TypeDouble
	case "date":
		return types.TypeDate
	case "datetime", "timestamp":
		return types.TypeTimestamp
	case "year":
		return types.TypeUint16
	case "char", "varchar", "text", "mediumtext", "longtext", "json", "enum", "set":
		return types.TypeText
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob", "bytes":
		return types.TypeBytes
	case "bit":
		return types.TypeUint64
	case "decimal", "numeric":
		return types.DecimalType(22, 9) // default precision/scale
	default:
		return types.TypeText
	}
}

// ydbYQLTypeFromMySQL returns YQL type name for DECLARE. Text and Utf8 are the same in YDB.
func ydbYQLTypeFromMySQL(dataType string, nullable bool) string {
	var name string
	switch dataType {
	case "bool":
		name = "Bool"
	case "int", "mediumint", "smallint", "tinyint":
		name = "Int32"
	case "bigint":
		name = "Int64"
	case "int unsigned", "mediumint unsigned", "smallint unsigned", "tinyint unsigned":
		name = "Uint32"
	case "bigint unsigned":
		name = "Uint64"
	case "float":
		name = "Float"
	case "double", "real":
		name = "Double"
	case "date":
		name = "Date"
	case "datetime", "timestamp":
		name = "Timestamp"
	case "year":
		name = "Uint16"
	case "char", "varchar", "text", "mediumtext", "longtext", "json", "enum", "set":
		name = "Text"
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob", "bytes":
		name = "String"
	case "bit":
		name = "Uint64"
	case "decimal", "numeric":
		name = "Decimal(22,9)"
	default:
		name = "Text"
	}
	if nullable {
		return "Optional<" + name + ">"
	}
	return name
}
