# mysql2ydb

Efficient utility for migrating schema and data from **MySQL to YDB**, designed for **large tables** (data larger than available RAM).

**Goal: a one-to-one copy of the source database.** Table and column names are preserved as in MySQL; data types are mapped to the closest YDB equivalents (`INT` → `Int32`, `VARCHAR` → `Text`, `AUTO_INCREMENT` → `BigSerial`, and so on). The result should be a YDB database that mirrors the original structure and can be queried with minimal mental translation.

Each chunk is read from MySQL in a single bounded `SELECT`, written to YDB via idempotent `BulkUpsert`, then discarded from memory. The process repeats until the table is done — memory usage stays flat regardless of table size.

## Why a separate project?

[YDB Importer](https://github.com/ydb-platform/ydb-importer) is a universal **JDBC** tool: PostgreSQL, Oracle, SQL Server, Db2, Informix, and more. It is powerful when you need one importer for many sources, parallel table import, BLOB/CLOB supplemental tables, XML-driven table mapping, and YDB partition tuning.

**mysql2ydb** solves a narrower problem: move a MySQL database to YDB as fast and simply as possible.

| | [ydb-importer](https://github.com/ydb-platform/ydb-importer) | **mysql2ydb** |
|---|---|---|
| Runtime | Java + JDBC drivers + XML config | Single Go binary, native MySQL protocol |
| Scope | Many JDBC sources | MySQL only |
| Default batch size | 1 000 rows (`max-batch-rows`) | 10 000 rows (`-batch-size`), auto-tuned |
| Large tables | Parallel range splits, partition buffers | Cursor pagination + RAM/network-aware chunk sizing |
| Resume after failure | Re-run import | Checkpoint per chunk in YDB state table |
| Setup | JDBC JARs, XML configuration | `~/.my.cnf` + two flags |

For a **MySQL-only** migration of **multi-gigabyte tables**, a dedicated tool with predictable chunk-by-chunk I/O is simpler to operate and easier to reason about than a general-purpose JDBC importer.

## MySQL schema fidelity

Schema migration aims for **structural parity**: same table names, same column names, indexes where YDB allows them, and types chosen to keep values representable without loss (unsigned integers stay unsigned, booleans stay booleans, and so on).

[YDB Importer](https://github.com/ydb-platform/ydb-importer) solves a different schema problem: it is built to import from many JDBC sources into a **configurable** YDB layout (`table-name-format`, `blob-name-format`, date conversion modes, optional synthetic keys). That flexibility changes the result — it is not a one-to-one mirror of the MySQL catalog.

mysql2ydb reads MySQL metadata from `information_schema` and maps it to native YDB DDL directly. The table below lists schema differences you will see in practice when importing the same MySQL database with both tools:

| MySQL feature | mysql2ydb (one-to-one copy) | ydb-importer |
|---|---|---|
| **Table names** | Same as in MySQL (`users`, `orders`, …) | Renamed by template, e.g. `mysql1/${schema}/${table}` → `mysql1/mydb/users` ([sample config](https://github.com/ydb-platform/ydb-importer/blob/main/scripts/sample-mysql.xml)) |
| **Column names** | Preserved as in MySQL | Sanitized: spaces, `.`, `/`, `` ` `` → `_` ([`ColumnInfo.safeYdbColumnName`](https://github.com/ydb-platform/ydb-importer/blob/main/src/main/java/tech/ydb/importer/source/ColumnInfo.java)) |
| `AUTO_INCREMENT` | `BigSerial` + `ALTER SEQUENCE … START WITH` from `TABLES.AUTO_INCREMENT` | Plain `Int32`/`Int64`; no `BigSerial`, no sequence reset |
| `INT UNSIGNED`, `BIGINT UNSIGNED`, … | `Uint32` / `Uint64` | JDBC maps integers to signed `Int32`/`Int64` |
| `TINYINT(1)` | `Bool` | `TINYINT` → `Int32` (`BOOLEAN` → `Bool`) |
| `BIT` | `Uint64` | `Bool` (JDBC `Types.BIT`) |
| Secondary `KEY` / `UNIQUE KEY` | `INDEX … GLOBAL ASYNC` / `GLOBAL UNIQUE SYNC` in `CREATE TABLE` | Only `PRIMARY KEY` in DDL; no secondary indexes |
| `ENUM`, `SET`, `JSON` | `Text` | `Text` (similar) |
| `BLOB` / `BINARY` | Same column, inline `String` (bytes) | Column becomes `Int64` (blob id); payload split into a **separate table** `${schema}/${table}_${field}` with rows `(id, pos, val)` in 64 KB blocks ([`BlobReader`](https://github.com/ydb-platform/ydb-importer/blob/main/src/main/java/tech/ydb/importer/target/BlobReader.java)) |
| `TEXT` / `CLOB` (large text) | Inline `Text` in the main table | Optional separate CLOB table (32K-char blocks) or inline `Text` depending on config |
| Table **without primary key** | Schema unchanged; data read via `LIMIT/OFFSET` | Extra column `ydb_synth_key Text` added as PK (SHA-256 over row); **duplicate rows collapse** to one ([`SynthKey`](https://github.com/ydb-platform/ydb-importer/blob/main/src/main/java/tech/ydb/importer/target/SynthKey.java)) |
| `DATE` | `Date` | `Date32` by default (`conv-date=DATE_NEW`) |
| `DATETIME` / `TIMESTAMP` | `Timestamp` | `Datetime64` / `Timestamp64` by default |
| `TIME` | `Text` (fallback) | `Int32` (seconds since midnight) |
| `DECIMAL(p,s)` | `Decimal(22, 9)` for all | `Decimal(p,s)` when `allow-custom-decimal=true` |
| `YEAR` | `Uint16` | Not covered in MySQL type tests |
| Partitioning | `AUTO_PARTITIONING_BY_LOAD` only | `PARTITION_AT_KEYS`, source partition splits, column-store `HASH`, etc. |

Implementation: [`internal/ydb/schema.go`](internal/ydb/schema.go), [`internal/schema/columns.go`](internal/schema/columns.go).

### Examples of structural differences

**`AUTO_INCREMENT`** — MySQL `id BIGINT AUTO_INCREMENT` becomes:

```sql
`id` BigSerial NOT NULL,
PRIMARY KEY (`id`)
```

After `CREATE TABLE`, if `information_schema.TABLES.AUTO_INCREMENT` is known:

```sql
ALTER SEQUENCE `<db>/<table>/_serial_column_id` START WITH <next_value> RESTART
```

**`BLOB`** — MySQL table `attachments (id INT, data MEDIUMBLOB)`:

| | Main table column `data` | Where bytes live |
|---|---|---|
| **mysql2ydb** | `data String` | In the same row |
| **ydb-importer** | `data Int64` (reference id) | Separate table `…/attachments_data` with chunked rows |

With mysql2ydb you can `SELECT data FROM attachments WHERE id = 1` exactly like in MySQL. With ydb-importer you join the main table to the supplemental blob table (or read by id).

**Tables without PK** — mysql2ydb does not add columns; ydb-importer injects `ydb_synth_key` and changes row cardinality for identical tuples.

**Secondary indexes** — non-unique keys become `GLOBAL ASYNC`, unique keys (except PK) become `GLOBAL UNIQUE SYNC`. Tables with sync unique indexes automatically fall back to transactional `UPSERT` during data load, because `BulkUpsert` supports only async indexes ([`internal/ydb/writer.go`](internal/ydb/writer.go)).

## Efficient MySQL access

The importer minimizes round-trips to MySQL and keeps each trip bounded:

1. **Cursor-based pagination** over the primary key — `WHERE (pk…) > (?) ORDER BY pk LIMIT n`. No full-table scan into memory, no degrading `OFFSET` on large tables. Implementation: [`internal/mysql/reader.go`](internal/mysql/reader.go) (`ChunkReader.ReadChunk`).

2. **Pipelined read → write** — one goroutine reads the next chunk from MySQL while another writes the previous chunk to YDB. At most two chunks are in flight per table. Implementation: [`cmd/mysql2ydb/main.go`](cmd/mysql2ydb/main.go) (`transferTable`).

3. **Adaptive batch size** — chunk row count is capped by available RAM, average row size from `information_schema`, and a network budget (≤125 MB per fetch ≈ 10 s at 100 Mbit/s). Implementation: [`batchSizeForTable`](cmd/mysql2ydb/main.go) in `main.go`.

4. **Progress checkpoint** — after each successful `BulkUpsert`, cursor position and row count are saved to a YDB state table; restart resumes from the last chunk.

```
MySQL: SELECT chunk N  →  BulkUpsert(chunk N)  →  YDB
MySQL: SELECT chunk N+1  →  BulkUpsert(chunk N+1)  →  YDB
...
```

See [docs/DATA_MIGRATION.md](docs/DATA_MIGRATION.md) for architecture details.

## Features

- **One-to-one schema copy** — original table and column names; MySQL types mapped to the closest YDB types (`AUTO_INCREMENT` → `BigSerial`, `UNSIGNED` → `Uint32`/`Uint64`, secondary indexes, `TINYINT(1)` → `Bool`; see above).
- **Schema creation** — tables in YDB are created from MySQL metadata (`information_schema`).
- **Chunked data migration** — bounded memory, suitable for tables larger than RAM.
- **Idempotent writes** — `BulkUpsert` with `table.WithIdempotent()`; re-running does not create duplicates.
- **Resume** — per-table progress stored in YDB; interrupted migrations continue from the last chunk.

## Usage

MySQL parameters are read by default from **~/.my.cnf** (same as the `mysql` CLI), section `[client]`. The `-mysql` flag overrides the config file.

Example `~/.my.cnf`:

```ini
[client]
user = myuser
password = mypass
host = localhost
port = 3306
database = mydb
```

If the server requires secure connection (`require_secure_transport=ON`), add to `[client]`:

```ini
ssl-mode = REQUIRED
```

For a self-signed certificate (no verification):

```ini
ssl-mode = REQUIRED
ssl-verify = 0
```

(or `ssl=1` and `ssl-verify=0`).

```bash
go build -o mysql2ydb ./cmd/mysql2ydb

# with ~/.my.cnf (only -ydb is required)
./mysql2ydb -ydb "grpc://localhost:2136"

# or explicit DSN (ignores .my.cnf)
./mysql2ydb -mysql "user:password@tcp(localhost:3306)/mydb" -ydb "grpc://localhost:2136" -batch-size 10000
```

### Flags

| Flag | Description |
|------|-------------|
| `-mysql` | MySQL DSN (if set — overrides ~/.my.cnf) |
| `-ydb` | YDB endpoint (required) |
| `-ydb-database` | YDB database path (default `local`) |
| `-schema-only` | Only create schema, do not migrate data |
| `-data-only` | Only migrate data (schema must already exist) |
| `-batch-size` | Target chunk size in rows (default 10000) |
| `-max-chunk-rows` | Hard cap on rows per chunk (default 25000) |
| `-parallel-tables` | Number of tables to migrate in parallel (default 1) |
| `-tables` | Comma-separated list of tables (default — all) |
| `-force` | Re-transfer all tables, ignore completed state |
| `-force-recreate` | Drop all YDB tables and recreate schema from scratch |

### Examples

Schema only:

```bash
./mysql2ydb -mysql "..." -ydb "grpc://localhost:2136" -schema-only
```

Data only (after schema is created):

```bash
./mysql2ydb -mysql "..." -ydb "grpc://localhost:2136" -data-only -batch-size 5000
```

Migrate specific tables:

```bash
./mysql2ydb -mysql "..." -ydb "grpc://localhost:2136" -tables "users,orders"
```

## Chunked reading

- For tables with a **primary key**, cursor-based pagination is used: `WHERE (pk > ?) ORDER BY pk LIMIT batch_size`, which gives stable memory usage and acceptable speed on large volumes.
- For tables without a suitable key, `LIMIT batch_size OFFSET offset` is used (very large offsets may degrade performance).

## BulkUpsert idempotency

Writes to YDB are performed via `BulkUpsert` with the **WithIdempotent()** option:

- Re-sending the same batch does not create duplicate rows.
- Convenient when restarting migration or re-loading after a failure.

## Tests

```bash
go test ./internal/...
```
