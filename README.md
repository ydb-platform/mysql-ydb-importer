# mysql2ydb

Utility for migrating schema and data from MySQL to YDB.

## Features

- **Schema creation** — tables in YDB are created from MySQL metadata (information_schema).
- **Data migration** — reading from MySQL in **chunks** (batches) so that large tables are not loaded entirely into memory.
- **Idempotent writes** — loading into YDB via **BulkUpsert** with `table.WithIdempotent()`, re-running does not create duplicates.

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
| `-batch-size` | Chunk size in rows (default 10000) |
| `-tables` | Comma-separated list of tables (default — all) |

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

- For tables with a **primary key**, cursor-based pagination is used: `WHERE (pk > ?) ORDER BY pk LIMIT batch_size`, which gives stable memory usage.
- For tables without a suitable key, `LIMIT batch_size OFFSET offset` is used (very large offsets may degrade performance).

## BulkUpsert idempotency

Writes to YDB are performed via `BulkUpsert` with the **WithIdempotent()** option:

- Re-sending the same batch does not create duplicate rows.
- Convenient when restarting migration or re-loading after a failure.

See [docs/DATA_MIGRATION.md](docs/DATA_MIGRATION.md) for details.

## Tests

```bash
go test ./internal/...
```
