# MySQL → YDB data migration

## Requirements

- The utility creates a schema in YDB from MySQL metadata and **migrates data**.
- Large tables must not be loaded entirely into memory — reading and writing are done in **chunks (batches)**.
- Writes to YDB use **idempotent BulkUpsert** (re-running does not create duplicates).

## Data migration architecture

### 1. Reading from MySQL in chunks

- **Chunk size** is set by the `--batch-size` parameter (default 10_000 rows).
- For tables with a primary key, **cursor-based pagination** is used:  
  `SELECT ... FROM table WHERE pk > ? ORDER BY pk LIMIT batch_size`  
  This gives stable memory usage and acceptable speed for large volumes.
- If no suitable key exists — `LIMIT batch_size OFFSET offset` is used, with a warning about the risk for very large offsets.

### 2. Writing to YDB

- **BulkUpsert** (YDB Table API) is used.
- The call is made with **table.WithIdempotent()** — the operation is idempotent; re-sending the same batch does not create duplicates.
- Each chunk from MySQL is sent in one `BulkUpsert` call; on error, retry is possible according to the SDK policy.

### 3. Data flow

```
MySQL (chunk 1) → BulkUpsert(chunk 1) → YDB
MySQL (chunk 2) → BulkUpsert(chunk 2) → YDB
...
```

Only one chunk (+ driver buffers) is held in memory at a time, which allows migrating tables larger than RAM.

### 4. Operation modes

- `--schema-only` — only create schema in YDB.
- `--data-only` — only migrate data (schema must already exist).
- Default — schema first, then data.

## BulkUpsert idempotency

- **BulkUpsert** in YDB has "insert or update" semantics by primary key.
- With **WithIdempotent()**, repeating the call with the same data is safe: no duplicate rows are created.
- This allows restarting migration from the last successful batch or re-running a table after failures.

## Limitations

- Data types must be compatible with the YDB schema (MySQL → YDB mapping is done when creating the schema).
- NULL and date/time types are handled according to the mapping (Optional in YDB where needed).
