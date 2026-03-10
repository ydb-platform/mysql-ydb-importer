# Bug report: ResourceExhausted "message larger than max" for Query service should be retryable

**Repository:** https://github.com/ydb-platform/ydb-go-sdk  
**Create issue:** https://github.com/ydb-platform/ydb-go-sdk/issues/new

---

## Title

**transport/ResourceExhausted (message larger than max) for Query.DoTx/Exec should be retryable**

## Description

When sending a large payload via `query.Client().DoTx` + `tx.Exec()` (e.g. UPSERT with `$data` parameter exceeding gRPC message limit), the driver returns `ResourceExhausted` and the operation is **not** retried. From the caller's perspective this looks like a hang or immediate failure, and the pool logs "query service pool try failed".

A similar situation was previously fixed for **BulkUpsert** (e.g. split bulk, or retry behaviour), and for **Topic** writer (issue #1660 — split message before send). The same class of error for the **Query** service (DoTx / Exec with large params) should be handled consistently: either treated as retryable so that the driver retries with backoff, or documented and/or handled so that clients can react (e.g. reduce batch size) instead of seeing pool try failures.

### Environment

- **ydb-go-sdk version:** 3.126.4
- **YDB:** Yandex Cloud (grpc)

### Error and stack trace

```
transport/ResourceExhausted (code = 8, source error = "rpc error: code = ResourceExhausted desc = trying to send message larger than max (70792102 vs. 64000000)", address: "vm-etnsdb9trdjenbo72eio-ru-central1-a-wqxz-acag.etnsdb9trdjenbo72eio.ydb.mdb.yandexcloud.net:2135", nodeID = 50828, traceID: "da41a95b-5729-480a-8375-c79af498ae2c") at `github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*grpcClientStream).SendMsg(grpc_client_stream.go:110)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.execute(execute_query.go:136)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).execute(session.go:170)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Transaction).Exec(transaction.go:238)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.doTx.func1(client.go:291)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/query.do.func1(client.go:224)` at `github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).try(pool.go:481)`
```

Log line:

```
WARN 'ydb.query.pool.try' => query service pool try failed {"latency":"82.247792ms","error":"transport/ResourceExhausted ...
```

### Expected behaviour

- **Option A:** This error is treated as **retryable** (like other `ResourceExhausted` in `retry/errors_data_test.go`), so the driver retries with backoff instead of failing the pool try immediately.
- **Option B:** Or the SDK handles large messages for Query.Exec similarly to BulkUpsert/Topic (e.g. document max message size, or split/cap so that requests stay under the limit).

### Related

- #1660 — Topic writer: "message larger than grpc limit" (fixed by splitting message before send)
- #1511 / #1803 — Split bulk for BulkUpsert

### Workaround

Application-side: limit the number of rows (or total size) per `tx.Exec` so that the serialized `$data` parameter stays below the gRPC limit (e.g. 50 MB to be safe). This avoids the error but does not fix the SDK behaviour for other clients.
