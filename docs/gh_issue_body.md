When sending a large payload via `query.Client().DoTx` + `tx.Exec()` (e.g. UPSERT with `$data` parameter exceeding gRPC message limit), the driver returns `ResourceExhausted` ("trying to send message larger than max"). **The problem is that this error is currently treated as retryable.** Retrying does not change the situation — the payload size is the same, so every retry fails again with the same error. From the caller's perspective the process effectively **hangs** (repeated retries until timeout or context cancel), instead of failing fast so the client can reduce batch size or handle the error.

A similar situation was addressed for **BulkUpsert** and **Topic** writer (issue #1660 — split message before send). For the **Query** service (DoTx / Exec with large params), this specific subtype of `ResourceExhausted` ("message larger than max") should be treated as **non-retryable**, so that the error is returned to the client immediately. Alternatively, the SDK could handle large messages (e.g. split or document the limit) like for BulkUpsert/Topic.

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

- **Option A (recommended):** Treat `ResourceExhausted` with description "message larger than max" (and similar parameter size limits) as **non-retryable**, so the error is returned immediately and the client can reduce batch size or react.
- **Option B:** Or handle large messages in the SDK (e.g. split before send, or document the limit) like for BulkUpsert/Topic.

### Related

- #1660 — Topic writer: "message larger than grpc limit" (fixed by splitting message before send)
- #1511 / #1803 — Split bulk for BulkUpsert

### Workaround

Application-side: limit the number of rows (or total size) per `tx.Exec` so that the serialized `$data` parameter stays below the gRPC limit (64 MB) and YDB params limit (50 MB). This avoids the error but does not fix the SDK behaviour for other clients.
