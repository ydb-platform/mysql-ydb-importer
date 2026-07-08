package ydb

import (
	"context"
	"errors"
	"time"
)

const progressFlushTimeout = 30 * time.Second

// SaveProgressFlushing persists migration progress, retrying once with a fresh context when ctx was canceled.
// This lets graceful shutdown flush the last checkpoint after in-flight work was canceled.
func SaveProgressFlushing(ctx context.Context, db TableClient, database, tableName string, nextCursor []interface{}, rowsSoFar int) error {
	err := SaveProgress(ctx, db, database, tableName, nextCursor, rowsSoFar)
	if err == nil || !errors.Is(err, context.Canceled) {
		return err
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), progressFlushTimeout)
	defer cancel()
	return SaveProgress(flushCtx, db, database, tableName, nextCursor, rowsSoFar)
}
