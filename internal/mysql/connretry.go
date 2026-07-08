package mysql

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
)

const connRetryAttempts = 3

// IsConnError reports whether err is a recoverable MySQL connection error.
func IsConnError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, mysqldriver.ErrInvalidConn) || errors.Is(err, driver.ErrBadConn) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "invalid connection") ||
		strings.Contains(msg, "bad connection") ||
		strings.Contains(msg, "connection is already closed")
}

// WithConnRetry runs fn up to connRetryAttempts times on transient connection errors.
func WithConnRetry(ctx context.Context, fn func() error) error {
	var err error
	for attempt := 0; attempt < connRetryAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err = fn()
		if err == nil || !IsConnError(err) {
			return err
		}
		if attempt+1 == connRetryAttempts {
			break
		}
		backoff := time.Duration(attempt+1) * 200 * time.Millisecond
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}
