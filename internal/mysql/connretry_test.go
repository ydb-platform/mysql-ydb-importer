package mysql

import (
	"errors"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
)

func TestIsConnError(t *testing.T) {
	if !IsConnError(mysqldriver.ErrInvalidConn) {
		t.Fatal("expected ErrInvalidConn")
	}
	if IsConnError(errors.New("syntax error")) {
		t.Fatal("unexpected match for unrelated error")
	}
	if !IsConnError(errors.New("sql: connection is already closed")) {
		t.Fatal("expected closed connection message")
	}
}

func TestWithConnRetry_givesUpOnNonConnError(t *testing.T) {
	calls := 0
	err := WithConnRetry(t.Context(), func() error {
		calls++
		return errors.New("permanent")
	})
	if err == nil || calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestWithConnRetry_retriesConnError(t *testing.T) {
	calls := 0
	err := WithConnRetry(t.Context(), func() error {
		calls++
		if calls < 3 {
			return mysqldriver.ErrInvalidConn
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}
