package shutdown

import (
	"context"
	"errors"
	"testing"
)

func TestIsInterrupt(t *testing.T) {
	if IsInterrupt(nil) {
		t.Fatal("nil error is not an interrupt")
	}
	if IsInterrupt(errors.New("other")) {
		t.Fatal("generic error is not an interrupt")
	}
	if !IsInterrupt(context.Canceled) {
		t.Fatal("context.Canceled should be interrupt")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !IsInterrupt(ctx.Err()) {
		t.Fatal("canceled context err should be interrupt")
	}
}

func TestExitCode(t *testing.T) {
	if ExitCode(nil) != 1 {
		t.Fatalf("nil err exit code = %d, want 1", ExitCode(nil))
	}
	if ExitCode(context.Canceled) != 130 {
		t.Fatalf("interrupt exit code = %d, want 130", ExitCode(context.Canceled))
	}
}
