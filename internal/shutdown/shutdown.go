package shutdown

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
)

// NotifyContext returns a context canceled on SIGINT or SIGTERM.
func NotifyContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case sig := <-sigCh:
			log.Printf("Received %v, shutting down gracefully (saving migration state)...", sig)
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, func() {
		signal.Stop(sigCh)
		cancel()
	}
}

// IsInterrupt reports whether err is a graceful shutdown (context canceled after a signal).
func IsInterrupt(err error) bool {
	return errors.Is(err, context.Canceled)
}

// ExitCode returns a conventional exit code for err (130 for interrupt).
func ExitCode(err error) int {
	if IsInterrupt(err) {
		return 130
	}
	return 1
}

// LogInterrupt logs that migration was interrupted and can be resumed.
func LogInterrupt() {
	log.Println("Interrupted, migration progress saved. Re-run to resume.")
}

// Exit exits the process with the code appropriate for err.
func Exit(err error) {
	if IsInterrupt(err) {
		LogInterrupt()
		os.Exit(130)
	}
	if err != nil {
		os.Exit(1)
	}
}
