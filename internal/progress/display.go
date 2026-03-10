package progress

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	barWidth   = 24
	redrawRate = 200 * time.Millisecond
)

// Display shows one progress bar per table. Call Update when rows change, SetDone when table finishes.
// Run must be started before transfers; call Stop when all transfers are done.
type Display struct {
	mu       sync.Mutex
	tables   []*tableState
	index    map[string]int // name -> index in tables
	stopped  bool
	spinner  int
	isTerminal bool
}

type tableState struct {
	name  string
	rows  int
	done  bool
	total int // set when done
}

// NewDisplay creates a display with one line per table name (order preserved).
func NewDisplay(tableNames []string) *Display {
	tables := make([]*tableState, len(tableNames))
	index := make(map[string]int, len(tableNames))
	for i, name := range tableNames {
		tables[i] = &tableState{name: name}
		index[name] = i
	}
	isTerminal := isTerminal(os.Stdout)
	return &Display{tables: tables, index: index, isTerminal: isTerminal}
}

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

// Update sets the current row count for the table (call after each chunk).
func (d *Display) Update(tableName string, rows int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if i, ok := d.index[tableName]; ok {
		d.tables[i].rows = rows
	}
}

// SetDone marks the table as completed with total rows.
func (d *Display) SetDone(tableName string, total int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if i, ok := d.index[tableName]; ok {
		d.tables[i].done = true
		d.tables[i].total = total
		d.tables[i].rows = total
	}
}

// Run starts the redraw loop. Call from a goroutine; it exits when ctx is done or Stop is called.
func (d *Display) Run(ctx context.Context) {
	if !d.isTerminal {
		return
	}
	ticker := time.NewTicker(redrawRate)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.mu.Lock()
			stop := d.stopped
			d.mu.Unlock()
			if stop {
				return
			}
			d.redraw()
		}
	}
}

// ReserveLines prints newlines so the progress bars have space (call once before Run).
func (d *Display) ReserveLines() {
	if !d.isTerminal {
		return
	}
	for range d.tables {
		fmt.Fprintln(os.Stdout)
	}
}

// Stop stops the redraw loop and prints a final state (all done).
func (d *Display) Stop() {
	d.mu.Lock()
	d.stopped = true
	d.mu.Unlock()
	if d.isTerminal {
		d.redraw()
		// Show cursor again
		fmt.Fprint(os.Stdout, "\033[?25h")
	}
}

func (d *Display) redraw() {
	d.mu.Lock()
	tables := make([]tableState, len(d.tables))
	for i, t := range d.tables {
		tables[i] = *t
	}
	d.spinner = (d.spinner + 1) % 4
	spinnerChar := []byte{'-', '\\', '|', '/'}[d.spinner]
	d.mu.Unlock()

	if !d.isTerminal {
		return
	}
	// Move cursor up to first progress line, then overwrite each line
	fmt.Fprintf(os.Stdout, "\033[?25l\033[%dA", len(tables))
	for _, t := range tables {
		line := formatLine(t, spinnerChar)
		fmt.Fprintf(os.Stdout, "\033[2K\r%s\n", line)
	}
}

func formatLine(t tableState, spinner byte) string {
	namePad := 20
	if len(t.name) > namePad {
		namePad = len(t.name)
	}
	name := t.name + strings.Repeat(" ", namePad-len(t.name))
	if t.done {
		bar := strings.Repeat("=", barWidth)
		return fmt.Sprintf("  %s [%s] %d rows done", name, bar, t.total)
	}
	// Indeterminate bar: fill by rows/1000, cap at barWidth
	filled := t.rows / 1000
	if filled > barWidth {
		filled = barWidth
	}
	// When filled == barWidth, barWidth-filled-1 would be -1 and Repeat would panic
	rest := barWidth - filled - 1
	if rest < 0 {
		rest = 0
	}
	bar := strings.Repeat("=", filled) + string(spinner) + strings.Repeat(" ", rest)
	return fmt.Sprintf("  %s [%s] %d rows", name, bar, t.rows)
}
