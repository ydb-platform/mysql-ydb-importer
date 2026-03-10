package mysql

import (
	"testing"
)

func TestBuildCursorCondition(t *testing.T) {
	tests := []struct {
		pkCols []string
		want   string
	}{
		{[]string{"id"}, "(`id` > ?)"},
		{[]string{"a", "b"}, "(`a` > ? OR (`a` = ? AND `b` > ?))"},
		{[]string{"x", "y", "z"}, "(`x` > ? OR (`x` = ? AND `y` > ?) OR (`x` = ? AND `y` = ? AND `z` > ?))"},
	}
	for _, tt := range tests {
		got := buildCursorCondition(tt.pkCols)
		if got != tt.want {
			t.Errorf("buildCursorCondition(%v) = %q, want %q", tt.pkCols, got, tt.want)
		}
	}
}

func TestCursorFromRow(t *testing.T) {
	cols := []string{"id", "name", "ts"}
	row := []interface{}{int64(1), "a", "2024-01-01"}
	pkCols := []string{"id"}
	got := cursorFromRow(cols, row, pkCols)
	if len(got) != 1 || got[0].(int64) != 1 {
		t.Errorf("cursorFromRow = %v, want [1]", got)
	}
	pkCols2 := []string{"id", "ts"}
	got2 := cursorFromRow(cols, row, pkCols2)
	if len(got2) != 2 {
		t.Errorf("cursorFromRow(2 pk) = %v, len want 2", got2)
	}
}

func TestRowToMap(t *testing.T) {
	cols := []string{"a", "b"}
	var a, b interface{}
	a, b = 10, "x"
	dest := []interface{}{&a, &b}
	m := rowToMap(cols, dest)
	if m["a"] != 10 || m["b"] != "x" {
		t.Errorf("rowToMap = %v", m)
	}
}
