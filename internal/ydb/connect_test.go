package ydb

import "testing"

func TestConnectionString(t *testing.T) {
	tests := []struct {
		endpoint, database, want string
	}{
		{"grpcs://host:2135", "local", "grpcs://host:2135/local"},
		{"grpcs://host:2135/", "/ru-central1/db", "grpcs://host:2135/ru-central1/db"},
		{"grpcs://host:2135/?database=/ru-central1/db", "local", "grpcs://host:2135/?database=/ru-central1/db"},
	}
	for _, tt := range tests {
		if got := ConnectionString(tt.endpoint, tt.database); got != tt.want {
			t.Errorf("ConnectionString(%q, %q) = %q, want %q", tt.endpoint, tt.database, got, tt.want)
		}
	}
}
