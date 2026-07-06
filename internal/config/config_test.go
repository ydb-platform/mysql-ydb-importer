package config

import (
	"flag"
	"os"
	"testing"
)

func TestValidateYDBAuth(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name: "token only",
			cfg:  Config{YDBTokenFile: "/tmp/token"},
		},
		{
			name: "sa key only",
			cfg:  Config{YDBSAKeyFile: "/tmp/sa.json"},
		},
		{
			name: "metadata only",
			cfg:  Config{YDBYCMetadata: true},
		},
		{
			name:    "token and sa key",
			cfg:     Config{YDBTokenFile: "/tmp/token", YDBSAKeyFile: "/tmp/sa.json"},
			wantErr: true,
		},
		{
			name:    "token and metadata",
			cfg:     Config{YDBTokenFile: "/tmp/token", YDBYCMetadata: true},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.validateYDBAuth()
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateYDBAuth() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseYDBAuthConflict(t *testing.T) {
	origArgs := os.Args
	t.Cleanup(func() {
		os.Args = origArgs
		flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	})

	os.Args = []string{"mysql2ydb", "-ydb", "grpcs://host:2135", "-ydb-token-file", "a", "-ydb-sa-key-file", "b"}
	_, err := Parse()
	if err == nil {
		t.Fatal("expected auth conflict error")
	}
}
