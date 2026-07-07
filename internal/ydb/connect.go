package ydb

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/mysql2ydb/mysql2ydb/internal/config"
	yc "github.com/ydb-platform/ydb-go-yc"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"
)

// ConnectionString builds a YDB DSN from endpoint and database path.
func ConnectionString(endpoint, database string) string {
	if strings.Contains(endpoint, "?database=") {
		return endpoint
	}
	if database == "" {
		return endpoint
	}
	return strings.TrimSuffix(endpoint, "/") + "/" + strings.TrimPrefix(database, "/")
}

func authOptions(cfg *config.Config) ([]ydbsdk.Option, error) {
	var opts []ydbsdk.Option
	switch {
	case cfg.YDBTokenFile != "":
		token, err := os.ReadFile(cfg.YDBTokenFile)
		if err != nil {
			return nil, fmt.Errorf("ydb-token-file: %w", err)
		}
		opts = append(opts, ydbsdk.WithAccessTokenCredentials(strings.TrimSpace(string(token))))
	case cfg.YDBSAKeyFile != "":
		opts = append(opts, yc.WithServiceAccountKeyFileCredentials(cfg.YDBSAKeyFile))
	case cfg.YDBYCMetadata:
		opts = append(opts, yc.WithMetadataCredentials())
	}
	return opts, nil
}

// Open connects to YDB using project config (TLS CA, credentials, connect timeout).
func Open(ctx context.Context, cfg *config.Config, extraOpts ...ydbsdk.Option) (*ydbsdk.Driver, error) {
	dsn := ConnectionString(cfg.YDBEndpoint, cfg.YDBDatabase)

	opts := append([]ydbsdk.Option{}, extraOpts...)
	opts = append(opts, yc.WithInternalCA())
	authOpts, err := authOptions(cfg)
	if err != nil {
		return nil, err
	}
	opts = append(opts, authOpts...)

	db, err := ydbsdk.Open(ctx, dsn, opts...)
	if err != nil {
		return nil, err
	}
	return db, nil
}
