package config

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// MySQLOptions holds connection options from ~/.my.cnf [client] section.
type MySQLOptions struct {
	User          string
	Password      string
	Host          string
	Port          int
	Database      string
	Socket        string
	UseTLS        bool // ssl-mode=REQUIRED or ssl=1
	TLSSkipVerify bool // skip certificate verification (for self-signed certs)
}

// ReadMyCnf reads ~/.my.cnf and returns options from [client] and [mysql] sections.
// Keys from [client] take precedence over [mysql]. Missing values are zero.
func ReadMyCnf() (*MySQLOptions, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("home dir: %w", err)
	}
	return ReadMyCnfFile(filepath.Join(home, ".my.cnf"))
}

// ReadMyCnfFile reads options from the given path (INI with [client] and [mysql]).
func ReadMyCnfFile(path string) (*MySQLOptions, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	opts := &MySQLOptions{Port: 3306}
	inClient := false
	inMysql := false
	client := make(map[string]string)
	mysqlSec := make(map[string]string)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") {
			inClient = line == "[client]"
			inMysql = line == "[mysql]"
			continue
		}
		key, val, ok := parseOption(line)
		if !ok {
			continue
		}
		if inClient {
			client[key] = val
		} else if inMysql {
			mysqlSec[key] = val
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	// [mysql] first, then [client] overrides (same order as mysql client)
	for k, v := range mysqlSec {
		applyOption(opts, k, v)
	}
	for k, v := range client {
		applyOption(opts, k, v)
	}
	return opts, nil
}

func parseOption(line string) (key, value string, ok bool) {
	i := strings.IndexAny(line, "=:")
	if i < 0 {
		return "", "", false
	}
	key = strings.TrimSpace(strings.ToLower(line[:i]))
	value = strings.TrimSpace(line[i+1:])
	if key == "" {
		return "", "", false
	}
	value = strings.Trim(value, "\"'")
	return key, value, true
}

func applyOption(o *MySQLOptions, key, value string) {
	switch key {
	case "user":
		o.User = value
	case "password":
		o.Password = value
	case "host":
		o.Host = value
	case "port":
		if n, err := strconv.Atoi(value); err == nil && n > 0 {
			o.Port = n
		}
	case "database", "db":
		o.Database = value
	case "socket":
		o.Socket = value
	case "ssl-mode":
		switch strings.ToUpper(value) {
		case "REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY":
			o.UseTLS = true
		case "PREFERRED":
			o.UseTLS = true
		}
	case "ssl":
		o.UseTLS = parseBool(value)
	case "ssl-verify":
		o.TLSSkipVerify = !parseBool(value)
	}
}

func parseBool(s string) bool {
	s = strings.ToLower(s)
	return s == "1" || s == "true" || s == "yes" || s == "on"
}

// DSN builds a Go MySQL driver DSN from options.
// If database is empty, returns DSN without /dbname (caller may append).
func (o *MySQLOptions) DSN() string {
	user := o.User
	if user == "" {
		user = "root"
	}
	cred := user
	if o.Password != "" {
		cred = user + ":" + o.Password
	}
	port := o.Port
	if port <= 0 {
		port = 3306
	}
	var addr string
	if o.Socket != "" {
		addr = "unix(" + o.Socket + ")"
	} else {
		host := o.Host
		if host == "" {
			host = "127.0.0.1"
		}
		addr = "tcp(" + host + ":" + strconv.Itoa(port) + ")"
	}
	dsn := cred + "@" + addr
	if o.Database != "" {
		dsn += "/" + o.Database
	}
	if o.UseTLS {
		tlsVal := "true"
		if o.TLSSkipVerify {
			tlsVal = "skip-verify"
		}
		dsn += "?tls=" + tlsVal
	}
	return dsn
}
