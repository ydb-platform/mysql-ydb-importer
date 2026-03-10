package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseOption(t *testing.T) {
	key, value, ok := parseOption("user=myuser")
	if !ok || key != "user" || value != "myuser" {
		t.Errorf("parseOption(user=myuser) = %q, %q, %v", key, value, ok)
	}
	key, value, ok = parseOption("password = \"p@ss\"")
	if !ok || key != "password" || value != "p@ss" {
		t.Errorf("parseOption = %q, %q", key, value)
	}
	_, _, ok = parseOption("# comment")
	if ok {
		t.Error("comment should not parse")
	}
}

func TestReadMyCnfFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".my.cnf")
	err := os.WriteFile(path, []byte(`
[client]
user = testuser
password = testpass
host = db.example.com
port = 3307
database = mydb
`), 0600)
	if err != nil {
		t.Fatal(err)
	}

	opts, err := ReadMyCnfFile(path)
	if err != nil {
		t.Fatalf("ReadMyCnfFile: %v", err)
	}
	if opts.User != "testuser" || opts.Password != "testpass" || opts.Host != "db.example.com" || opts.Port != 3307 || opts.Database != "mydb" {
		t.Errorf("opts = %+v", opts)
	}
	dsn := opts.DSN()
	expect := "testuser:testpass@tcp(db.example.com:3307)/mydb"
	if dsn != expect {
		t.Errorf("DSN = %q, want %q", dsn, expect)
	}
}

func TestDSNTLS(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".my.cnf")
	err := os.WriteFile(path, []byte(`
[client]
user = u
password = p
host = localhost
database = d
ssl-mode = REQUIRED
ssl-verify = 0
`), 0600)
	if err != nil {
		t.Fatal(err)
	}
	opts, err := ReadMyCnfFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !opts.UseTLS || !opts.TLSSkipVerify {
		t.Errorf("UseTLS=%v TLSSkipVerify=%v", opts.UseTLS, opts.TLSSkipVerify)
	}
	dsn := opts.DSN()
	if !strings.Contains(dsn, "tls=skip-verify") {
		t.Errorf("DSN should contain tls=skip-verify: %s", dsn)
	}
}

func TestDSNDefaults(t *testing.T) {
	o := &MySQLOptions{User: "u", Database: "d"}
	dsn := o.DSN()
	if dsn != "u@tcp(127.0.0.1:3306)/d" {
		t.Errorf("DSN = %q", dsn)
	}
	o.Socket = "/var/run/mysqld.sock"
	dsn = o.DSN()
	if dsn != "u@unix(/var/run/mysqld.sock)/d" {
		t.Errorf("DSN with socket = %q", dsn)
	}
}
