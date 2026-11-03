package upstream_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/initiallyqq/data-api-framework/upstream"
)

// The credentials file is a shell env file: Connect reads DDB_USER and
// DDB_PASSWORD from it, quoted or not, behind export or not, and refuses a
// file without both or with shell syntax it would have to run. An accepted
// file reaches the dial (nothing listens at 127.0.0.1:1); with a DolphinDB
// to log in to (DATA_API_E2E_DOLPHINDB_ADDRESS), the accepted forms of its
// account connect.
func TestConnectReadsTheShellEnvFile(t *testing.T) {
	write := func(content string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "dolphindb.env")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	connect := func(address, credentials string) error {
		session, err := upstream.Connect(context.Background(), upstream.Source{Address: address, CredentialsFile: credentials, Database: "dfs://example"})
		if err == nil {
			session.Close()
		}
		return err
	}
	accepted := []string{
		"# account\nexport MYSQL_USER=ignored\nexport DDB_USER=admin\nDDB_PASSWORD='123456'\n",
		"DDB_USER=admin\nDDB_PASSWORD=\"123456\"\n",
		"DDB_USER=\"admin\"\nDDB_PASSWORD=123456\n",
	}
	for _, content := range accepted {
		if err := connect("127.0.0.1:1", write(content)); err == nil || !strings.Contains(err.Error(), "connect DolphinDB") {
			t.Fatalf("%q did not reach the dial: %v", content, err)
		}
	}
	for _, content := range []string{"DDB_USER=a\n", "DDB_PASSWORD=b\n", "DDB_USER=a\nDDB_PASSWORD=$(cat secret)\n", "DDB_USER=a\nDDB_PASSWORD='open\n", "DDB_USER=a\nDDB_PASSWORD=\"x\n"} {
		if err := connect("127.0.0.1:1", write(content)); err == nil || strings.Contains(err.Error(), "connect DolphinDB") {
			t.Fatalf("%q was accepted: %v", content, err)
		}
	}
	if err := connect("127.0.0.1:1", filepath.Join(t.TempDir(), "missing.env")); err == nil || strings.Contains(err.Error(), "connect DolphinDB") {
		t.Fatalf("a missing file was accepted: %v", err)
	}
	address := os.Getenv("DATA_API_E2E_DOLPHINDB_ADDRESS")
	if address == "" {
		t.Skip("DATA_API_E2E_DOLPHINDB_ADDRESS is not set")
	}
	for _, content := range accepted {
		if err := connect(address, write(content)); err != nil {
			t.Fatalf("%q did not log in: %v", content, err)
		}
	}
}
func TestNothingUnsafeIsWrittenIntoAScript(t *testing.T) {
	credentials := filepath.Join(t.TempDir(), "dolphindb.env")
	if err := os.WriteFile(credentials, []byte("DDB_USER=a\nDDB_PASSWORD=b\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, database := range []string{"example", "dfs://", "dfs://example\");drop", "dfs://ex ample"} {
		if _, err := upstream.Connect(context.Background(), upstream.Source{Address: "127.0.0.1:1", CredentialsFile: credentials, Database: database}); err == nil ||
			!strings.Contains(err.Error(), "dfs://<name>") {
			t.Fatalf("database %q: %v", database, err)
		}
	}
	for _, table := range []string{"", "x`y", "x y", "x\"); drop"} {
		if _, err := upstream.Select[struct{}](context.Background(), &upstream.Session{}, table, "*", ""); err == nil {
			t.Fatalf("table %q was accepted", table)
		}
	}
}

func TestDolphinDBDatesAndTimes(t *testing.T) {
	for value, want := range map[string]string{"2026.09.30": "2026-09-30", "2026-09-30": "2026-09-30", " 2026.01.02 ": "2026-01-02"} {
		if got, err := upstream.ParseDate("d", value); err != nil || got != want {
			t.Fatalf("ParseDate(%q) = %q, %v", value, got, err)
		}
	}
	for _, value := range []string{"", "2026/09/30", "30.09.2026", "2026.13.01"} {
		if _, err := upstream.ParseDate("d", value); err == nil || !strings.Contains(err.Error(), "d must be a DolphinDB DATE") {
			t.Fatalf("ParseDate(%q) = %v", value, err)
		}
	}
	for value, want := range map[string]string{
		"2026.08.27T22:08:29":     "2026-08-27T22:08:29+08:00",
		"2026.08.27T22:08:29.000": "2026-08-27T22:08:29+08:00",
		"2026-08-27T22:08:29.5":   "2026-08-27T22:08:29.5+08:00",
		// Beijing time is UTC+8 throughout: no daylight saving in 1988.
		"1988.07.01T12:00:00": "1988-07-01T12:00:00+08:00",
	} {
		if got, err := upstream.ParseDateTime("t", value); err != nil || got.Format(time.RFC3339Nano) != want {
			t.Fatalf("ParseDateTime(%q) = %v, %v; want %s", value, got, err, want)
		}
	}
	for _, value := range []string{"", "soon", "2026.08.27", "2026.08.27 22:08:29+08:00"} {
		if _, err := upstream.ParseDateTime("t", value); err == nil || !strings.Contains(err.Error(), "t must be a DolphinDB DATETIME") {
			t.Fatalf("ParseDateTime(%q) = %v", value, err)
		}
	}
}
