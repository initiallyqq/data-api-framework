package service_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/initiallyqq/data-api-framework/service"
)

func TestLoadConfigFillsDefaultsAndRefusesWhatItCannotRunWith(t *testing.T) {
	write := func(lines ...string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "service.yaml")
		if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	required := []string{
		"databasePath: /var/lib/x/copy.db",
		"backupPath: /var/lib/x/last-good.db",
		"adminTokenFile: /run/secrets/x/admin-token",
		"dolphinDBAddress: dolphindb.example:8848",
		"dolphinDBCredentialsFile: /run/secrets/x/dolphindb.env",
		"dolphinDBDatabase: dfs://example",
	}
	config, err := service.LoadConfig(write(required...), ":8091")
	if err != nil {
		t.Fatal(err)
	}
	if config.ListenAddr != ":8091" || config.RequestTimeout != 30*time.Second ||
		config.SyncInterval != time.Hour || config.SyncRetryInterval != 5*time.Minute || config.MaxStaleness != 48*time.Hour {
		t.Fatalf("defaults = %+v", config)
	}
	for index := range required {
		missing := append(append([]string{}, required[:index]...), required[index+1:]...)
		if _, err := service.LoadConfig(write(missing...), ":8091"); err == nil {
			t.Fatalf("a configuration without %q was accepted", required[index])
		}
	}
	if _, err := service.LoadConfig(write("databasePath: /var/lib/x/copy.db", "backupPath: /var/lib/x/../x/copy.db", required[2], required[3], required[4], required[5]), ":8091"); err == nil {
		t.Fatal("one file as copy and backup was accepted")
	}
	// A key the service does not read, such as one an older release had,
	// fails the load instead of being ignored.
	for _, retired := range []string{"dolphinDBTables:\n  a: table_a", "syncIntervall: 1h"} {
		if _, err := service.LoadConfig(write(append(required, retired)...), ":8091"); err == nil {
			t.Fatalf("the key in %q was accepted", retired)
		}
	}
}
