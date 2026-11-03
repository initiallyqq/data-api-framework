package replica_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/initiallyqq/data-api-framework/replica"
)

// item is the row of the test layout: one table, items, keyed by group and
// name.
type item struct {
	Group string `json:"group"`
	Name  string `json:"name"`
	Value string `json:"value"`
}

type itemLayout struct{}

func (itemLayout) Key(row item) string { return row.Group + "\x00" + row.Name }

func (itemLayout) Replace(ctx context.Context, conn *sql.Conn, rows []item) error {
	if _, err := conn.ExecContext(ctx, `
		DROP TABLE IF EXISTS items;
		CREATE TABLE items (grp TEXT NOT NULL, name TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (grp, name));`); err != nil {
		return err
	}
	for _, row := range rows {
		if _, err := conn.ExecContext(ctx, `INSERT INTO items VALUES (?, ?, ?)`, row.Group, row.Name, row.Value); err != nil {
			return err
		}
	}
	return nil
}

func (itemLayout) Load(ctx context.Context, q replica.Queryer) ([]item, error) {
	var tables int
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE name = 'items'`).Scan(&tables); err != nil || tables == 0 {
		return nil, err
	}
	result, err := q.QueryContext(ctx, `SELECT grp, name, value FROM items`)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	var rows []item
	for result.Next() {
		var row item
		if err := result.Scan(&row.Group, &row.Name, &row.Value); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, result.Err()
}

// failingLayout drops the table, as a replacement does first, and then
// fails, as a replacement that runs out of disk would.
type failingLayout struct{ itemLayout }

func (failingLayout) Replace(ctx context.Context, conn *sql.Conn, _ []item) error {
	if _, err := conn.ExecContext(ctx, `DROP TABLE IF EXISTS items`); err != nil {
		return err
	}
	return errors.New("disk full")
}

func openItems(t *testing.T, dir string, layout replica.Layout[item]) *replica.Store[item] {
	t.Helper()
	store, err := replica.Open(filepath.Join(dir, "copy.db"), filepath.Join(dir, "backup", "last-good.db"), 7, layout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return store
}

// file opens one of a store's files as the operator would, outside the
// store; it is closed before the test's directory is removed.
func file(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func items(values ...string) []item {
	var rows []item
	for i, value := range values {
		rows = append(rows, item{Group: "g", Name: fmt.Sprint("n", i), Value: value})
	}
	return rows
}

func mustState(t *testing.T, store *replica.Store[item]) replica.State {
	t.Helper()
	state, err := store.State(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return state
}

// Failure modes of the copy, listed before the store was written:
//   - read before the first synchronization (nothing to serve);
//   - a file another layout or schema version wrote;
//   - a source answer that is empty, or names one key twice;
//   - a replacement that fails half way;
//   - an identical answer that must not move the data version;
//   - any visible change that must move it, whatever the source's order;
//   - a damaged row or row count;
//   - a backup that is missing, stale or damaged;
//   - readers during a replacement;
//   - a restart.

func TestAFreshCopyHasNothingToServe(t *testing.T) {
	store := openItems(t, t.TempDir(), itemLayout{})
	if state := mustState(t, store); state.HasData || state.Digest != "" || state.Rows != 0 {
		t.Fatalf("fresh state = %+v", state)
	}
	called := false
	if err := store.Read(context.Background(), func(replica.Queryer) error { called = true; return nil }); !errors.Is(err, replica.ErrNoData) || called {
		t.Fatalf("read before the first synchronization = %v (reader called: %v), want ErrNoData", err, called)
	}
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("a second Migrate refused its own file: %v", err)
	}
}

func TestMigrateRefusesAFileItDidNotWrite(t *testing.T) {
	ctx := context.Background()
	for name, setup := range map[string]string{
		"an older layout":        `CREATE TABLE copy_state (singleton INTEGER PRIMARY KEY, schema_version INTEGER NOT NULL)`,
		"another schema version": `CREATE TABLE sync_state (singleton INTEGER PRIMARY KEY, schema_version INTEGER NOT NULL); INSERT INTO sync_state VALUES (1, 6)`,
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			store, err := replica.Open(filepath.Join(dir, "copy.db"), filepath.Join(dir, "last-good.db"), 7, replica.Layout[item](itemLayout{}))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if _, err := file(t, filepath.Join(dir, "copy.db")).ExecContext(ctx, setup); err != nil {
				t.Fatal(err)
			}
			if err := store.Migrate(ctx); err == nil {
				t.Fatal("Migrate accepted a file it did not write")
			}
		})
	}
}

func TestSynchronizeRefusesAnswersThatWouldReplaceTheCopyWithAWrongOne(t *testing.T) {
	ctx := context.Background()
	store := openItems(t, t.TempDir(), itemLayout{})
	if _, err := store.Synchronize(ctx, items("a", "b"), time.Now()); err != nil {
		t.Fatal(err)
	}
	before := mustState(t, store)
	for name, rows := range map[string][]item{
		"no rows":       nil,
		"repeated key":  {{Group: "g", Name: "n", Value: "a"}, {Group: "g", Name: "n", Value: "b"}},
		"identical row": {{Group: "g", Name: "n", Value: "a"}, {Group: "g", Name: "n", Value: "a"}},
	} {
		if _, err := store.Synchronize(ctx, rows, time.Now()); err == nil {
			t.Fatalf("%s: accepted", name)
		}
		if after := mustState(t, store); after != before {
			t.Fatalf("%s: a refused answer changed the state %+v -> %+v", name, before, after)
		}
	}
	if _, err := store.Synchronize(ctx, []item{{Group: "g", Name: "n"}, {Group: "g", Name: "n"}}, time.Now()); err == nil ||
		!strings.Contains(err.Error(), "g/n") {
		t.Fatalf("the refusal must name the key: %v", err)
	}
}

func TestAFailedReplacementKeepsTheCopy(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := openItems(t, dir, itemLayout{})
	if _, err := store.Synchronize(ctx, items("a"), time.Now()); err != nil {
		t.Fatal(err)
	}
	before := mustState(t, store)
	failing := openItems(t, dir, failingLayout{})
	if _, err := failing.Synchronize(ctx, items("b"), time.Now().Add(time.Hour)); err == nil {
		t.Fatal("a failed replacement reported success")
	}
	if after := mustState(t, store); after != before {
		t.Fatalf("a failed replacement changed the state: %+v -> %+v", before, after)
	}
}

func TestOnlyAVisibleChangeMovesTheDataVersion(t *testing.T) {
	ctx := context.Background()
	store := openItems(t, t.TempDir(), itemLayout{})
	first := time.Date(2026, 11, 3, 8, 0, 0, 0, time.UTC)
	result, err := store.Synchronize(ctx, items("a", "b", "c"), first)
	if err != nil || !result.Changed || result.Rows != 3 || result.BackupError != nil {
		t.Fatalf("first synchronization = %+v, %v", result, err)
	}
	before := mustState(t, store)

	// The same rows in another order are the same copy.
	shuffled := items("a", "b", "c")
	shuffled[0], shuffled[2] = shuffled[2], shuffled[0]
	result, err = store.Synchronize(ctx, shuffled, first.Add(time.Hour))
	if err != nil || result.Changed {
		t.Fatalf("identical synchronization = %+v, %v", result, err)
	}
	after := mustState(t, store)
	if after.Digest != before.Digest || !after.DataChangedAt.Equal(before.DataChangedAt) || !after.LastSourceSuccessAt.Equal(first.Add(time.Hour)) {
		t.Fatalf("an identical synchronization moved the data version or missed the check: %+v -> %+v", before, after)
	}

	for name, rows := range map[string][]item{
		"value changed": items("a", "b", "x"),
		"row dropped":   items("a", "b"),
		"row added":     items("a", "b", "c", "d"),
		"key changed":   {{Group: "h", Name: "n0", Value: "a"}, {Group: "g", Name: "n1", Value: "b"}, {Group: "g", Name: "n2", Value: "c"}},
	} {
		t.Run(name, func(t *testing.T) {
			store := openItems(t, t.TempDir(), itemLayout{})
			if _, err := store.Synchronize(ctx, items("a", "b", "c"), first); err != nil {
				t.Fatal(err)
			}
			before := mustState(t, store)
			result, err := store.Synchronize(ctx, rows, first.Add(time.Hour))
			after := mustState(t, store)
			if err != nil || !result.Changed || after.Digest == before.Digest || after.Rows != len(rows) {
				t.Fatalf("change not applied: %+v, %v; state %+v -> %+v", result, err, before, after)
			}
			var loaded []item
			err = store.Read(ctx, func(q replica.Queryer) (err error) {
				loaded, err = itemLayout{}.Load(ctx, q)
				return err
			})
			if err != nil || len(loaded) != len(rows) {
				t.Fatalf("stored rows = %+v, %v", loaded, err)
			}
		})
	}
}

func TestADamagedCopyIsNeverTakenForValid(t *testing.T) {
	ctx := context.Background()
	for name, damage := range map[string]string{
		"row rewritten": `UPDATE items SET value = 'x' WHERE name = 'n0'`,
		"row deleted":   `DELETE FROM items WHERE name = 'n1'`,
		"count wrong":   `UPDATE sync_state SET row_count = 5`,
		"digest wiped":  `UPDATE sync_state SET state_digest = ''`,
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			store := openItems(t, dir, itemLayout{})
			result, err := store.Synchronize(ctx, items("a", "b"), time.Now())
			if err != nil {
				t.Fatal(err)
			}
			digest := mustState(t, store).Digest
			if _, err := file(t, filepath.Join(dir, "copy.db")).ExecContext(ctx, damage); err != nil {
				t.Fatal(err)
			}
			if _, err := store.State(ctx); err == nil {
				t.Fatal("a damaged copy passed the state check")
			}
			// The backup taken before the damage still holds the copy.
			if result.BackupError != nil || store.CheckBackup(ctx, digest) != nil {
				t.Fatalf("backup = %v / %v", result.BackupError, store.CheckBackup(ctx, digest))
			}
		})
	}
}

func TestTheBackupFollowsTheCopy(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := openItems(t, dir, itemLayout{})
	backupPath := filepath.Join(dir, "backup", "last-good.db")
	if _, err := store.Synchronize(ctx, items("a"), time.Now()); err != nil {
		t.Fatal(err)
	}
	digest := mustState(t, store).Digest
	if err := store.CheckBackup(ctx, digest); err != nil {
		t.Fatalf("fresh backup: %v", err)
	}
	if err := store.CheckBackup(ctx, "sha256:another"); err == nil {
		t.Fatal("the backup matched another digest")
	}
	// A missing backup fails the check, and the next synchronization, though
	// nothing changed, writes it again.
	if err := os.Remove(backupPath); err != nil {
		t.Fatal(err)
	}
	if err := store.CheckBackup(ctx, digest); err == nil {
		t.Fatal("a missing backup passed")
	}
	if result, err := store.Synchronize(ctx, items("a"), time.Now()); err != nil || result.Changed || result.BackupError != nil {
		t.Fatalf("resync = %+v, %v", result, err)
	}
	if err := store.CheckBackup(ctx, digest); err != nil {
		t.Fatalf("backup not restored: %v", err)
	}
	// A damaged backup fails the check.
	backup, err := sql.Open("sqlite", backupPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backup.Exec(`UPDATE items SET value = 'x'`); err != nil {
		t.Fatal(err)
	}
	_ = backup.Close()
	if err := store.CheckBackup(ctx, digest); err == nil {
		t.Fatal("a damaged backup passed")
	}
	// A backup that cannot be written does not fail the synchronization.
	if err := os.RemoveAll(filepath.Dir(backupPath)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Dir(backupPath), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if result, err := store.Synchronize(ctx, items("b"), time.Now()); err != nil || !result.Changed || result.BackupError == nil {
		t.Fatalf("synchronization with an unwritable backup = %+v, %v", result, err)
	}
}

func TestReadersSeeOneCompleteCopyDuringAReplacement(t *testing.T) {
	ctx := context.Background()
	store := openItems(t, t.TempDir(), itemLayout{})
	generation := func(value string) []item {
		rows := make([]item, 200)
		for i := range rows {
			rows[i] = item{Group: "g", Name: fmt.Sprintf("n%03d", i), Value: value}
		}
		return rows
	}
	if _, err := store.Synchronize(ctx, generation("old"), time.Now()); err != nil {
		t.Fatal(err)
	}
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		readErr error
	)
	for range 8 {
		wg.Go(func() {
			for range 30 {
				err := store.Read(ctx, func(q replica.Queryer) error {
					rows, err := itemLayout{}.Load(ctx, q)
					if err != nil {
						return err
					}
					if len(rows) != 200 {
						return fmt.Errorf("reader saw %d rows", len(rows))
					}
					for _, row := range rows {
						if row.Value != rows[0].Value {
							return fmt.Errorf("reader saw %q beside %q", row.Value, rows[0].Value)
						}
					}
					return nil
				})
				if err != nil {
					mu.Lock()
					readErr = err
					mu.Unlock()
					return
				}
			}
		})
	}
	for _, value := range []string{"new", "newer", "newest"} {
		if _, err := store.Synchronize(ctx, generation(value), time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	if readErr != nil {
		t.Fatal(readErr)
	}
}

func TestTheCopySurvivesARestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := openItems(t, dir, itemLayout{})
	if _, err := store.Synchronize(ctx, items("a", "b"), time.Now()); err != nil {
		t.Fatal(err)
	}
	before := mustState(t, store)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openItems(t, dir, itemLayout{})
	if after := mustState(t, reopened); after != before || !after.HasData {
		t.Fatalf("after a restart %+v, before %+v", after, before)
	}
	if err := reopened.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}
