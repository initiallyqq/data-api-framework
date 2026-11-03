// Package replica is a data service's local copy of its source tables in
// SQLite, and the synchronization that keeps it current.
package replica

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	"modernc.org/sqlite"
)

// ErrNoData is a read before the first synchronization: there is nothing
// trustworthy to serve yet.
var ErrNoData = errors.New("no synchronization has completed yet")

// Queryer is what a layout reads through: the database or one transaction.
type Queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// Layout is how a service keeps its rows in SQLite, beside the store's own
// sync_state table.
type Layout[R any] interface {
	// Key identifies a row. Rows are kept and digested in key order, and two
	// rows with one key are refused.
	Key(R) string
	// Replace drops the layout's tables and writes rows, in key order, in
	// their place, inside the caller's write transaction.
	Replace(ctx context.Context, conn *sql.Conn, rows []R) error
	// Load reads every stored row; before the first synchronization, none.
	Load(ctx context.Context, q Queryer) ([]R, error)
}

// Store is a service's local copy of its source tables: one SQLite file, and
// a last-good backup of it beside. Each synchronization replaces the copy
// whole, in one transaction, and only when its content changed; the digest
// of the content, kept with it, is recomputed from the stored rows at start,
// on every backup and on every backup check, so a damaged copy is never
// served or backed up as valid.
type Store[R any] struct {
	db         *sql.DB
	backupPath string
	version    int
	layout     Layout[R]
}

// State is what sync_state records about the copy.
type State struct {
	HasData             bool
	Digest              string
	Rows                int
	LastSourceSuccessAt time.Time
	DataChangedAt       time.Time
}

// SyncResult is one synchronization's outcome. A failed backup does not fail
// it: the copy is current, the service reports the backup as degraded.
type SyncResult struct {
	Changed     bool
	Rows        int
	BackupError error
}

type onlineBackupConn interface {
	NewBackup(string) (*sqlite.Backup, error)
}

// Open opens the copy at path, laid out by layout; version is the
// layout's schema version, which sync_state records.
func Open[R any](path, backupPath string, version int, layout Layout[R]) (*Store[R], error) {
	for _, dir := range []string{filepath.Dir(path), filepath.Dir(backupPath)} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("create SQLite directory: %w", err)
		}
	}
	db, err := sql.Open("sqlite", sqliteDSN(path, false))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("open SQLite database: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("protect SQLite database: %w", err)
	}
	return &Store[R]{db: db, backupPath: backupPath, version: version, layout: layout}, nil
}

func sqliteDSN(path string, readOnly bool) string {
	absolute, err := filepath.Abs(path)
	if err != nil {
		absolute = path
	}
	urlPath := filepath.ToSlash(absolute)
	if filepath.VolumeName(absolute) != "" && !strings.HasPrefix(urlPath, "/") {
		urlPath = "/" + urlPath
	}
	uri := url.URL{Scheme: "file", Path: urlPath}
	query := uri.Query()
	if readOnly {
		query.Set("mode", "ro")
		query.Set("immutable", "1")
	} else {
		query.Set("_busy_timeout", "5000")
		query.Set("_journal_mode", "wal")
		query.Set("_synchronous", "full")
	}
	uri.RawQuery = query.Encode()
	return uri.String()
}

func (s *Store[R]) Close() error { return s.db.Close() }

// Migrate prepares a new file and accepts an existing one only if this
// layout's schema version wrote it. A file of another version, or one laid
// out before sync_state existed, is refused rather than read: the service
// rebuilds its copy from the source in a new file faster than it could
// convert one.
func (s *Store[R]) Migrate(ctx context.Context) error {
	tables, err := tableNames(ctx, s.db)
	if err != nil {
		return fmt.Errorf("list SQLite tables: %w", err)
	}
	if !slices.Contains(tables, "sync_state") {
		if len(tables) > 0 {
			return fmt.Errorf("SQLite database holds %s but no sync_state: another layout wrote it; move it aside", strings.Join(tables, ", "))
		}
		return s.initialize(ctx)
	}
	var version int
	if err := s.db.QueryRowContext(ctx, `SELECT schema_version FROM sync_state WHERE singleton = 1`).Scan(&version); err != nil {
		return fmt.Errorf("read SQLite schema version: %w", err)
	}
	if version != s.version {
		return fmt.Errorf("SQLite schema version %d is unsupported; want %d", version, s.version)
	}
	return nil
}

func (s *Store[R]) initialize(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
		CREATE TABLE sync_state (
		    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
		    schema_version INTEGER NOT NULL,
		    state_digest TEXT NOT NULL DEFAULT '',
		    row_count INTEGER NOT NULL DEFAULT 0,
		    last_source_success_at TEXT NOT NULL DEFAULT '',
		    data_changed_at TEXT NOT NULL DEFAULT ''
		)`); err != nil {
		return fmt.Errorf("initialize SQLite schema: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO sync_state (singleton, schema_version) VALUES (1, ?)`, s.version); err != nil {
		return fmt.Errorf("initialize SQLite schema: %w", err)
	}
	return tx.Commit()
}

func tableNames(ctx context.Context, q Queryer) ([]string, error) {
	rows, err := q.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

// Ping is the health check's: the file opens and passes quick_check.
func (s *Store[R]) Ping(ctx context.Context) error {
	if err := s.db.PingContext(ctx); err != nil {
		return err
	}
	return quickCheck(ctx, s.db)
}

func quickCheck(ctx context.Context, db *sql.DB) error {
	var result string
	if err := db.QueryRowContext(ctx, `PRAGMA quick_check`).Scan(&result); err != nil {
		return fmt.Errorf("SQLite quick_check: %w", err)
	}
	if result != "ok" {
		return fmt.Errorf("SQLite quick_check returned %q", result)
	}
	return nil
}

// State reads sync_state and checks the stored rows against it.
func (s *Store[R]) State(ctx context.Context) (State, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return State{}, err
	}
	defer tx.Rollback()
	state, err := s.verify(ctx, tx)
	if err != nil {
		return State{}, err
	}
	return state, tx.Commit()
}

func (s *Store[R]) verify(ctx context.Context, q Queryer) (State, error) {
	var (
		state                    State
		lastSuccess, dataChanged string
	)
	if err := q.QueryRowContext(ctx, `
		SELECT state_digest, row_count, last_source_success_at, data_changed_at
		FROM sync_state WHERE singleton = 1`,
	).Scan(&state.Digest, &state.Rows, &lastSuccess, &dataChanged); err != nil {
		return State{}, err
	}
	var err error
	if lastSuccess != "" {
		if state.LastSourceSuccessAt, err = time.Parse(time.RFC3339Nano, lastSuccess); err != nil {
			return State{}, fmt.Errorf("parse last source success time: %w", err)
		}
	}
	if dataChanged != "" {
		if state.DataChangedAt, err = time.Parse(time.RFC3339Nano, dataChanged); err != nil {
			return State{}, fmt.Errorf("parse data changed time: %w", err)
		}
	}
	rows, err := s.layout.Load(ctx, q)
	if err != nil {
		return State{}, err
	}
	state.HasData = state.Digest != ""
	if !state.HasData {
		if len(rows) != 0 || state.Rows != 0 {
			return State{}, errors.New("SQLite holds rows but no digest")
		}
		return state, nil
	}
	if state.LastSourceSuccessAt.IsZero() || state.DataChangedAt.IsZero() {
		return State{}, errors.New("SQLite state is incomplete")
	}
	if state.Rows != len(rows) {
		return State{}, fmt.Errorf("SQLite row count mismatch: stored %d counted %d", state.Rows, len(rows))
	}
	calculated, err := s.digest(s.sorted(rows))
	if err != nil {
		return State{}, err
	}
	if calculated != state.Digest {
		return State{}, fmt.Errorf("SQLite state digest mismatch: stored %s calculated %s", state.Digest, calculated)
	}
	return state, nil
}

func (s *Store[R]) sorted(rows []R) []R {
	sorted := slices.Clone(rows)
	slices.SortFunc(sorted, func(a, b R) int { return strings.Compare(s.layout.Key(a), s.layout.Key(b)) })
	return sorted
}

// digest hashes rows, in key order, as JSON lines after the schema version.
func (s *Store[R]) digest(rows []R) (string, error) {
	sum := sha256.New()
	fmt.Fprintf(sum, "schema=%d\n", s.version)
	for _, row := range rows {
		line, err := json.Marshal(row)
		if err != nil {
			return "", err
		}
		sum.Write(line)
		sum.Write([]byte{'\n'})
	}
	return "sha256:" + hex.EncodeToString(sum.Sum(nil)), nil
}

// Read runs read in one read-only transaction, so all it queries is one
// synchronized state. Before the first synchronization it fails with
// ErrNoData.
func (s *Store[R]) Read(ctx context.Context, read func(Queryer) error) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var digest string
	if err := tx.QueryRowContext(ctx, `SELECT state_digest FROM sync_state WHERE singleton = 1`).Scan(&digest); err != nil {
		return err
	}
	if digest == "" {
		return ErrNoData
	}
	if err := read(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// Synchronize makes rows the copy: it replaces the stored rows when their
// content changed and otherwise only records the successful source check,
// then backs the copy up if the backup no longer matches it. No rows, or two
// rows with one key, are refused before anything is written: an empty or
// ambiguous source must never replace what is served.
func (s *Store[R]) Synchronize(ctx context.Context, rows []R, sourceSuccessAt time.Time) (SyncResult, error) {
	if len(rows) == 0 {
		return SyncResult{}, errors.New("the source returned no rows; refusing to clear the local copy")
	}
	sorted := s.sorted(rows)
	for index := 1; index < len(sorted); index++ {
		if key := s.layout.Key(sorted[index]); key == s.layout.Key(sorted[index-1]) {
			return SyncResult{}, fmt.Errorf("two rows have the key %s", strings.ReplaceAll(key, "\x00", "/"))
		}
	}
	digest, err := s.digest(sorted)
	if err != nil {
		return SyncResult{}, err
	}
	at := sourceSuccessAt.UTC().Format(time.RFC3339Nano)

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return SyncResult{}, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return SyncResult{}, err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	var current string
	if err := conn.QueryRowContext(ctx, `SELECT state_digest FROM sync_state WHERE singleton = 1`).Scan(&current); err != nil {
		return SyncResult{}, err
	}
	changed := current != digest
	if changed {
		if err := s.layout.Replace(ctx, conn, sorted); err != nil {
			return SyncResult{}, err
		}
		if _, err := conn.ExecContext(ctx, `
			UPDATE sync_state
			SET state_digest = ?, row_count = ?, last_source_success_at = ?, data_changed_at = ?
			WHERE singleton = 1`,
			digest, len(sorted), at, at,
		); err != nil {
			return SyncResult{}, err
		}
	} else if _, err := conn.ExecContext(ctx,
		`UPDATE sync_state SET last_source_success_at = ? WHERE singleton = 1`, at,
	); err != nil {
		return SyncResult{}, err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return SyncResult{}, err
	}
	committed = true

	result := SyncResult{Changed: changed, Rows: len(sorted)}
	stale := changed
	if !changed {
		matches, err := s.backupMatches(ctx, s.backupPath, digest)
		stale = err != nil || !matches
	}
	if stale {
		if err := s.createBackup(ctx, digest); err != nil {
			result.BackupError = fmt.Errorf("create SQLite last-good backup: %w", err)
		}
	}
	return result, nil
}

// CheckBackup tells whether the last-good backup holds the copy whose digest
// is digest, checked from its rows.
func (s *Store[R]) CheckBackup(ctx context.Context, digest string) error {
	matches, err := s.backupMatches(ctx, s.backupPath, digest)
	if err != nil {
		return fmt.Errorf("check SQLite last-good backup: %w", err)
	}
	if !matches {
		return errors.New("SQLite last-good backup is missing or does not match current state")
	}
	return nil
}

func (s *Store[R]) backupMatches(ctx context.Context, path, digest string) (bool, error) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	db, err := sql.Open("sqlite", sqliteDSN(path, true))
	if err != nil {
		return false, err
	}
	defer db.Close()
	if err := quickCheck(ctx, db); err != nil {
		return false, err
	}
	state, err := s.verify(ctx, db)
	if err != nil {
		return false, err
	}
	return state.HasData && state.Digest == digest, nil
}

// createBackup copies the database online into a temporary file, checks the
// copy's rows against digest, and only then renames it over the backup.
func (s *Store[R]) createBackup(ctx context.Context, digest string) error {
	tempPath := s.backupPath + ".tmp"
	if err := os.Remove(tempPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	err = conn.Raw(func(driverConn any) error {
		source, ok := driverConn.(onlineBackupConn)
		if !ok {
			return fmt.Errorf("SQLite driver does not expose online backup")
		}
		backup, err := source.NewBackup(tempPath)
		if err != nil {
			return err
		}
		if _, err := backup.Step(-1); err != nil {
			_ = backup.Finish()
			return err
		}
		destination, err := backup.Commit()
		if err != nil {
			return err
		}
		return closeDriverConn(destination)
	})
	if err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	if err := os.Chmod(tempPath, 0o600); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	matches, err := s.backupMatches(ctx, tempPath, digest)
	if err != nil || !matches {
		_ = os.Remove(tempPath)
		if err != nil {
			return fmt.Errorf("validate temporary backup: %w", err)
		}
		return errors.New("temporary backup digest does not match current state")
	}
	if runtime.GOOS == "windows" {
		if err := os.Remove(s.backupPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			_ = os.Remove(tempPath)
			return err
		}
	}
	if err := os.Rename(tempPath, s.backupPath); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	return nil
}

func closeDriverConn(conn driver.Conn) error {
	if conn == nil {
		return nil
	}
	return conn.Close()
}
