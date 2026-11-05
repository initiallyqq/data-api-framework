// Package upstream reads a data service's source tables from DolphinDB.
package upstream

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/dolphindb/api-go/v3/dialer"
	"github.com/dolphindb/api-go/v3/dolphindb"
	"github.com/dolphindb/api-go/v3/model"

	"github.com/initiallyqq/data-api-framework/timezone"
)

// Source is the DolphinDB database a service copies its tables from. The
// account (DDB_USER, DDB_PASSWORD) is the service's own shell env file.
type Source struct {
	Address         string
	CredentialsFile string
	Database        string
	Timeout         time.Duration
}

// Session is one login to the source, held for one synchronization: every
// table the synchronization reads is read through it.
type Session struct {
	client   *dolphindb.Client
	database string
}

// Connect logs in to the source. The session ends at ctx's deadline.
func Connect(ctx context.Context, source Source) (*Session, error) {
	if strings.TrimSpace(source.Address) == "" {
		return nil, fmt.Errorf("DolphinDB address is required")
	}
	path, ok := strings.CutPrefix(source.Database, "dfs://")
	if !ok || !safeName(strings.NewReplacer("/", "", ":", "").Replace(path)) {
		return nil, fmt.Errorf("DolphinDB database path %q is not dfs://<name>", source.Database)
	}
	user, password, err := loadCredentials(source.CredentialsFile)
	if err != nil {
		return nil, err
	}
	timeout := source.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	client, err := dolphindb.Dial(source.Address, user, password, &dialer.BehaviorOptions{
		Timeout:    timeout,
		NetTimeout: 30 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("connect DolphinDB: %w", err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = client.SetDeadline(deadline)
	}
	return &Session{client: client, database: source.Database}, nil
}

// Close closes the connection.
func (s *Session) Close() error { return s.client.Close() }

// Tables lists the tables of the database, in name order.
func (s *Session) Tables(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	listed, err := s.client.RunScript(fmt.Sprintf("getTables(database(%q))", s.database))
	if err != nil {
		return nil, fmt.Errorf("list tables of %s: %w", s.database, err)
	}
	vector, ok := listed.(*model.Vector)
	if !ok {
		return nil, fmt.Errorf("list tables of %s returned %T, want a vector", s.database, listed)
	}
	names := make([]string, 0, vector.Rows())
	for index := 0; index < vector.Rows(); index++ {
		names = append(names, vector.Get(index).String())
	}
	slices.Sort(names)
	return names, nil
}

// Query is `select Columns from Table [where Where]` over the session's
// database. Columns names each column as the row type's json tags do.
type Query struct {
	Table   string
	Columns string
	Where   string
}

// Select runs q and decodes every row into T with encoding/json. A failure
// fails the whole read, never a part of it.
func Select[T any](ctx context.Context, s *Session, q Query) ([]T, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	table := q.Table
	if !safeName(table) {
		return nil, fmt.Errorf("table name %q cannot be written into a DolphinDB script", table)
	}
	script := fmt.Sprintf("select %s from loadTable(%q, `%s)", q.Columns, s.database, table)
	if q.Where != "" {
		script += " where " + q.Where
	}
	result, err := s.client.RunScript(script)
	if err != nil {
		return nil, fmt.Errorf("query %s: %w", table, err)
	}
	queried, ok := result.(*model.Table)
	if !ok {
		return nil, fmt.Errorf("query %s returned %T, want a table", table, result)
	}
	rows := make([]T, 0, queried.Rows())
	for index := 0; index < queried.Rows(); index++ {
		var row T
		if err := json.Unmarshal([]byte(queried.GetRowJSON(index)), &row); err != nil {
			return nil, fmt.Errorf("decode %s row %d: %w", table, index, err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// safeName accepts what a DolphinDB table name may hold here, so the name
// can be written into a script as is.
func safeName(value string) bool {
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' ||
			r == '.' {
			continue
		}
		return false
	}
	return value != ""
}

// loadCredentials reads DDB_USER and DDB_PASSWORD from a shell env file
// (dolphindb.env); other keys and `export` prefixes are ignored.
func loadCredentials(path string) (string, string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", "", fmt.Errorf("read DolphinDB credentials: %w", err)
	}
	defer file.Close()
	values := make(map[string]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		key, raw, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key != "DDB_USER" && key != "DDB_PASSWORD" {
			continue
		}
		value, err := parseShellValue(strings.TrimSpace(raw))
		if err != nil {
			return "", "", fmt.Errorf("parse %s: %w", key, err)
		}
		values[key] = value
	}
	if err := scanner.Err(); err != nil {
		return "", "", err
	}
	if values["DDB_USER"] == "" || values["DDB_PASSWORD"] == "" {
		return "", "", fmt.Errorf("credentials file must define DDB_USER and DDB_PASSWORD")
	}
	return values["DDB_USER"], values["DDB_PASSWORD"], nil
}

func parseShellValue(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	if value[0] == '\'' {
		if len(value) < 2 || value[len(value)-1] != '\'' {
			return "", fmt.Errorf("unterminated single-quoted value")
		}
		return value[1 : len(value)-1], nil
	}
	if value[0] == '"' {
		return strconv.Unquote(value)
	}
	if strings.ContainsAny(value, " \t;&|`$()") {
		return "", fmt.Errorf("unquoted value contains shell syntax")
	}
	return value, nil
}

// ParseDate reads a DATE as DolphinDB writes it (2026.09.30), or written
// YYYY-MM-DD, and writes it YYYY-MM-DD.
func ParseDate(field, value string) (string, error) {
	value = strings.TrimSpace(value)
	for _, layout := range []string{"2006.01.02", "2006-01-02"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.Format("2006-01-02"), nil
		}
	}
	return "", fmt.Errorf("%s must be a DolphinDB DATE, got %q", field, value)
}

// ParseDateTime reads a DATETIME or TIMESTAMP as DolphinDB writes it
// (2026.08.27T22:08:29, a TIMESTAMP with its milliseconds) as the Beijing
// time it is written in.
func ParseDateTime(field, value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	for _, layout := range []string{"2006.01.02T15:04:05.999999999", "2006-01-02T15:04:05.999999999"} {
		if parsed, err := time.ParseInLocation(layout, value, timezone.Beijing); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("%s must be a DolphinDB DATETIME, got %q", field, value)
}
