package service_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/initiallyqq/data-api-framework/problem"
	"github.com/initiallyqq/data-api-framework/replica"
	"github.com/initiallyqq/data-api-framework/service"
)

const testToken = "admin-token-value"

func init() { problem.TypeBase = "https://example.com/problems/" }

// item is the row of the test copy: one table, items.
type item struct {
	Name string `json:"name"`
}

type itemLayout struct{}

func (itemLayout) Key(row item) string { return row.Name }

func (itemLayout) Replace(ctx context.Context, conn *sql.Conn, rows []item) error {
	if _, err := conn.ExecContext(
		ctx,
		`DROP TABLE IF EXISTS items; CREATE TABLE items (name TEXT PRIMARY KEY)`,
	); err != nil {
		return err
	}
	for _, row := range rows {
		if _, err := conn.ExecContext(ctx, `INSERT INTO items VALUES (?)`, row.Name); err != nil {
			return err
		}
	}
	return nil
}

func (itemLayout) Load(ctx context.Context, q replica.Queryer) ([]item, error) {
	var tables int
	if err := q.QueryRowContext(
		ctx,
		`SELECT count(*) FROM sqlite_master WHERE name = 'items'`,
	).Scan(
		&tables,
	); err != nil ||
		tables == 0 {
		return nil, err
	}
	result, err := q.QueryContext(ctx, `SELECT name FROM items`)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	var rows []item
	for result.Next() {
		var row item
		if err := result.Scan(&row.Name); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, result.Err()
}

func openItems(t *testing.T, dir string) *replica.Store[item] {
	t.Helper()
	files := replica.Files{
		Path:       filepath.Join(dir, "copy.db"),
		BackupPath: filepath.Join(dir, "backup", "last-good.db"),
	}
	store, err := replica.Open(files, 1, replica.Layout[item](itemLayout{}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return store
}

// testService answers GET /v1/api/items from the copy, and a problem or a
// plain failure when asked for one.
var testService = service.Spec[item]{
	Name:              "test-service",
	Noun:              "item",
	DefaultListenAddr: ":0",
	Register: func(server *service.Server, store *replica.Store[item]) {
		server.Query("/v1/api/items", func(r *http.Request) (any, error) {
			switch r.URL.Query().Get("fail") {
			case "problem":
				return nil, problem.New(problem.InvalidParameter, "fail is not a parameter").ForParameter("fail")
			case "disk":
				return nil, errors.New("disk on fire")
			}
			rows := []item{}
			err := store.Read(r.Context(), func(q replica.Queryer) error {
				loaded, err := itemLayout{}.Load(r.Context(), q)
				rows = append(rows, loaded...)
				return err
			})
			return rows, err
		})
	},
}

type fakeSyncer struct {
	calls int
	err   error
}

func (s *fakeSyncer) RequestSync() error {
	s.calls++
	return s.err
}

// state is what handler starts testService with: whether one
// synchronization ran first, and the monitor and syncer (nil: a fresh
// success and a syncer that accepts).
type state struct {
	synced  bool
	monitor *replica.Monitor
	syncer  *fakeSyncer
}

// handler serves testService over a copy in a fresh directory, in state s.
func handler(t *testing.T, s state) (http.Handler, *replica.Store[item]) {
	synced, monitor, syncer := s.synced, s.monitor, s.syncer
	t.Helper()
	dir := t.TempDir()
	store := openItems(t, dir)
	if synced {
		if _, err := store.Synchronize(context.Background(), []item{{Name: "a"}, {Name: "b"}}, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	token := filepath.Join(dir, "admin-token")
	if err := os.WriteFile(token, []byte(testToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if monitor == nil {
		monitor = &replica.Monitor{}
		monitor.Success(time.Now())
	}
	if syncer == nil {
		syncer = &fakeSyncer{}
	}
	config := service.Config{AdminTokenFile: token, MaxStaleness: 2 * time.Hour}
	h, err := testService.Handler(config, service.Replica[item]{Store: store, Monitor: monitor, Syncer: syncer})
	if err != nil {
		t.Fatal(err)
	}
	return h, store
}

// call is one request do sends: method and target, as the administrator
// when token is set.
type call struct{ method, target, token string }

// do sends c to h and returns the recorder and the decoded JSON body.
func do(t *testing.T, h http.Handler, c call) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	request := httptest.NewRequest(c.method, c.target, nil)
	request.Header.Set("X-Request-ID", "req-1")
	if c.token != "" {
		request.Header.Set("Authorization", "Bearer "+c.token)
	}
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, request)
	var body map[string]any
	_ = json.Unmarshal(recorder.Body.Bytes(), &body)
	return recorder, body
}

// isProblem checks the RFC 9457 shape every refusal has: the code's status,
// type and title, the detail repeated as error and message, the request ID.
func isProblem(t *testing.T, recorder *httptest.ResponseRecorder, body map[string]any, status int, code problem.Code) {
	t.Helper()
	if recorder.Code != status || recorder.Header().Get("Content-Type") != problem.MediaType ||
		recorder.Header().Get("Cache-Control") != "no-store" || body["code"] != string(code) ||
		body["status"] != float64(
			status,
		) || body["type"] != "https://example.com/problems/"+string(code) || body["title"] == "" ||
		body["detail"] == "" || body["error"] != body["detail"] || body["message"] != body["detail"] ||
		body["request_id"] != "req-1" {
		t.Fatalf("want a %d %s problem, got %d %s %s", status, code, recorder.Code, recorder.Header(), recorder.Body)
	}
}

func TestTheQueryRouteAnswersTheCopyOrAProblem(t *testing.T) {
	h, _ := handler(t, state{synced: true})
	recorder, _ := do(t, h, call{http.MethodGet, "/v1/api/items", ""})
	var rows []item
	if recorder.Code != http.StatusOK || recorder.Header().Get("Content-Type") != "application/json" ||
		json.Unmarshal(recorder.Body.Bytes(), &rows) != nil || len(rows) != 2 {
		t.Fatalf("query = %d %s", recorder.Code, recorder.Body)
	}
	recorder, body := do(t, h, call{http.MethodGet, "/v1/api/items?fail=problem", ""})
	isProblem(t, recorder, body, http.StatusBadRequest, problem.InvalidParameter)
	if body["parameter"] != "fail" {
		t.Fatalf("the problem lost its parameter: %v", body)
	}
	recorder, body = do(t, h, call{http.MethodGet, "/v1/api/items?fail=disk", ""})
	isProblem(t, recorder, body, http.StatusInternalServerError, problem.InternalError)
	if strings.Contains(recorder.Body.String(), "disk on fire") || body["detail"] != "item database operation failed" {
		t.Fatalf("a local failure leaked or lost its wording: %s", recorder.Body)
	}
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		recorder, body := do(t, h, call{method, "/v1/api/items", ""})
		isProblem(t, recorder, body, http.StatusMethodNotAllowed, problem.MethodNotAllowed)
		if recorder.Header().Get("Allow") != http.MethodGet {
			t.Fatalf("%s: Allow = %q", method, recorder.Header().Get("Allow"))
		}
	}
	for _, path := range []string{"/v1/api/items/", "/v1/items", "/"} {
		if recorder, _ := do(t, h, call{http.MethodGet, path, ""}); recorder.Code != http.StatusNotFound {
			t.Fatalf("%s = %d, want 404", path, recorder.Code)
		}
	}

	// Before the first synchronization there is nothing to serve.
	h, _ = handler(t, state{})
	recorder, body = do(t, h, call{http.MethodGet, "/v1/api/items", ""})
	isProblem(t, recorder, body, http.StatusServiceUnavailable, problem.DataUnavailable)
	if body["detail"] != "item data is not available yet" {
		t.Fatalf("detail = %v", body["detail"])
	}
}

func TestHealthSaysWhetherTheCopyCanBeServed(t *testing.T) {
	type check struct {
		code     int
		overall  string
		database string
		source   string
		fresh    string
		secret   string // only the administrator may see it
	}
	for name, test := range map[string]struct {
		monitor func(*replica.Monitor)
		closed  bool
		want    check
	}{
		"fresh": {
			func(m *replica.Monitor) { m.Success(time.Now().Add(-time.Hour)) }, false,
			check{200, "healthy", "healthy", "healthy", "fresh", ""},
		},
		"never": {func(*replica.Monitor) {}, false, check{503, "unhealthy", "healthy", "unknown", "unknown", ""}},
		"stale": {
			func(m *replica.Monitor) { m.Success(time.Now().Add(-3 * time.Hour)) }, false,
			check{503, "unhealthy", "healthy", "healthy", "stale", ""},
		},
		"source failing": {func(m *replica.Monitor) {
			m.Success(time.Now().Add(-time.Hour))
			m.Failure(errors.New("DolphinDB down"))
		}, false, check{200, "degraded", "healthy", "failed", "fresh", "DolphinDB down"}},
		"syncing": {
			func(m *replica.Monitor) { m.Success(time.Now().Add(-time.Hour)); m.Begin(time.Now()) }, false,
			check{200, "degraded", "healthy", "syncing", "fresh", ""},
		},
		"backup failing": {
			func(m *replica.Monitor) { m.Success(time.Now()); m.SetStorageError(errors.New("backup denied")) }, false,
			check{200, "degraded", "degraded", "healthy", "fresh", "backup denied"},
		},
		"database failing": {
			func(m *replica.Monitor) { m.Success(time.Now()); m.Begin(time.Now()) }, true,
			check{503, "unhealthy", "unavailable", "syncing", "fresh", "closed"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			monitor := &replica.Monitor{}
			test.monitor(monitor)
			h, store := handler(t, state{synced: true, monitor: monitor})
			if test.closed {
				_ = store.Close()
			}
			for _, token := range []string{"", testToken} {
				path := "/healthz"
				if token != "" {
					path = "/admin/v1/healthz"
				}
				recorder, _ := do(t, h, call{http.MethodGet, path, token})
				var body struct {
					Status   string
					Database struct{ Status, LastError string }
					Source   struct{ Status, LastError string } `json:"dolphinDB"`
					Fresh    struct{ Status string }            `json:"dataFreshness"`
				}
				_ = json.Unmarshal(recorder.Body.Bytes(), &body)
				got := check{
					recorder.Code,
					body.Status,
					body.Database.Status,
					body.Source.Status,
					body.Fresh.Status,
					"",
				}
				want := test.want
				want.secret = ""
				if got != want {
					t.Fatalf("%s = %+v, want %+v: %s", path, got, want, recorder.Body)
				}
				shown := strings.Contains(recorder.Body.String(), test.want.secret)
				if test.want.secret != "" && shown != (token != "") {
					t.Fatalf(
						"%s shows %q: %v; only the administrator may see it: %s",
						path,
						test.want.secret,
						shown,
						recorder.Body,
					)
				}
			}
		})
	}
}

func TestAdministratorRoutesNeedTheToken(t *testing.T) {
	syncer := &fakeSyncer{}
	h, _ := handler(t, state{synced: true, syncer: syncer})
	for _, request := range []struct{ method, path, token string }{
		{http.MethodGet, "/admin/v1/healthz", ""},
		{http.MethodGet, "/admin/v1/healthz", "wrong"},
		{http.MethodPost, "/admin/v1/sync", ""},
		{http.MethodPost, "/admin/v1/sync", "wrong"},
		{http.MethodGet, "/admin/v1/sync", ""},
	} {
		recorder, body := do(t, h, call{request.method, request.path, request.token})
		isProblem(t, recorder, body, http.StatusUnauthorized, problem.Unauthenticated)
	}
	recorder, body := do(t, h, call{http.MethodGet, "/admin/v1/sync", testToken})
	isProblem(t, recorder, body, http.StatusMethodNotAllowed, problem.MethodNotAllowed)
	recorder, _ = do(t, h, call{http.MethodPost, "/admin/v1/sync", testToken})
	if recorder.Code != http.StatusAccepted || strings.TrimSpace(recorder.Body.String()) != `{"status":"accepted"}` ||
		syncer.calls != 1 {
		t.Fatalf("sync = %d %s, calls %d", recorder.Code, recorder.Body, syncer.calls)
	}
	syncer.err = replica.ErrSyncInProgress
	recorder, body = do(t, h, call{http.MethodPost, "/admin/v1/sync", testToken})
	isProblem(t, recorder, body, http.StatusConflict, problem.SyncInProgress)
	syncer.err = errors.New("stopped")
	recorder, body = do(t, h, call{http.MethodPost, "/admin/v1/sync", testToken})
	isProblem(t, recorder, body, http.StatusServiceUnavailable, problem.ServiceUnavailable)

	empty := filepath.Join(t.TempDir(), "admin-token")
	if err := os.WriteFile(empty, []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	empties := service.Replica[item]{Store: openItems(t, t.TempDir()), Monitor: &replica.Monitor{}, Syncer: syncer}
	if _, err := testService.Handler(service.Config{AdminTokenFile: empty}, empties); err == nil {
		t.Fatal("an empty token file was accepted")
	}
}
