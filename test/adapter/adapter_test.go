package adapter_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/initiallyqq/data-api-framework/adapter"
	"github.com/initiallyqq/data-api-framework/endpoint"
	"github.com/initiallyqq/data-api-framework/problem"
)

// testAdapter relays GET /v1/api/items?name= as the tool get_items.
func testAdapter(tool string) adapter.Spec {
	return adapter.Spec{
		Name:              "test-mcp",
		Noun:              "item",
		DefaultListenAddr: ":0",
		DefaultServiceURL: "http://127.0.0.1:1",
		Tool:              "get_items",
		Narrow:            "ask for fewer items",
		Register: func(server *mcp.Server, client *adapter.Client) error {
			arguments, err := adapter.InputSchema(
				[]endpoint.Parameter{{Name: "name", Required: true}},
				map[string]*jsonschema.Schema{
					"name": adapter.OneOrManySchema("One name or several.", jsonschema.Ptr(2)),
				},
			)
			if err != nil {
				return err
			}
			mcp.AddTool(server, &mcp.Tool{Name: tool, InputSchema: arguments},
				func(ctx context.Context, _ *mcp.CallToolRequest, args struct {
					Name adapter.OneOrMany `json:"name"`
				}) (*mcp.CallToolResult, any, error) {
					return client.Call(
						ctx,
						"/v1/api/items",
						url.Values{"name": {strings.Join(args.Name, ",")}},
					), nil, nil
				})
			return nil
		},
		Smoke: func(today string) adapter.SmokeTest {
			return adapter.SmokeTest{
				Good:    map[string]any{"name": "a"},
				Bad:     map[string]any{"name": "missing"},
				BadCode: problem.ExchangeNotFound,
			}
		},
	}
}

// fakeItems is the service behind the adapter; each name answers one way.
type fakeItems struct {
	mu       sync.Mutex
	requests []string
	health   int
	empty    bool // "a" answers []
	release  chan struct{}
	blocked  chan struct{}
}

func (f *fakeItems) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests = append(f.requests, r.Method+" "+r.URL.RequestURI())
	f.mu.Unlock()
	if r.URL.Path == "/healthz" {
		switch f.health {
		case 0:
			endpoint.WriteJSON(w, http.StatusOK, map[string]string{"status": "healthy"})
		case -1:
			_, _ = w.Write([]byte("not json"))
		default:
			endpoint.WriteJSON(
				w,
				f.health,
				map[string]string{"status": map[bool]string{true: "degraded", false: "unhealthy"}[f.health == 200]},
			)
		}
		return
	}
	switch name := r.URL.Query().Get("name"); name {
	case "a", "a,b":
		if f.empty {
			_, _ = w.Write([]byte("[]\n"))
			return
		}
		_, _ = w.Write([]byte(`[{"name":"a"}]` + "\n"))
	case "missing":
		problem.Write(w, r, problem.NotFound(problem.ExchangeNotFound, "name", []string{"missing"}, []string{"a"}))
	case "html":
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("<html>bad gateway \xff</html>"))
	case "object":
		_, _ = w.Write([]byte(`{"not":"an array"}`))
	case "slow":
		time.Sleep(2 * time.Second)
	case "huge":
		_, _ = w.Write(bytes.Repeat([]byte(" "), adapter.MaxResponseBytes+1))
	case "large":
		fmt.Fprintf(w, `["%s"]`, strings.Repeat("x", adapter.MaxResponseBytes/2+1))
	case "block":
		f.blocked <- struct{}{}
		<-f.release
		_, _ = w.Write([]byte("[]"))
	default: // an empty 200
	}
}

func startAdapter(t *testing.T, spec adapter.Spec, fake *fakeItems, config adapter.Config) *httptest.Server {
	t.Helper()
	service := httptest.NewServer(fake)
	t.Cleanup(service.Close)
	if config.ServiceURL == "" {
		config.ServiceURL = service.URL
	}
	h, err := spec.Handler(config)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	return server
}

func session(t *testing.T, server *httptest.Server) *mcp.ClientSession {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "test"}, nil).
		Connect(ctx, &mcp.StreamableClientTransport{Endpoint: server.URL + "/mcp"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// Failure modes of a relayed call: the service answers rows (passed through
// unchanged), a problem (passed through as the tool error), something that
// is not a problem, something that is not an array, nothing, too late, too
// much, or rows that make a tool result too large.
func TestToolCallsRelayTheServiceAnswer(t *testing.T) {
	fake := &fakeItems{}
	s := session(
		t,
		startAdapter(t, testAdapter("get_items"), fake, adapter.Config{RequestTimeout: 500 * time.Millisecond}),
	)
	call := func(name any) (string, *problem.Details) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		result, err := s.CallTool(ctx, &mcp.CallToolParams{Name: "get_items", Arguments: map[string]any{"name": name}})
		if err != nil {
			t.Fatal(err)
		}
		text := result.Content[0].(*mcp.TextContent).Text
		var fromText any
		if json.Unmarshal([]byte(text), &fromText) != nil || !reflect.DeepEqual(fromText, result.StructuredContent) {
			t.Fatalf("structured content %v differs from the text %s", result.StructuredContent, text)
		}
		if !result.IsError {
			return text, nil
		}
		answer, ok := problem.Parse([]byte(text))
		if !ok {
			t.Fatalf("tool error %s is not a problem", text)
		}
		return text, answer
	}
	if text, refusal := call([]any{"a", "b"}); refusal != nil || text != `[{"name":"a"}]`+"\n" {
		t.Fatalf("rows = %q %v, want the body unchanged", text, refusal)
	}
	if got := fake.requests[len(fake.requests)-1]; got != "GET /v1/api/items?name=a%2Cb" {
		t.Fatalf("forwarded %q", got)
	}
	for name, want := range map[string]struct {
		code   problem.Code
		detail string
	}{
		"missing": {problem.ExchangeNotFound, "name not found: missing; available: a"},
		"html":    {problem.UpstreamError, "item service returned HTTP 502: <html>bad gateway </html>"},
		"object":  {problem.UpstreamError, `decode item service response: want an array of rows: {"not":"an array"}`},
		"slow":    {problem.UpstreamTimeout, "the item service did not answer in time"},
		"huge":    {problem.ResponseTooLarge, fmt.Sprintf("item service response exceeds %d bytes; ask for fewer items", adapter.MaxResponseBytes)},
		"large":   {problem.ResponseTooLarge, fmt.Sprintf("MCP tool response exceeds %d bytes; ask for fewer items", adapter.MaxResponseBytes)},
	} {
		if _, refusal := call(name); refusal == nil || refusal.Code != want.code || refusal.Detail != want.detail {
			t.Fatalf("%s = %+v, want %s %q", name, refusal, want.code, want.detail)
		}
	}
	if _, refusal := call(
		"missing",
	); strings.Join(refusal.Values, ",") != "missing" ||
		strings.Join(refusal.Available, ",") != "a" {
		t.Fatalf("the relayed problem lost its lists: %+v", refusal)
	}
	// Arguments the published schema refuses never reach the service.
	before := len(fake.requests)
	for _, name := range []any{"", 7, []any{}, []any{"a", "b", "c"}} {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		result, err := s.CallTool(ctx, &mcp.CallToolParams{Name: "get_items", Arguments: map[string]any{"name": name}})
		cancel()
		if err == nil && !result.IsError {
			t.Fatalf("name %v was accepted", name)
		}
	}
	if len(fake.requests) != before {
		t.Fatalf("refused arguments reached the service: %v", fake.requests[before:])
	}

	down := httptest.NewServer(http.NotFoundHandler())
	down.Close()
	s = session(t, startAdapter(t, testAdapter("get_items"), fake, adapter.Config{ServiceURL: down.URL}))
	if _, refusal := call("a"); refusal == nil || refusal.Code != problem.UpstreamError {
		t.Fatalf("service down = %+v", refusal)
	}
}

func send(server *httptest.Server, body []byte, origin string) (*http.Response, error) {
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/mcp", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	if origin != "" {
		request.Header.Set("Origin", origin)
	}
	return http.DefaultClient.Do(request)
}

func post(t *testing.T, server *httptest.Server, body []byte, origin string) *http.Response {
	t.Helper()
	response, err := send(server, body, origin)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { response.Body.Close() })
	return response
}

func TestTheMCPPathTakesOnlyBoundedPOSTs(t *testing.T) {
	fake := &fakeItems{release: make(chan struct{}), blocked: make(chan struct{}, adapter.MaxConcurrency)}
	server := startAdapter(
		t,
		testAdapter("get_items"),
		fake,
		adapter.Config{AllowedOrigins: []string{" https://ok.example ", ""}},
	)
	isRefusal := func(response *http.Response, status int, code problem.Code) {
		t.Helper()
		var body map[string]any
		_ = json.NewDecoder(response.Body).Decode(&body)
		response.Body.Close()
		if response.StatusCode != status || response.Header.Get("Content-Type") != problem.MediaType ||
			body["code"] != string(code) ||
			body["error"] != body["detail"] ||
			body["message"] != body["detail"] {
			t.Fatalf("want %d %s, got %d %v", status, code, response.StatusCode, body)
		}
	}
	for _, method := range []string{http.MethodGet, http.MethodDelete, http.MethodPut} {
		request, _ := http.NewRequest(method, server.URL+"/mcp", nil)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		if response.Header.Get("Allow") != http.MethodPost {
			t.Fatalf("%s: Allow = %q", method, response.Header.Get("Allow"))
		}
		isRefusal(response, http.StatusMethodNotAllowed, problem.MethodNotAllowed)
	}
	initialize := []byte(
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}`,
	)
	for _, origin := range []string{"", "https://ok.example"} {
		if response := post(t, server, initialize, origin); response.StatusCode != http.StatusOK {
			t.Fatalf("initialize from %q = %d", origin, response.StatusCode)
		}
	}
	isRefusal(post(t, server, initialize, "https://untrusted.example"), http.StatusForbidden, problem.OriginNotAllowed)
	isRefusal(
		post(t, server, bytes.Repeat([]byte("x"), adapter.MaxRequestBytes+1), ""),
		http.StatusRequestEntityTooLarge,
		problem.RequestTooLarge,
	)

	call := []byte(
		`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"get_items","arguments":{"name":"block"}}}`,
	)
	var wg sync.WaitGroup
	for range adapter.MaxConcurrency {
		wg.Go(func() {
			if response, err := send(server, call, ""); err == nil {
				response.Body.Close()
			}
		})
	}
	for range adapter.MaxConcurrency {
		select {
		case <-fake.blocked:
		case <-time.After(10 * time.Second):
			t.Fatal("the calls did not reach the service")
		}
	}
	over := post(t, server, call, "")
	if over.Header.Get("Retry-After") != "1" {
		t.Fatalf("Retry-After = %q", over.Header.Get("Retry-After"))
	}
	isRefusal(over, http.StatusTooManyRequests, problem.RateLimited)
	close(fake.release)
	wg.Wait()
}

func TestAdapterHealthFollowsTheService(t *testing.T) {
	for name, test := range map[string]struct {
		health int
		down   bool
		code   int
		body   string
	}{
		"healthy":     {0, false, 200, `{"item":{"status":"healthy"},"status":"healthy"}`},
		"degraded":    {200, false, 200, `{"item":{"status":"degraded"},"status":"degraded"}`},
		"unhealthy":   {503, false, 503, `{"item":{"status":"unhealthy"},"status":"unhealthy"}`},
		"not JSON":    {-1, false, 503, `{"item":{"status":"unavailable"},"status":"unhealthy"}`},
		"unreachable": {0, true, 503, `{"item":{"status":"unavailable"},"status":"unhealthy"}`},
	} {
		t.Run(name, func(t *testing.T) {
			config := adapter.Config{}
			if test.down {
				config.ServiceURL = "http://127.0.0.1:1"
			}
			server := startAdapter(t, testAdapter("get_items"), &fakeItems{health: test.health}, config)
			response, err := http.Get(server.URL + "/healthz")
			if err != nil {
				t.Fatal(err)
			}
			var body bytes.Buffer
			_, _ = body.ReadFrom(response.Body)
			response.Body.Close()
			if response.StatusCode != test.code || strings.TrimSpace(body.String()) != test.body ||
				response.Header.Get("Cache-Control") != "no-store" {
				t.Fatalf("health = %d %s", response.StatusCode, body.String())
			}
		})
	}
}

func TestAdapterConfigRefusesWhatItCannotUse(t *testing.T) {
	spec := testAdapter("get_items")
	for _, config := range []adapter.Config{
		{ServiceURL: "ftp://item:8096"},
		{ServiceURL: "http://item:8096?x=1"},
		{ServiceURL: "http:///path"},
		{AllowedOrigins: []string{"https://ok.example/path"}},
		{AllowedOrigins: []string{"ok.example"}},
	} {
		if _, err := spec.Handler(config); err == nil {
			t.Fatalf("%+v was accepted", config)
		}
	}
	// A service URL with a trailing slash reaches the same routes.
	fake := &fakeItems{}
	service := httptest.NewServer(fake)
	t.Cleanup(service.Close)
	s := session(t, startAdapter(t, spec, fake, adapter.Config{ServiceURL: service.URL + "/"}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := s.CallTool(
		ctx,
		&mcp.CallToolParams{Name: "get_items", Arguments: map[string]any{"name": "a"}},
	); err != nil ||
		len(fake.requests) != 1 ||
		fake.requests[0] != "GET /v1/api/items?name=a" {
		t.Fatalf("forwarded %v, %v", fake.requests, err)
	}
	if _, err := adapter.InputSchema(
		[]endpoint.Parameter{{Name: "a"}, {Name: "b"}},
		map[string]*jsonschema.Schema{"a": {}},
	); err == nil {
		t.Fatal("a parameter without a shape was accepted")
	}
	if _, err := adapter.InputSchema(
		[]endpoint.Parameter{{Name: "a"}},
		map[string]*jsonschema.Schema{"b": {}},
	); err == nil {
		t.Fatal("a shape for another parameter was accepted")
	}
}

// The smoke run must fail against an adapter that lists another tool, or
// whose tool answers no rows or the wrong refusal.
func TestSmokePassesOnlyAgainstAWorkingAdapter(t *testing.T) {
	smoke := func(spec adapter.Spec, fake *fakeItems) error {
		server := startAdapter(t, spec, fake, adapter.Config{})
		return spec.Run([]string{"smoke", "-url", server.URL + "/mcp", "-timeout", "10s"})
	}
	if err := smoke(testAdapter("get_items"), &fakeItems{}); err != nil {
		t.Fatalf("smoke against a working adapter: %v", err)
	}
	if err := smoke(testAdapter("get_other_items"), &fakeItems{}); err == nil {
		t.Fatal("smoke passed with another tool listed")
	}
	if err := smoke(testAdapter("get_items"), &fakeItems{empty: true}); err == nil {
		t.Fatal("smoke passed with no rows")
	}
	wrongRefusal := testAdapter("get_items")
	wrongRefusal.Smoke = func(string) adapter.SmokeTest {
		return adapter.SmokeTest{
			Good:    map[string]any{"name": "a"},
			Bad:     map[string]any{"name": "html"},
			BadCode: problem.ExchangeNotFound,
		}
	}
	if err := smoke(wrongRefusal, &fakeItems{}); err == nil {
		t.Fatal("smoke passed with the wrong refusal")
	}
}
