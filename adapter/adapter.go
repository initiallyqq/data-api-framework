// Package adapter is a data service's MCP adapter binary: one read-only
// tool that relays the service's public query.
package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/initiallyqq/data-api-framework/buildinfo"
	"github.com/initiallyqq/data-api-framework/endpoint"
	"github.com/initiallyqq/data-api-framework/problem"
	"github.com/initiallyqq/data-api-framework/timezone"
)

// Limits of an adapter.
const (
	// DefaultTimeout is the longest a call to the service may take; a
	// configured timeout above it is lowered to it.
	DefaultTimeout = 30 * time.Second
	// MaxRequestBytes is the largest MCP request body read.
	MaxRequestBytes = 1 << 20
	// MaxResponseBytes is the largest service answer relayed. The largest
	// answer of the services today is about 2 MB; the limit leaves room for
	// growth and for the result being carried twice (text and structured).
	MaxResponseBytes = 16 << 20
	// MaxConcurrency is how many MCP requests are served at once.
	MaxConcurrency = 8
)

// Spec is a service's MCP adapter: one read-only tool that relays the
// service's public query, holding no data, SQLite file or credential of its
// own.
type Spec struct {
	Name              string // the binary's name and the MCP implementation name
	Noun              string // the service relayed, as in --<noun>-url and in messages
	DefaultListenAddr string
	DefaultServiceURL string
	Tool              string // the one tool's name
	Narrow            string // how to ask for less, when an answer is too large
	// Register adds Tool to server, answering through client.
	Register func(server *mcp.Server, client *Client) error
	// Smoke is what `smoke` calls the deployed tool with, today being the
	// Beijing date.
	Smoke func(today string) SmokeTest
}

// SmokeTest is one smoke run's two calls: Good must answer a non-empty
// array; Bad must be refused with the problem code BadCode.
type SmokeTest struct {
	Good    map[string]any
	Bad     map[string]any
	BadCode problem.Code
}

// Config is how an adapter runs; zero values take the defaults.
type Config struct {
	ListenAddr     string
	ServiceURL     string
	RequestTimeout time.Duration
	AllowedOrigins []string
}

func (a Spec) validate(c *Config) error {
	if strings.TrimSpace(c.ListenAddr) == "" {
		c.ListenAddr = a.DefaultListenAddr
	}
	if strings.TrimSpace(c.ServiceURL) == "" {
		c.ServiceURL = a.DefaultServiceURL
	}
	parsed, err := url.Parse(strings.TrimSpace(c.ServiceURL))
	if err != nil {
		return fmt.Errorf("parse %s URL: %w", a.Noun, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("%s URL must use http or https", a.Noun)
	}
	if parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("%s URL must contain a host and no query or fragment", a.Noun)
	}
	c.ServiceURL = strings.TrimRight(parsed.String(), "/")
	if c.RequestTimeout <= 0 || c.RequestTimeout > DefaultTimeout {
		c.RequestTimeout = DefaultTimeout
	}
	var origins []string
	for _, value := range c.AllowedOrigins {
		origin := strings.TrimSpace(value)
		if origin == "" {
			continue
		}
		originURL, err := url.Parse(origin)
		if err != nil || originURL.Scheme == "" || originURL.Host == "" ||
			originURL.Path != "" || originURL.RawQuery != "" || originURL.Fragment != "" {
			return fmt.Errorf("allowed origin %q must be scheme://host[:port]", origin)
		}
		origins = append(origins, origin)
	}
	c.AllowedOrigins = origins
	return nil
}

// Handler is the adapter's HTTP surface: /healthz, and /mcp taking POST
// only, from an allowed Origin (or none), bodies up to MaxRequestBytes and
// MaxConcurrency requests at a time.
func (a Spec) Handler(config Config) (http.Handler, error) {
	if err := a.validate(&config); err != nil {
		return nil, err
	}
	client := &Client{
		noun:    a.Noun,
		narrow:  a.Narrow,
		baseURL: config.ServiceURL,
		http:    &http.Client{Timeout: config.RequestTimeout},
		timeout: config.RequestTimeout,
	}
	server := mcp.NewServer(&mcp.Implementation{Name: a.Name, Version: buildinfo.Version}, &mcp.ServerOptions{
		// Only what is true: one fixed tool, so no listChanged; no log
		// notifications (Logging is deprecated in 2026-07-28 and the SDK
		// advertises it by default).
		Capabilities: &mcp.ServerCapabilities{Tools: &mcp.ToolCapabilities{}},
	})
	if err := a.Register(server, client); err != nil {
		return nil, err
	}
	transport := mcp.NewStreamableHTTPHandler(
		func(*http.Request) *mcp.Server { return server },
		&mcp.StreamableHTTPOptions{
			Stateless:                    true,
			JSONResponse:                 true,
			MaxRequestBodyBytes:          MaxRequestBytes,
			PropagateRequestCancellation: true,
		},
	)
	allowed := make(map[string]bool, len(config.AllowedOrigins))
	for _, origin := range config.AllowedOrigins {
		allowed[origin] = true
	}
	slots := make(chan struct{}, MaxConcurrency)
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { a.health(w, r, client) })
	mux.HandleFunc("/mcp", func(w http.ResponseWriter, r *http.Request) {
		// The transport is stateless, so the SDK would refuse GET (no server
		// stream) and DELETE (no session) with a plain-text 405.
		if r.Method != http.MethodPost {
			problem.WriteMethodNotAllowed(w, r, http.MethodPost)
			return
		}
		if origin := strings.TrimSpace(r.Header.Get("Origin")); origin != "" && !allowed[origin] {
			problem.Write(w, r, problem.New(problem.OriginNotAllowed, fmt.Sprintf("Origin %q is not allowed", origin)))
			return
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			w.Header().Set("Retry-After", "1")
			problem.Write(
				w,
				r,
				problem.New(problem.RateLimited, fmt.Sprintf("more than %d MCP requests in progress", MaxConcurrency)),
			)
			return
		}
		payload, err := io.ReadAll(io.LimitReader(r.Body, MaxRequestBytes+1))
		_ = r.Body.Close()
		if err != nil {
			problem.Write(w, r, problem.New(problem.InvalidParameter, "unable to read request body"))
			return
		}
		if len(payload) > MaxRequestBytes {
			problem.Write(w, r, problem.New(problem.RequestTooLarge, "MCP request body exceeds 1 MiB"))
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(payload))
		transport.ServeHTTP(w, r)
	})
	return mux, nil
}

// health reports the adapter healthy exactly when the service is: the
// adapter has no state of its own that could fail separately.
func (a Spec) health(w http.ResponseWriter, r *http.Request, client *Client) {
	if r.Method != http.MethodGet {
		problem.WriteMethodNotAllowed(w, r, http.MethodGet)
		return
	}
	response := map[string]any{"status": "healthy"}
	status := http.StatusOK
	code, body, err := client.get(r.Context(), "/healthz")
	var serviceHealth struct {
		Status string `json:"status"`
	}
	if err == nil && json.Unmarshal(body, &serviceHealth) != nil {
		err = fmt.Errorf("decode %s service health response", a.Noun)
	}
	switch {
	case err != nil:
		response["status"] = "unhealthy"
		response[a.Noun] = map[string]string{"status": "unavailable"}
		status = http.StatusServiceUnavailable
	case code < http.StatusOK || code >= http.StatusMultipleChoices:
		response["status"] = "unhealthy"
		response[a.Noun] = json.RawMessage(body)
		status = http.StatusServiceUnavailable
	default:
		response[a.Noun] = json.RawMessage(body)
		if serviceHealth.Status != "" {
			response["status"] = serviceHealth.Status
		}
	}
	w.Header().Set("Cache-Control", "no-store")
	endpoint.WriteJSON(w, status, response)
}

// Client reads the service's public routes with GET only: it holds no
// administrator token and has no way to change anything.
type Client struct {
	noun, narrow string
	baseURL      string
	http         *http.Client
	timeout      time.Duration
}

func (c *Client) get(ctx context.Context, path string) (int, []byte, error) {
	requestCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return 0, nil, err
	}
	response, err := c.http.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, MaxResponseBytes+1))
	if err != nil {
		return 0, nil, err
	}
	if len(payload) > MaxResponseBytes {
		return 0, nil, problem.New(
			problem.ResponseTooLarge,
			fmt.Sprintf("%s service response exceeds %d bytes; %s", c.noun, MaxResponseBytes, c.narrow),
		)
	}
	return response.StatusCode, payload, nil
}

// Call is one GET of the service's path with query: the rows the service
// answers become the tool's result, unchanged; a problem it answers becomes
// a tool error carrying the same problem, so an agent branches on the code
// an HTTP caller sees.
func (c *Client) Call(ctx context.Context, path string, query url.Values) *mcp.CallToolResult {
	if len(query) > 0 {
		path += "?" + query.Encode()
	}
	status, body, err := c.get(ctx, path)
	var known *problem.Details
	switch {
	case errors.As(err, &known):
		return ToolError(known)
	case errors.Is(err, context.DeadlineExceeded):
		return ToolError(problem.New(problem.UpstreamTimeout, "the "+c.noun+" service did not answer in time"))
	case err != nil:
		return ToolError(problem.New(problem.UpstreamError, err.Error()))
	case status < http.StatusOK || status >= http.StatusMultipleChoices:
		if answer, ok := problem.Parse(body); ok {
			return ToolError(answer)
		}
		return ToolError(
			problem.New(
				problem.UpstreamError,
				fmt.Sprintf("%s service returned HTTP %d: %s", c.noun, status, summarize(body)),
			),
		)
	default: // 2xx: the body is checked below
	}
	var rows []json.RawMessage
	if json.Unmarshal(body, &rows) != nil || rows == nil {
		return ToolError(
			problem.New(
				problem.UpstreamError,
				"decode "+c.noun+" service response: want an array of rows: "+summarize(body),
			),
		)
	}
	result := &mcp.CallToolResult{
		Content:           []mcp.Content{&mcp.TextContent{Text: string(body)}},
		StructuredContent: json.RawMessage(body),
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return ToolError(problem.New(problem.InternalError, err.Error()))
	}
	const jsonRPCEnvelopeAllowance = 1024
	if len(encoded)+jsonRPCEnvelopeAllowance > MaxResponseBytes {
		return ToolError(
			problem.New(
				problem.ResponseTooLarge,
				fmt.Sprintf("MCP tool response exceeds %d bytes; %s", MaxResponseBytes, c.narrow),
			),
		)
	}
	return result
}

func summarize(body []byte) string {
	const maxErrorBytes = 512
	text := strings.TrimSpace(string(bytes.ToValidUTF8(body, nil)))
	if len(text) > maxErrorBytes {
		return text[:maxErrorBytes] + "..."
	}
	return text
}

// ToolError is a tool execution error (MCP tools, Error Handling) whose
// structured content is the problem object HTTP answers with.
func ToolError(p *problem.Details) *mcp.CallToolResult {
	body, _ := json.Marshal(p)
	return &mcp.CallToolResult{
		Content:           []mcp.Content{&mcp.TextContent{Text: string(body)}},
		StructuredContent: json.RawMessage(body),
		IsError:           true,
	}
}

// InputSchema declares a tool's arguments from the query's parameter list:
// one argument per parameter, under its name, required where the list says
// so, each shaped by shapes. Tools are registered with the SDK's typed
// mcp.AddTool, which validates every call against this same schema before
// the handler runs, so what tools/list publishes is exactly what a call is
// checked against. A parameter without a shape, or a shape without a
// parameter, is a programming error and stops the adapter from starting.
func InputSchema(parameters []endpoint.Parameter, shapes map[string]*jsonschema.Schema) (*jsonschema.Schema, error) {
	if len(shapes) != len(parameters) {
		return nil, fmt.Errorf(
			"%d argument shapes for %d parameters %v",
			len(shapes),
			len(parameters),
			endpoint.ParameterNames(parameters),
		)
	}
	schema := &jsonschema.Schema{
		Type:                 "object",
		Properties:           make(map[string]*jsonschema.Schema, len(parameters)),
		AdditionalProperties: &jsonschema.Schema{Not: &jsonschema.Schema{}},
	}
	for _, parameter := range parameters {
		shape, ok := shapes[parameter.Name]
		if !ok {
			return nil, fmt.Errorf("no argument shape for parameter %q", parameter.Name)
		}
		schema.Properties[parameter.Name] = shape
		if parameter.Required {
			schema.Required = append(schema.Required, parameter.Name)
		}
	}
	return schema, nil
}

// OneOrMany decodes an argument OneOrManySchema declares.
type OneOrMany []string

// UnmarshalJSON reads one string or an array of strings.
func (l *OneOrMany) UnmarshalJSON(data []byte) error {
	var single string
	if err := json.Unmarshal(data, &single); err == nil {
		*l = OneOrMany{single}
		return nil
	}
	var many []string
	if err := json.Unmarshal(data, &many); err != nil {
		return fmt.Errorf("must be a string or an array of strings")
	}
	*l = many
	return nil
}

// OneOrManySchema declares an argument that takes one non-empty string or a
// non-empty array of them, at most maxItems when given. No value list: what
// exists is the data's answer, and a value the data does not hold comes back
// as a not-found problem naming the ones it does.
func OneOrManySchema(description string, maxItems *int) *jsonschema.Schema {
	item := func() *jsonschema.Schema { return &jsonschema.Schema{Type: "string", MinLength: jsonschema.Ptr(1)} }
	return &jsonschema.Schema{
		Description: description,
		AnyOf: []*jsonschema.Schema{
			item(),
			{Type: "array", Items: item(), MinItems: jsonschema.Ptr(1), MaxItems: maxItems},
		},
	}
}

// Main is the binary: flags serve, `healthcheck [-url]` probes a running
// adapter, `smoke [-url] [-timeout]` drives a deployed one as an MCP client.
func (a Spec) Main(args []string) {
	if err := a.Run(args); err != nil {
		fmt.Fprintln(os.Stderr, a.Name+":", err)
		os.Exit(1)
	}
}

// Run is Main returning its error instead of exiting.
func (a Spec) Run(args []string) error {
	if len(args) > 0 && args[0] == "healthcheck" {
		return endpoint.Healthcheck(a.Name, "http://127.0.0.1"+a.DefaultListenAddr+"/healthz", args[1:])
	}
	if len(args) > 0 && args[0] == "smoke" {
		return a.smoke(args[1:])
	}
	flags := flag.NewFlagSet(a.Name, flag.ContinueOnError)
	listenAddr := flags.String("listen-addr", a.DefaultListenAddr, "listen address")
	serviceURL := flags.String(a.Noun+"-url", a.DefaultServiceURL, a.Noun+" service URL")
	timeout := flags.Duration("timeout", DefaultTimeout, "downstream request timeout")
	origins := flags.String("allowed-origins", "", "comma-separated Origin allowlist")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("%s accepts flags only", a.Name)
	}
	config := Config{
		ListenAddr:     *listenAddr,
		ServiceURL:     *serviceURL,
		RequestTimeout: *timeout,
		AllowedOrigins: strings.Split(*origins, ","),
	}
	if err := a.validate(&config); err != nil {
		return err
	}
	handler, err := a.Handler(config)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	server := endpoint.Server(config.ListenAddr, handler,
		config.RequestTimeout+5*time.Second, config.RequestTimeout+10*time.Second)
	return endpoint.Run(ctx, a.Name, server)
}

// smoke drives the deployed endpoint the way an MCP client does: it
// initializes a session, checks that tools/list is exactly the one tool, and
// calls it once for data and once for the service's refusal.
func (a Spec) smoke(args []string) error {
	flags := flag.NewFlagSet(a.Name+" smoke", flag.ContinueOnError)
	endpoint := flags.String("url", "http://127.0.0.1"+a.DefaultListenAddr+"/mcp", "MCP endpoint")
	timeout := flags.Duration("timeout", 60*time.Second, "smoke test timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("%s smoke accepts flags only", a.Name)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	session, err := mcp.NewClient(&mcp.Implementation{Name: a.Name + "-smoke", Version: buildinfo.Version}, nil).
		Connect(ctx, &mcp.StreamableClientTransport{Endpoint: *endpoint}, nil)
	if err != nil {
		return fmt.Errorf("initialize MCP session: %w", err)
	}
	defer session.Close()
	tools, err := session.ListTools(ctx, nil)
	if err != nil {
		return fmt.Errorf("list MCP tools: %w", err)
	}
	if len(tools.Tools) != 1 || tools.Tools[0].Name != a.Tool {
		var names []string
		for _, tool := range tools.Tools {
			names = append(names, tool.Name)
		}
		return fmt.Errorf("MCP tools/list = %v, want exactly %s", names, a.Tool)
	}
	test := a.Smoke(time.Now().In(timezone.Beijing).Format("2006-01-02"))
	good, err := callText(ctx, session, &mcp.CallToolParams{Name: a.Tool, Arguments: test.Good})
	if err != nil {
		return err
	}
	text, isError := good.text, good.isError
	var rows []json.RawMessage
	if isError || json.Unmarshal([]byte(text), &rows) != nil || len(rows) == 0 {
		return fmt.Errorf("MCP tool %s %v answered %s, want a non-empty array of rows", a.Tool, test.Good, text)
	}
	bad, err := callText(ctx, session, &mcp.CallToolParams{Name: a.Tool, Arguments: test.Bad})
	if err != nil {
		return err
	}
	text, isError = bad.text, bad.isError
	if answer, ok := problem.Parse([]byte(text)); !isError || !ok || answer.Code != test.BadCode {
		return fmt.Errorf("MCP tool %s %v answered %s, want %s", a.Tool, test.Bad, text, test.BadCode)
	}
	return nil
}

// toolAnswer is the text of a tool's answer and whether it is a tool error.
type toolAnswer struct {
	text    string
	isError bool
}

// callText makes the tool call and returns its answer.
func callText(ctx context.Context, session *mcp.ClientSession, call *mcp.CallToolParams) (toolAnswer, error) {
	result, err := session.CallTool(ctx, call)
	if err != nil {
		return toolAnswer{}, fmt.Errorf("call MCP tool %s: %w", call.Name, err)
	}
	for _, content := range result.Content {
		if text, ok := content.(*mcp.TextContent); ok {
			return toolAnswer{text.Text, result.IsError}, nil
		}
	}
	return toolAnswer{isError: result.IsError}, nil
}
