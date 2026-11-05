// Package harness runs a data service as its deployed binaries against a
// real DolphinDB, for the service's end-to-end tests.
//
// The only double is the data: a scratch database on the DolphinDB named by
// DATA_API_E2E_DOLPHINDB_ADDRESS (CI runs one as a service; locally,
// `docker run -d -p 127.0.0.1:18848:8848
// registry.cn-hangzhou.aliyuncs.com/dolphindbit/dolphindb:v2.00.12.11`).
package harness

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/dolphindb/api-go/v3/dialer"
	"github.com/dolphindb/api-go/v3/dolphindb"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// AdminToken is the administrator token StartService configures.
const AdminToken = "e2e-admin-token-0123456789abcdef"

// DolphinDB is one scratch dfs:// database, dropped when the test ends.
type DolphinDB struct {
	t        *testing.T
	Address  string
	Database string
	client   *dolphindb.Client
}

// ScratchDatabase creates a database named after prefix with partitioning
// (a DolphinDB partition scheme such as `VALUE, 2026.01M..2026.12M`); the
// test is skipped when DATA_API_E2E_DOLPHINDB_ADDRESS is not set.
func ScratchDatabase(t *testing.T, prefix, partitioning string) *DolphinDB {
	t.Helper()
	address := os.Getenv("DATA_API_E2E_DOLPHINDB_ADDRESS")
	if address == "" {
		t.Skip("DATA_API_E2E_DOLPHINDB_ADDRESS is not set")
	}
	var client *dolphindb.Client
	var err error
	for deadline := time.Now().Add(60 * time.Second); ; time.Sleep(time.Second) {
		client, err = dolphindb.Dial(address, "admin", "123456", &dialer.BehaviorOptions{Timeout: time.Minute})
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("connect DolphinDB at %s: %v", address, err)
		}
	}
	t.Cleanup(func() { client.Close() })
	suffix := make([]byte, 4)
	_, _ = rand.Read(suffix)
	d := &DolphinDB{t: t, Address: address, Database: "dfs://" + prefix + hex.EncodeToString(suffix), client: client}
	d.Run(fmt.Sprintf("database(%q, %s)", d.Database, partitioning))
	t.Cleanup(func() { d.Run(fmt.Sprintf("if (existsDatabase(%q)) dropDatabase(%q)", d.Database, d.Database)) })
	return d
}

// Run runs script, failing the test if it fails.
func (d *DolphinDB) Run(script string) {
	d.t.Helper()
	if _, err := d.client.RunScript(script); err != nil {
		d.t.Fatalf("DolphinDB script failed: %v\n%s", err, script)
	}
}

// Build builds the module's commands ./cmd/<name>, as the Dockerfiles do,
// and returns their paths in the order named. The module is the one the
// test runs in.
func Build(t *testing.T, names ...string) []string {
	t.Helper()
	gomod, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil || strings.TrimSpace(string(gomod)) == "" {
		t.Fatalf("find the module of the test: %v", err)
	}
	root := filepath.Dir(strings.TrimSpace(string(gomod)))
	dir := t.TempDir()
	exe := ""
	if runtime.GOOS == "windows" {
		exe = ".exe"
	}
	var paths []string
	for _, name := range names {
		target := filepath.Join(dir, name+exe)
		args := []string{"build", "-o", target}
		if os.Getenv("GOCOVERDIR") != "" {
			// Coverage of the binaries themselves, for pruning against the
			// scenarios: go tool covdata textfmt -i $GOCOVERDIR -o keep.out
			args = append(args, "-cover", "-coverpkg=./...")
		}
		build := exec.Command("go", append(args, "./cmd/"+name)...)
		build.Dir = root
		if output, err := build.CombinedOutput(); err != nil {
			t.Fatalf("go build %s: %v\n%s", name, err, output)
		}
		paths = append(paths, target)
	}
	return paths
}

// FreeAddress is a 127.0.0.1 address nothing listens on.
func FreeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().String()
}

// Process is a started binary.
type Process struct {
	command *exec.Cmd
	mu      sync.Mutex
	output  bytes.Buffer
	done    chan struct{}
}

// Write records the process's output; it is the process's stdout and
// stderr.
func (p *Process) Write(data []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.output.Write(data)
}

// Output is everything the process printed so far.
func (p *Process) Output() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.output.String()
}

// Start runs binary with args until the test ends.
func Start(t *testing.T, binary string, args ...string) *Process {
	t.Helper()
	p := &Process{command: exec.Command(binary, args...), done: make(chan struct{})}
	p.command.Stdout, p.command.Stderr = p, p
	if err := p.command.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { _ = p.command.Wait(); close(p.done) }()
	t.Cleanup(p.Stop)
	return p
}

// Stop asks the process to shut down the way the container runtime does, so
// it exits through its own signal handling (and flushes coverage counters);
// where SIGTERM cannot be delivered (Windows) it is killed.
func (p *Process) Stop() {
	select {
	case <-p.done:
		return
	default:
	}
	if err := p.command.Process.Signal(syscall.SIGTERM); err != nil {
		_ = p.command.Process.Kill()
	}
	select {
	case <-p.done:
	case <-time.After(15 * time.Second):
		_ = p.command.Process.Kill()
		<-p.done
	}
}

// WaitFor polls ready until it holds, failing the test after 60 seconds with
// p's output.
func WaitFor(t *testing.T, what string, p *Process, ready func() bool) {
	t.Helper()
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if ready() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s; output:\n%s", what, p.Output())
}

// WaitHealthy waits until url answers 200.
func WaitHealthy(t *testing.T, url string, p *Process) {
	t.Helper()
	WaitFor(t, url+" to be healthy", p, func() bool {
		status, _ := Get(url)
		return status == http.StatusOK
	})
}

// Get answers a GET of url with its status and body; status 0 when nothing
// answered.
func Get(url string) (int, []byte) {
	response, err := http.Get(url)
	if err != nil {
		return 0, nil
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	return response.StatusCode, body
}

// WriteFile writes content to path, creating its directory.
func WriteFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// Service is a running data service.
type Service struct {
	URL     string
	Process *Process
}

// Upstream is the DolphinDB a service under test reads: d's database, at
// Address (d.Address, or one where nothing answers).
type Upstream struct {
	*DolphinDB
	Address string
}

// StartService runs the service binary with a configuration in root that
// reads upstream: the keys configs/<service>.yaml holds, with SQLite and
// secrets in root. A restart with the same root finds the same SQLite copy.
func StartService(t *testing.T, binary, root string, upstream Upstream) *Service {
	t.Helper()
	address := FreeAddress(t)
	credentials := filepath.Join(root, "dolphindb.env")
	token := filepath.Join(root, "admin-token")
	WriteFile(t, credentials, "DDB_USER=admin\nDDB_PASSWORD=123456\n")
	WriteFile(t, token, AdminToken+"\n")
	config := filepath.Join(root, "service.yaml")
	WriteFile(t, config, strings.Join([]string{
		"listenAddr: " + address,
		"databasePath: " + filepath.ToSlash(filepath.Join(root, "sqlite", "copy.db")),
		"backupPath: " + filepath.ToSlash(filepath.Join(root, "sqlite", "last-good.db")),
		"adminTokenFile: " + filepath.ToSlash(token),
		"dolphinDBAddress: " + upstream.Address,
		"dolphinDBCredentialsFile: " + filepath.ToSlash(credentials),
		"dolphinDBDatabase: " + upstream.Database,
		"",
	}, "\n"))
	return &Service{URL: "http://" + address, Process: Start(t, binary, "--config", config)}
}

// Sync asks the service for a synchronization as the administrator; it
// fails the test unless the request is accepted.
func (s *Service) Sync(t *testing.T) {
	t.Helper()
	request, _ := http.NewRequest(http.MethodPost, s.URL+"/admin/v1/sync", nil)
	request.Header.Set("Authorization", "Bearer "+AdminToken)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("admin sync = HTTP %d", response.StatusCode)
	}
}

// StartAdapter runs the MCP adapter binary in front of a service, named by
// service as the adapter's flag and value (for example "--calendar-url" and
// the service's URL), and connects an MCP client to it.
func StartAdapter(t *testing.T, binary string, service [2]string) (*mcp.ClientSession, string) {
	t.Helper()
	address := FreeAddress(t)
	adapter := Start(t, binary, "--listen-addr", address, service[0], service[1])
	WaitHealthy(t, "http://"+address+"/healthz", adapter)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	session, err := mcp.NewClient(&mcp.Implementation{Name: "e2e", Version: "test"}, nil).
		Connect(ctx, &mcp.StreamableClientTransport{Endpoint: "http://" + address + "/mcp"}, nil)
	if err != nil {
		t.Fatalf("connect MCP: %v\n%s", err, adapter.Output())
	}
	t.Cleanup(func() { session.Close() })
	return session, "http://" + address + "/mcp"
}

// Call makes the tool call and returns its text and whether it is a tool
// error.
func Call(t *testing.T, session *mcp.ClientSession, call *mcp.CallToolParams) (string, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tool, arguments := call.Name, call.Arguments
	result, err := session.CallTool(ctx, call)
	if err != nil {
		t.Fatalf("call %s %v: %v", tool, arguments, err)
	}
	for _, content := range result.Content {
		if text, ok := content.(*mcp.TextContent); ok {
			return text.Text, result.IsError
		}
	}
	t.Fatalf("%s %v returned no text", tool, arguments)
	return "", false
}
