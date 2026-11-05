// Package service is a data service binary: its configuration, its HTTP
// surface (the query, /healthz and the administrator routes) and Main.
package service

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/initiallyqq/data-api-framework/endpoint"
	"github.com/initiallyqq/data-api-framework/replica"
	"github.com/initiallyqq/data-api-framework/upstream"
)

// Spec is one data service: what it is called, how it keeps its copy in
// SQLite, how it reads its source tables and the query it answers.
type Spec[R any] struct {
	Name              string // the binary's name, in logs and errors
	Noun              string // what errors call the data: "calendar" or "mapping"
	DefaultListenAddr string
	Open              func(replica.Files) (*replica.Store[R], error)
	Fetch             func(context.Context, upstream.Source) ([]R, error)
	// Register adds the service's query routes to server, answering from
	// store.
	Register func(server *Server, store *replica.Store[R])
}

// Handler is the service's HTTP surface over r, as Main serves it.
func (s Spec[R]) Handler(config Config, r Replica[R]) (http.Handler, error) {
	server, err := newServer(s.Noun, config, r)
	if err != nil {
		return nil, err
	}
	s.Register(server, r.Store)
	return server.mux, nil
}

// Main is the binary: `--config <file>` serves, `healthcheck [-url]` probes
// a running one, as the container's health check does.
func (s Spec[R]) Main(args []string) {
	if err := s.run(args); err != nil {
		fmt.Fprintln(os.Stderr, s.Name+":", err)
		os.Exit(1)
	}
}

func (s Spec[R]) run(args []string) error {
	if len(args) > 0 && args[0] == "healthcheck" {
		return endpoint.Healthcheck(s.Name, "http://127.0.0.1"+s.DefaultListenAddr+"/healthz", args[1:])
	}
	flags := flag.NewFlagSet(s.Name, flag.ContinueOnError)
	configPath := flags.String("config", s.Name+".yaml", "configuration file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("%s accepts flags only", s.Name)
	}
	config, err := LoadConfig(*configPath, s.DefaultListenAddr)
	if err != nil {
		return err
	}
	store, err := s.Open(replica.Files{Path: config.DatabasePath, BackupPath: config.BackupPath})
	if err != nil {
		return err
	}
	defer store.Close()
	syncer, err := s.start(config, store)
	if err != nil {
		return err
	}
	handler, err := s.Handler(config, Replica[R]{Store: store, Monitor: syncer.Monitor, Syncer: syncer})
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	go func() {
		if err := syncer.Run(ctx); err != nil && ctx.Err() == nil {
			fmt.Printf("%s synchronization stopped: %v\n", s.Name, err)
			cancel()
		}
	}()
	server := endpoint.Server(config.ListenAddr, handler, config.RequestTimeout, config.RequestTimeout+10*time.Second)
	return endpoint.Run(ctx, s.Name, server)
}

// start readies store to serve: its schema, its recorded state carried into
// the monitor, and one synchronization. That synchronization may fail only
// when the copy already holds data to serve. The returned syncer keeps the
// copy current once run.
func (s Spec[R]) start(config Config, store *replica.Store[R]) (*replica.Syncer[R], error) {
	startup, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	if err := store.Migrate(startup); err != nil {
		return nil, err
	}
	if err := store.Ping(startup); err != nil {
		return nil, fmt.Errorf("connect SQLite: %w", err)
	}
	state, err := store.State(startup)
	if err != nil {
		return nil, fmt.Errorf("read SQLite state: %w", err)
	}
	monitor := &replica.Monitor{}
	monitor.RestoreSuccess(state.LastSourceSuccessAt)
	if state.HasData {
		monitor.SetStorageError(store.CheckBackup(startup, state.Digest))
	}
	source := upstream.Source{
		Address:         config.DolphinDBAddress,
		CredentialsFile: config.DolphinDBCredentialsFile,
		Database:        config.DolphinDBDatabase,
		Timeout:         10 * time.Minute,
	}
	syncer := &replica.Syncer[R]{
		Name:          s.Name,
		Fetch:         func(ctx context.Context) ([]R, error) { return s.Fetch(ctx, source) },
		Store:         store,
		Interval:      config.SyncInterval,
		RetryInterval: config.SyncRetryInterval,
		Monitor:       monitor,
	}
	result, err := syncer.Sync(startup)
	switch {
	case err != nil && !state.HasData:
		return nil, fmt.Errorf("initial synchronization: %w", err)
	case err != nil:
		fmt.Printf("%s initial synchronization failed; serving existing SQLite state: %v\n", s.Name, err)
	default:
		fmt.Printf("%s initial synchronization completed (changed=%t rows=%d)\n", s.Name, result.Changed, result.Rows)
		if result.BackupError != nil {
			fmt.Printf("%s initial backup failed: %v\n", s.Name, result.BackupError)
		}
	}
	return syncer, nil
}
