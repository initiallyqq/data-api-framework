package endpoint

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Server is the HTTP server a binary runs: handler on addr, with the read
// and write timeouts its slowest answer needs.
func Server(addr string, handler http.Handler, readTimeout, writeTimeout time.Duration) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 15 * time.Second,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       60 * time.Second,
	}
}

// Run serves server until ctx ends, then shuts it down gracefully.
func Run(ctx context.Context, name string, server *http.Server) error {
	errCh := make(chan error, 1)
	go func() {
		fmt.Printf("%s listening on %s\n", name, server.Addr)
		errCh <- server.ListenAndServe()
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// Healthcheck is a binary's `healthcheck [-url] [-timeout]` command: it
// fails unless url answers 2xx.
func Healthcheck(name, defaultURL string, args []string) error {
	flags := flag.NewFlagSet(name+" healthcheck", flag.ContinueOnError)
	url := flags.String("url", defaultURL, "health endpoint")
	timeout := flags.Duration("timeout", 5*time.Second, "request timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("%s healthcheck accepts flags only", name)
	}
	client := http.Client{Timeout: *timeout}
	response, err := client.Get(*url)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("health endpoint returned %s", response.Status)
	}
	return nil
}

// WriteJSON answers with body as JSON.
func WriteJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
