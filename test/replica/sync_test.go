package replica_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/initiallyqq/data-api-framework/replica"
)

// Failure modes of a synchronization: the source fails (the cause must reach
// the administrator, the last success must stay), the store refuses the
// answer, the backup fails (the synchronization still counts), and an
// administrator asks while one is queued or running.

func TestSyncRecordsWhatHappenedForTheHealthEndpoints(t *testing.T) {
	ctx := context.Background()
	store := openItems(t, t.TempDir(), itemLayout{})
	answer, fetchErr := items("a"), error(nil)
	monitor := &replica.Monitor{}
	syncer := &replica.Syncer[item]{
		Name:    "test",
		Fetch:   func(context.Context) ([]item, error) { return answer, fetchErr },
		Store:   store,
		Monitor: monitor,
	}
	started := time.Now().UTC()
	if result, err := syncer.Sync(ctx); err != nil || !result.Changed {
		t.Fatalf("sync = %+v, %v", result, err)
	}
	state := monitor.Snapshot()
	if state.LastSuccessAt.Before(started) || !state.LastAttemptAt.Equal(state.LastSuccessAt) || state.LastError != "" || state.InProgress || state.StorageError != "" {
		t.Fatalf("after a success: %+v", state)
	}
	succeeded := state.LastSuccessAt

	fetchErr = errors.New("DolphinDB is down")
	if _, err := syncer.Sync(ctx); !errors.Is(err, fetchErr) {
		t.Fatalf("source failure = %v, want it to wrap the cause", err)
	}
	state = monitor.Snapshot()
	if !state.LastSuccessAt.Equal(succeeded) || !strings.Contains(state.LastError, "DolphinDB is down") || state.InProgress {
		t.Fatalf("after a source failure: %+v", state)
	}

	answer, fetchErr = nil, nil
	if _, err := syncer.Sync(ctx); err == nil {
		t.Fatal("an empty answer was stored")
	}
	if state := monitor.Snapshot(); state.LastError == "" || !state.LastSuccessAt.Equal(succeeded) {
		t.Fatalf("after a refused answer: %+v", state)
	}

	answer = items("a")
	if _, err := syncer.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if state := monitor.Snapshot(); state.LastError != "" {
		t.Fatalf("a success did not clear the failure: %+v", state)
	}
}

func TestOneAdministratorSyncAtATime(t *testing.T) {
	store := openItems(t, t.TempDir(), itemLayout{})
	release := make(chan struct{})
	fetched := make(chan struct{}, 2)
	syncer := &replica.Syncer[item]{
		Name: "test",
		Fetch: func(context.Context) ([]item, error) {
			fetched <- struct{}{}
			<-release
			return items("a"), nil
		},
		Store:         store,
		Interval:      time.Hour,
		RetryInterval: time.Hour,
		Monitor:       &replica.Monitor{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- syncer.Run(ctx) }()
	defer func() {
		cancel()
		<-done
	}()

	if err := syncer.RequestSync(); err != nil {
		t.Fatal(err)
	}
	if err := syncer.RequestSync(); !errors.Is(err, replica.ErrSyncInProgress) {
		t.Fatalf("a second request while one is queued = %v", err)
	}
	select {
	case <-fetched:
	case <-time.After(5 * time.Second):
		t.Fatal("the requested synchronization did not run")
	}
	if err := syncer.RequestSync(); !errors.Is(err, replica.ErrSyncInProgress) {
		t.Fatalf("a request while one runs = %v", err)
	}
	close(release)
	// Once it finished, the next request is accepted.
	deadline := time.Now().Add(5 * time.Second)
	for syncer.RequestSync() != nil {
		if time.Now().After(deadline) {
			t.Fatal("requests stayed refused after the synchronization finished")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
