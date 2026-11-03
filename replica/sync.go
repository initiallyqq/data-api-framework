package replica

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrSyncInProgress refuses an administrator's synchronization request while
// one is queued or running.
var ErrSyncInProgress = errors.New("a synchronization is already in progress")

// Monitor is the synchronization state the health endpoints report.
type Monitor struct {
	mu            sync.RWMutex
	lastAttemptAt time.Time
	lastSuccessAt time.Time
	lastError     string
	storageError  string
	inProgress    bool
}

// Snapshot is the Monitor's state at one moment.
type Snapshot struct {
	LastAttemptAt time.Time
	LastSuccessAt time.Time
	LastError     string
	StorageError  string
	InProgress    bool
}

func (m *Monitor) Begin(at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastAttemptAt = at.UTC()
	m.inProgress = true
}

func (m *Monitor) Success(at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastSuccessAt = at.UTC()
	m.lastError = ""
	m.inProgress = false
}

func (m *Monitor) Failure(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastError = err.Error()
	m.inProgress = false
}

// RestoreSuccess carries the last success the copy records across a
// restart, so a restart does not make fresh data look never synchronized.
func (m *Monitor) RestoreSuccess(at time.Time) {
	if at.IsZero() {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastSuccessAt = at.UTC()
}

// SetStorageError records a failed backup or backup check; nil clears it.
func (m *Monitor) SetStorageError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.storageError = ""
	if err != nil {
		m.storageError = err.Error()
	}
}

func (m *Monitor) Snapshot() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return Snapshot{
		LastAttemptAt: m.lastAttemptAt,
		LastSuccessAt: m.lastSuccessAt,
		LastError:     m.lastError,
		StorageError:  m.storageError,
		InProgress:    m.inProgress,
	}
}

// Syncer copies the source into the store: once at start, then every
// Interval, every RetryInterval after a failure, and at once when an
// administrator asks. The source is the copy's only writer.
type Syncer[R any] struct {
	Name          string // prefixes the log lines: the binary's name
	Fetch         func(context.Context) ([]R, error)
	Store         *Store[R]
	Interval      time.Duration
	RetryInterval time.Duration
	Monitor       *Monitor

	mu       sync.Mutex
	requests chan struct{}
	pending  bool
	active   bool
}

func (s *Syncer[R]) Sync(ctx context.Context) (SyncResult, error) {
	at := time.Now().UTC()
	s.Monitor.Begin(at)
	rows, err := s.Fetch(ctx)
	if err != nil {
		err = fmt.Errorf("read DolphinDB: %w", err)
		s.Monitor.Failure(err)
		return SyncResult{}, err
	}
	result, err := s.Store.Synchronize(ctx, rows, at)
	if err != nil {
		err = fmt.Errorf("store the copy: %w", err)
		s.Monitor.Failure(err)
		return SyncResult{}, err
	}
	s.Monitor.Success(at)
	s.Monitor.SetStorageError(result.BackupError)
	return result, nil
}

// RequestSync queues one synchronization now. It is refused while another
// is queued or running.
func (s *Syncer[R]) RequestSync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending || s.active {
		return ErrSyncInProgress
	}
	s.pending = true
	s.queue() <- struct{}{}
	return nil
}

// queue is the one-slot channel of administrator requests; s.mu is held.
func (s *Syncer[R]) queue() chan struct{} {
	if s.requests == nil {
		s.requests = make(chan struct{}, 1)
	}
	return s.requests
}

func (s *Syncer[R]) runOnce(ctx context.Context, requested bool) (SyncResult, error) {
	s.mu.Lock()
	s.active = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.active = false
		if requested {
			s.pending = false
		}
	}()
	syncCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	return s.Sync(syncCtx)
}

// Run synchronizes until ctx ends.
func (s *Syncer[R]) Run(ctx context.Context) error {
	s.mu.Lock()
	requests := s.queue()
	s.mu.Unlock()
	wait := s.Interval
	if s.Monitor.Snapshot().LastError != "" {
		wait = s.RetryInterval
	}
	for {
		timer := time.NewTimer(wait)
		requested := false
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-requests:
			timer.Stop()
			requested = true
		case <-timer.C:
		}
		result, err := s.runOnce(ctx, requested)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			fmt.Printf("%s synchronization failed: %v\n", s.Name, err)
			wait = s.RetryInterval
			continue
		}
		if result.BackupError != nil {
			fmt.Printf("%s backup failed: %v\n", s.Name, result.BackupError)
		}
		fmt.Printf("%s synchronization completed (changed=%t rows=%d)\n", s.Name, result.Changed, result.Rows)
		wait = s.Interval
	}
}
