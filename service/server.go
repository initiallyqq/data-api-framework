package service

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/initiallyqq/data-api-framework/endpoint"
	"github.com/initiallyqq/data-api-framework/problem"
	"github.com/initiallyqq/data-api-framework/replica"
)

// Server is a data service's HTTP surface: /healthz, the administrator
// routes /admin/v1/healthz and /admin/v1/sync, and the query routes the
// service adds with Query. Every other path is a 404.
type Server struct {
	noun        string
	config      Config
	store       interface{ Ping(context.Context) error }
	monitor     *replica.Monitor
	syncer      interface{ RequestSync() error }
	adminDigest [sha256.Size]byte
	mux         *http.ServeMux
}

func newServer(noun string, config Config, store interface{ Ping(context.Context) error }, monitor *replica.Monitor, syncer interface{ RequestSync() error }) (*Server, error) {
	content, err := os.ReadFile(config.AdminTokenFile)
	if err != nil {
		return nil, fmt.Errorf("load admin token: %w", err)
	}
	token := strings.TrimSpace(string(content))
	if token == "" {
		return nil, errors.New("load admin token: token file is empty")
	}
	s := &Server{
		noun: noun, config: config, store: store, monitor: monitor, syncer: syncer,
		adminDigest: sha256.Sum256([]byte(token)), mux: http.NewServeMux(),
	}
	s.mux.HandleFunc("/healthz", s.handleHealth)
	s.mux.HandleFunc("/admin/v1/healthz", s.handleAdminHealth)
	s.mux.HandleFunc("/admin/v1/sync", s.handleAdminSync)
	return s, nil
}

// Query serves GET path with answer, whose value is written as the JSON
// body. A failure is the problem it is, or: before the first
// synchronization DATA_UNAVAILABLE; any other a local database failure,
// INTERNAL_ERROR without its detail.
func (s *Server) Query(path string, answer func(*http.Request) (any, error)) {
	s.mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			problem.WriteMethodNotAllowed(w, r, http.MethodGet)
			return
		}
		value, err := answer(r)
		var known *problem.Details
		switch {
		case errors.As(err, &known):
			problem.Write(w, r, known)
		case errors.Is(err, replica.ErrNoData):
			problem.Write(w, r, problem.New(problem.DataUnavailable, s.noun+" data is not available yet"))
		case err != nil:
			problem.Write(w, r, problem.New(problem.InternalError, s.noun+" database operation failed"))
		default:
			endpoint.WriteJSON(w, http.StatusOK, value)
		}
	})
}

func (s *Server) handleAdminSync(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		problem.Write(w, r, problem.New(problem.Unauthenticated, "invalid or missing administrator token"))
		return
	}
	if r.Method != http.MethodPost {
		problem.WriteMethodNotAllowed(w, r, http.MethodPost)
		return
	}
	if err := s.syncer.RequestSync(); err != nil {
		if errors.Is(err, replica.ErrSyncInProgress) {
			problem.Write(w, r, problem.New(problem.SyncInProgress, s.noun+" synchronization is already in progress"))
			return
		}
		problem.Write(w, r, problem.New(problem.ServiceUnavailable, s.noun+" synchronization is unavailable"))
		return
	}
	endpoint.WriteJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		problem.WriteMethodNotAllowed(w, r, http.MethodGet)
		return
	}
	response, status := s.health(r.Context(), false)
	endpoint.WriteJSON(w, status, response)
}

func (s *Server) handleAdminHealth(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		problem.Write(w, r, problem.New(problem.Unauthenticated, "invalid or missing administrator token"))
		return
	}
	if r.Method != http.MethodGet {
		problem.WriteMethodNotAllowed(w, r, http.MethodGet)
		return
	}
	response, status := s.health(r.Context(), true)
	endpoint.WriteJSON(w, status, response)
}

type healthResponse struct {
	Status        string              `json:"status"`
	CheckedAt     string              `json:"checkedAt"`
	Database      healthDatabase      `json:"database"`
	DolphinDB     healthDolphinDB     `json:"dolphinDB"`
	DataFreshness healthDataFreshness `json:"dataFreshness"`
}

type healthDatabase struct {
	Status    string `json:"status"`
	LastError string `json:"lastError,omitempty"`
}

type healthDolphinDB struct {
	Status        string `json:"status"`
	InProgress    bool   `json:"inProgress"`
	LastAttemptAt string `json:"lastAttemptAt,omitempty"`
	LastSuccessAt string `json:"lastSuccessAt,omitempty"`
	LastError     string `json:"lastError,omitempty"`
}

type healthDataFreshness struct {
	Status               string `json:"status"`
	AgeSeconds           int64  `json:"ageSeconds,omitempty"`
	MaxStalenessSeconds  int64  `json:"maxStalenessSeconds"`
	LastSuccessfulSyncAt string `json:"lastSuccessfulSyncAt,omitempty"`
}

// health is 503 when SQLite is unusable, when no synchronization ever
// succeeded, or when the last success is older than maxStaleness; it is
// "degraded" (still 200) while the source fails, a synchronization runs or
// the backup is off inside that window, because the local copy is still
// served. Error texts are for the administrator only.
func (s *Server) health(ctx context.Context, includeDetails bool) (healthResponse, int) {
	state := s.monitor.Snapshot()
	response := healthResponse{
		Status:    "healthy",
		CheckedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Database:  healthDatabase{Status: "healthy"},
		DolphinDB: healthDolphinDB{
			Status:        "unknown",
			InProgress:    state.InProgress,
			LastAttemptAt: formatHealthTime(state.LastAttemptAt),
			LastSuccessAt: formatHealthTime(state.LastSuccessAt),
		},
		DataFreshness: healthDataFreshness{
			Status:              "unknown",
			MaxStalenessSeconds: int64(s.config.MaxStaleness / time.Second),
		},
	}
	status := http.StatusOK
	if err := s.store.Ping(ctx); err != nil {
		response.Status = "unhealthy"
		response.Database.Status = "unavailable"
		if includeDetails {
			response.Database.LastError = err.Error()
		}
		status = http.StatusServiceUnavailable
	} else if state.StorageError != "" {
		response.Status = "degraded"
		response.Database.Status = "degraded"
		if includeDetails {
			response.Database.LastError = state.StorageError
		}
	}
	if includeDetails {
		response.DolphinDB.LastError = state.LastError
	}
	switch {
	case state.InProgress:
		response.DolphinDB.Status = "syncing"
	case state.LastError != "":
		response.DolphinDB.Status = "failed"
	case !state.LastSuccessAt.IsZero():
		response.DolphinDB.Status = "healthy"
	}
	if state.LastSuccessAt.IsZero() {
		response.Status = "unhealthy"
		return response, http.StatusServiceUnavailable
	}
	age := max(time.Since(state.LastSuccessAt), 0)
	response.DataFreshness.Status = "fresh"
	response.DataFreshness.AgeSeconds = int64(age / time.Second)
	response.DataFreshness.LastSuccessfulSyncAt = formatHealthTime(state.LastSuccessAt)
	if age > s.config.MaxStaleness {
		response.Status = "unhealthy"
		response.DataFreshness.Status = "stale"
		return response, http.StatusServiceUnavailable
	}
	if response.Status == "healthy" && response.DolphinDB.Status != "healthy" {
		response.Status = "degraded"
	}
	return response, status
}

func formatHealthTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func (s *Server) authorized(r *http.Request) bool {
	header := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return false
	}
	token := strings.TrimSpace(header[len(prefix):])
	if token == "" {
		return false
	}
	actual := sha256.Sum256([]byte(token))
	return subtle.ConstantTimeCompare(actual[:], s.adminDigest[:]) == 1
}
