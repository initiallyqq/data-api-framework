// Package problem is the RFC 9457 error body every data-api endpoint answers
// a refusal with, and its codes.
package problem

import (
	"encoding/json"
	"net/http"
	"strings"
)

// MediaType is the RFC 9457 media type of every error body.
const MediaType = "application/problem+json"

// TypeBase prefixes the code to form the RFC 9457 type URI. The URI names
// the problem type; it is not served. Each service sets it once, at init, to
// a URI under its own public address.
var TypeBase string

// Code is the machine-readable code of a problem. Each service documents the
// codes, their HTTP status and what a caller should do; the MCP adapter
// returns the same object as a tool error, so HTTP and MCP callers branch on
// one set.
type Code string

// The codes every data-api endpoint answers with; catalogue fixes each
// one's status and title.
const (
	MissingParameter   Code = "MISSING_PARAMETER"
	InvalidParameter   Code = "INVALID_PARAMETER"
	UnknownParameter   Code = "UNKNOWN_PARAMETER"
	ResponseTooLarge   Code = "RESPONSE_TOO_LARGE"
	Unauthenticated    Code = "UNAUTHENTICATED"
	OriginNotAllowed   Code = "ORIGIN_NOT_ALLOWED"
	RouteNotFound      Code = "ROUTE_NOT_FOUND"
	CategoryNotFound   Code = "CATEGORY_NOT_FOUND"
	ExchangeNotFound   Code = "EXCHANGE_NOT_FOUND"
	MethodNotAllowed   Code = "METHOD_NOT_ALLOWED"
	SyncInProgress     Code = "SYNC_IN_PROGRESS"
	RequestTooLarge    Code = "REQUEST_TOO_LARGE"
	RateLimited        Code = "RATE_LIMITED"
	InternalError      Code = "INTERNAL_ERROR"
	UpstreamError      Code = "UPSTREAM_ERROR"
	DataUnavailable    Code = "DATA_UNAVAILABLE"
	ServiceUnavailable Code = "SERVICE_UNAVAILABLE"
	UpstreamTimeout    Code = "UPSTREAM_TIMEOUT"
)

// catalogue fixes each code's HTTP status and title: RFC 9457 section 4, a
// problem type has one status and a title that does not vary.
var catalogue = map[Code]struct {
	status int
	title  string
}{
	MissingParameter:   {http.StatusBadRequest, "A required parameter is missing"},
	InvalidParameter:   {http.StatusBadRequest, "A parameter value is malformed"},
	UnknownParameter:   {http.StatusBadRequest, "The endpoint does not take this parameter"},
	ResponseTooLarge:   {http.StatusBadRequest, "The answer exceeds the size limit; narrow the query"},
	Unauthenticated:    {http.StatusUnauthorized, "Missing or invalid credentials"},
	OriginNotAllowed:   {http.StatusForbidden, "The request Origin is not allowed"},
	RouteNotFound:      {http.StatusNotFound, "No such endpoint"},
	CategoryNotFound:   {http.StatusNotFound, "Category not found"},
	ExchangeNotFound:   {http.StatusNotFound, "Exchange not found"},
	MethodNotAllowed:   {http.StatusMethodNotAllowed, "Method not allowed"},
	SyncInProgress:     {http.StatusConflict, "A synchronization is already running"},
	RequestTooLarge:    {http.StatusRequestEntityTooLarge, "The request body is too large"},
	RateLimited:        {http.StatusTooManyRequests, "Too many requests"},
	InternalError:      {http.StatusInternalServerError, "Internal error"},
	UpstreamError:      {http.StatusBadGateway, "The service behind this endpoint failed to answer"},
	DataUnavailable:    {http.StatusServiceUnavailable, "No synchronized data to serve yet"},
	ServiceUnavailable: {http.StatusServiceUnavailable, "The service is temporarily unavailable"},
	UpstreamTimeout:    {http.StatusGatewayTimeout, "The service behind this endpoint timed out"},
}

// Details is one error answer: RFC 9457 problem details plus the code.
// parameter, values and available carry the request-specific facts the
// detail mentions, so a program never parses the detail (Google AIP-193,
// ErrorInfo.metadata).
type Details struct {
	Type      string   `json:"type"`
	Title     string   `json:"title"`
	Status    int      `json:"status"`
	Code      Code     `json:"code"`
	Detail    string   `json:"detail"`
	Parameter string   `json:"parameter,omitempty"`
	Values    []string `json:"values,omitempty"`
	Available []string `json:"available,omitempty"`
	RequestID string   `json:"request_id,omitempty"`
}

// New is the problem code names, with detail. An unknown code is
// INTERNAL_ERROR.
func New(code Code, detail string) *Details {
	entry, ok := catalogue[code]
	if !ok {
		code, entry = InternalError, catalogue[InternalError]
	}
	return &Details{Type: TypeBase + string(code), Title: entry.title, Status: entry.status, Code: code, Detail: detail}
}

// NotFound is a query naming values of parameter that the synchronized data
// does not hold. Values are judged against the data, not a fixed list: the
// answer names the missing ones and what exists, and nothing else is
// returned, because half an answer would read as "not there".
func NotFound(code Code, parameter string, missing, available []string) *Details {
	answer := New(
		code,
		parameter+" not found: "+strings.Join(missing, ", ")+"; available: "+strings.Join(available, ", "),
	)
	answer.Parameter, answer.Values, answer.Available = parameter, missing, available
	return answer
}

// ForParameter names the parameter the problem is about.
func (p *Details) ForParameter(name string) *Details {
	p.Parameter = name
	return p
}

// Error is the code and the detail.
func (p *Details) Error() string { return string(p.Code) + ": " + p.Detail }

// MarshalJSON writes the problem with two more members, error and message,
// both the detail: the interface documents handed to callers promise them
// beside code. RFC 9457 section 3.2 allows extension members.
func (p Details) MarshalJSON() ([]byte, error) {
	type members Details
	return json.Marshal(struct {
		members
		Error   string `json:"error"`
		Message string `json:"message"`
	}{members(p), p.Detail, p.Detail})
}

// Write answers the request with the problem. The gateway's
// X-Request-ID is echoed as request_id so a caller can quote it when
// reporting.
func Write(w http.ResponseWriter, r *http.Request, p *Details) {
	if r != nil && p.RequestID == "" {
		p.RequestID = strings.TrimSpace(r.Header.Get("X-Request-ID"))
	}
	w.Header().Set("Content-Type", MediaType)
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(p.Status)
	_ = json.NewEncoder(w).Encode(p)
}

// WriteMethodNotAllowed answers the 405 problem naming the one method a route takes.
func WriteMethodNotAllowed(w http.ResponseWriter, r *http.Request, allowed string) {
	w.Header().Set("Allow", allowed)
	Write(w, r, New(MethodNotAllowed, allowed+" only"))
}

// Parse reads a problem another data-api endpoint answered with. It
// reports false for anything that is not one, such as a proxy's HTML error
// page.
func Parse(body []byte) (*Details, bool) {
	var p Details
	if err := json.Unmarshal(body, &p); err != nil || p.Code == "" || p.Status == 0 {
		return nil, false
	}
	return &p, true
}
