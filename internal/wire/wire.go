// Package wire holds task-manager's view of the broker contract
// (agents.md rules 9 and 10): header names, the request envelope
// {params, body}, replies with an HTTP-equivalent status and the flat
// stream message envelope. Every type here matches a schema under
// svc/task-manager/schemas/ or a publisher's schema it consumes.
package wire

import (
	"bytes"
	"encoding/json"
	"strconv"
	"time"

	"github.com/pandops/flowai-task-manager/internal/ids"
)

// Service is task-manager's publisher name.
const Service = "task-manager"

// Header names.
const (
	HeaderTeam         = "team"
	HeaderRequestID    = "request_id"
	HeaderOperatorID   = "operator_id"
	HeaderTeamName     = "team_name"
	HeaderAdminSubject = "admin_subject"
	HeaderIfMatch      = "If-Match"
	HeaderStatus       = "status"
	HeaderMsgID        = "Nats-Msg-Id"
	TeamAdmin          = "admin"
)

// TimeFormat is RFC 3339 UTC with milliseconds (agents.md rule 10).
const TimeFormat = "2006-01-02T15:04:05.000Z"

// FormatTime renders t in TimeFormat.
func FormatTime(t time.Time) string { return t.UTC().Format(TimeFormat) }

// Msg is one received message or request.
type Msg struct {
	Subject string
	Header  map[string]string
	Data    []byte
}

// Reply is an answer to a request.
type Reply struct {
	Status int
	Header map[string]string
	Body   []byte
}

// ErrorBody is the body of every error reply.
type ErrorBody struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID *int64 `json:"request_id,omitempty"` // absent without a valid header (D12)
}

// Request is the decoded request envelope.
type Request struct {
	Params map[string]json.RawMessage `json:"params"`
	Body   json.RawMessage            `json:"body"`
}

// DecodeRequest decodes the {params, body} envelope. An empty payload is
// an empty request.
func DecodeRequest(data []byte) (Request, error) {
	var r Request
	if len(bytes.TrimSpace(data)) == 0 {
		return r, nil
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&r); err != nil {
		return Request{}, err
	}
	return r, nil
}

// Strings returns the values of a string parameter: absent → (nil,false);
// a JSON string → one value; a JSON array of strings → its values (a
// repeated query key). Anything else returns ok with a nil slice marked bad.
func (r Request) Strings(name string) (values []string, present bool, valid bool) {
	raw, ok := r.Params[name]
	if !ok {
		return nil, false, true
	}
	var one string
	if json.Unmarshal(raw, &one) == nil {
		return []string{one}, true, true
	}
	var many []string
	if json.Unmarshal(raw, &many) == nil {
		return many, true, true
	}
	return nil, true, false
}

// ID returns an identifier parameter that must be a JSON integer number.
func (r Request) ID(name string) (int64, bool, error) {
	raw, ok := r.Params[name]
	if !ok {
		return 0, false, nil
	}
	v, err := ids.ParseJSON(raw)
	return v, true, err
}

// Int returns a numeric parameter given as a JSON number or a decimal
// string.
func (r Request) Int(name string) (int, bool, bool) {
	raw, ok := r.Params[name]
	if !ok {
		return 0, false, true
	}
	text := string(bytes.TrimSpace(raw))
	var s string
	if json.Unmarshal(raw, &s) == nil {
		text = s
	}
	v, err := strconv.Atoi(text)
	if err != nil || strconv.Itoa(v) != text {
		return 0, true, false
	}
	return v, true, true
}

// JSON builds a reply with a JSON body.
func JSON(status int, v any) Reply {
	body, err := json.Marshal(v)
	if err != nil {
		return Error(500, "internal_error", "reply could not be encoded", nil)
	}
	return Reply{Status: status, Body: body}
}

// Error builds an error reply.
func Error(status int, code, message string, requestID *int64) Reply {
	body, _ := json.Marshal(ErrorBody{Code: code, Message: message, RequestID: requestID})
	return Reply{Status: status, Body: body}
}

// Envelope is the flat stream message envelope every consumed message
// carries.
type Envelope struct {
	Publisher    string `json:"publisher"`
	PublisherSeq int64  `json:"publisher_seq"`
	TeamID       int64  `json:"team_id"`
	ExecutorID   int64  `json:"executor_id"`
	Kind         string `json:"kind"`
	OccurredAt   string `json:"occurred_at"`
}

// PublisherKey is the deduplication key of a message's publisher:
// the service name, or executor-<team_id>-<executor_id> for an Executor.
func PublisherKey(publisher string, teamID, executorID int64) string {
	if publisher == "executor" {
		return "executor-" + ids.Token(teamID) + "-" + ids.Token(executorID)
	}
	return publisher
}

// MsgID is the Nats-Msg-Id of a task-manager message.
func MsgID(seq int64) string { return Service + "-" + ids.Token(seq) }
