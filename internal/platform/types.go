package platform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

const DoneSentinel = "[DONE]"

type QueryRequest struct {
	RequestID  string         `json:"requestId,omitempty"`
	RunID      string         `json:"runId,omitempty"`
	ChatID     string         `json:"chatId,omitempty"`
	AgentKey   string         `json:"agentKey,omitempty"`
	TeamID     string         `json:"teamId,omitempty"`
	Role       string         `json:"role,omitempty"`
	Message    string         `json:"message"`
	References []Reference    `json:"references,omitempty"`
	Params     map[string]any `json:"params,omitempty"`
	Model      *ModelOptions  `json:"model,omitempty"`
	Scene      *Scene         `json:"scene,omitempty"`
	Stream     *bool          `json:"stream,omitempty"`
	Hidden     *bool          `json:"hidden,omitempty"`
}

type ModelOptions struct {
	Key             string `json:"key,omitempty"`
	ModelID         string `json:"modelId,omitempty"`
	ReasoningEffort string `json:"reasoningEffort,omitempty"`
}

type Scene struct {
	URL   string `json:"url,omitempty"`
	Title string `json:"title,omitempty"`
}

type Reference struct {
	ID          string         `json:"id,omitempty"`
	Type        string         `json:"type,omitempty"`
	Name        string         `json:"name,omitempty"`
	MimeType    string         `json:"mimeType,omitempty"`
	SizeBytes   *int64         `json:"sizeBytes,omitempty"`
	URL         string         `json:"url,omitempty"`
	SHA256      string         `json:"sha256,omitempty"`
	SandboxPath string         `json:"sandboxPath,omitempty"`
	Meta        map[string]any `json:"meta,omitempty"`
}

type SubmitRequest struct {
	RunID      string           `json:"runId"`
	AwaitingID string           `json:"awaitingId"`
	Params     []map[string]any `json:"params"`
}

type SteerRequest struct {
	RequestID string `json:"requestId,omitempty"`
	ChatID    string `json:"chatId,omitempty"`
	RunID     string `json:"runId"`
	SteerID   string `json:"steerId,omitempty"`
	AgentKey  string `json:"agentKey,omitempty"`
	TeamID    string `json:"teamId,omitempty"`
	Message   string `json:"message"`
}

type InterruptRequest struct {
	RequestID string `json:"requestId,omitempty"`
	ChatID    string `json:"chatId,omitempty"`
	RunID     string `json:"runId"`
	AgentKey  string `json:"agentKey,omitempty"`
	TeamID    string `json:"teamId,omitempty"`
	Message   string `json:"message,omitempty"`
}

type SubmitResponse struct {
	Accepted   bool   `json:"accepted"`
	Status     string `json:"status"`
	RunID      string `json:"runId"`
	AwaitingID string `json:"awaitingId"`
	Detail     string `json:"detail"`
}

type SteerResponse struct {
	Accepted bool   `json:"accepted"`
	Status   string `json:"status"`
	RunID    string `json:"runId"`
	SteerID  string `json:"steerId"`
	Detail   string `json:"detail"`
}

type InterruptResponse struct {
	Accepted bool   `json:"accepted"`
	Status   string `json:"status"`
	RunID    string `json:"runId"`
	Detail   string `json:"detail"`
}

type APIResponse[T any] struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data T      `json:"data"`
}

func Success[T any](data T) APIResponse[T] {
	return APIResponse[T]{Code: 0, Msg: "success", Data: data}
}

func Failure(code int, msg string) APIResponse[map[string]any] {
	return APIResponse[map[string]any]{Code: code, Msg: msg, Data: map[string]any{}}
}

type EventData struct {
	Seq       int64          `json:"seq"`
	Type      string         `json:"type"`
	Timestamp int64          `json:"timestamp"`
	Payload   map[string]any `json:"-"`
}

func NewEvent(seq int64, eventType string, payload map[string]any) EventData {
	return EventData{
		Seq:       seq,
		Type:      eventType,
		Timestamp: time.Now().UnixMilli(),
		Payload:   clonePayload(payload),
	}
}

func EventDataFromMap(data map[string]any) EventData {
	payload := clonePayload(data)
	if payload == nil {
		payload = map[string]any{}
	}
	seq, _ := int64Value(payload["seq"])
	timestamp, _ := int64Value(payload["timestamp"])
	eventType, _ := payload["type"].(string)
	delete(payload, "seq")
	delete(payload, "timestamp")
	delete(payload, "type")
	return EventData{Seq: seq, Type: eventType, Timestamp: timestamp, Payload: payload}
}

func (d EventData) Map() map[string]any {
	out := clonePayload(d.Payload)
	if out == nil {
		out = map[string]any{}
	}
	out["seq"] = d.Seq
	out["type"] = d.Type
	out["timestamp"] = d.Timestamp
	return out
}

func (d EventData) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	first := true
	writeField := func(key string, value any) error {
		if value == nil {
			return nil
		}
		keyJSON, err := json.Marshal(key)
		if err != nil {
			return err
		}
		valueJSON, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if !first {
			buf.WriteByte(',')
		}
		first = false
		buf.Write(keyJSON)
		buf.WriteByte(':')
		buf.Write(valueJSON)
		return nil
	}

	if err := writeField("seq", d.Seq); err != nil {
		return nil, err
	}
	if err := writeField("type", d.Type); err != nil {
		return nil, err
	}
	payload := clonePayload(d.Payload)
	for _, key := range orderedPayloadKeys(d.Type) {
		if value, ok := payload[key]; ok {
			if err := writeField(key, value); err != nil {
				return nil, err
			}
			delete(payload, key)
		}
	}
	if len(payload) > 0 {
		keys := make([]string, 0, len(payload))
		for key := range payload {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if err := writeField(key, payload[key]); err != nil {
				return nil, err
			}
		}
	}
	if err := writeField("timestamp", d.Timestamp); err != nil {
		return nil, err
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func (d *EventData) UnmarshalJSON(data []byte) error {
	var raw map[string]any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&raw); err != nil {
		return err
	}
	*d = EventDataFromMap(raw)
	return nil
}

func DecodeJSON[T any](data []byte) (T, error) {
	var value T
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&value); err != nil {
		return value, err
	}
	return value, nil
}

func orderedPayloadKeys(eventType string) []string {
	switch eventType {
	case "request.query":
		return []string{"requestId", "runId", "chatId", "agentKey", "role", "message", "references", "params", "model", "scene"}
	case "run.start":
		return []string{"runId", "chatId", "agentKey"}
	case "content.start":
		return []string{"contentId", "runId", "taskId"}
	case "content.delta":
		return []string{"contentId", "delta"}
	case "content.end":
		return []string{"contentId"}
	case "reasoning.start":
		return []string{"reasoningId", "runId", "taskId", "reasoningLabel"}
	case "reasoning.delta":
		return []string{"reasoningId", "delta"}
	case "reasoning.end":
		return []string{"reasoningId"}
	case "tool.start":
		return []string{"toolId", "runId", "taskId", "toolName", "toolLabel", "toolDescription"}
	case "tool.args":
		return []string{"toolId", "delta", "chunkIndex"}
	case "tool.end":
		return []string{"toolId"}
	case "tool.result":
		return []string{"toolId", "result", "hitl", "error", "exitCode"}
	case "awaiting.ask":
		return []string{"awaitingId", "mode", "viewportType", "viewportKey", "timeout", "runId", "questions", "approvals", "forms"}
	case "request.submit":
		return []string{"requestId", "chatId", "runId", "awaitingId", "params"}
	case "awaiting.answer":
		return []string{"awaitingId", "mode", "status", "answers", "approvals", "error"}
	case "request.steer":
		return []string{"requestId", "chatId", "runId", "steerId", "message"}
	case "plan.create", "plan.update":
		return []string{"planId", "chatId", "plan"}
	case "run.complete":
		return []string{"runId", "finishReason", "usage"}
	case "run.cancel":
		return []string{"runId", "usage"}
	case "run.error":
		return []string{"runId", "error", "usage"}
	default:
		return nil
	}
}

func clonePayload(payload map[string]any) map[string]any {
	if payload == nil {
		return nil
	}
	out := make(map[string]any, len(payload))
	for key, value := range payload {
		out[key] = value
	}
	return out
}

func int64Value(value any) (int64, bool) {
	switch typed := value.(type) {
	case int64:
		return typed, true
	case int:
		return int64(typed), true
	case float64:
		return int64(typed), true
	case json.Number:
		parsed, err := typed.Int64()
		return parsed, err == nil
	case string:
		var parsed int64
		if _, err := fmt.Sscanf(strings.TrimSpace(typed), "%d", &parsed); err == nil {
			return parsed, true
		}
	}
	return 0, false
}
