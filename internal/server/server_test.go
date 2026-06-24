package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gws "github.com/gorilla/websocket"

	"proxy-acp-codex/internal/acpbridge"
	"proxy-acp-codex/internal/config"
	"proxy-acp-codex/internal/platform"
)

func TestQuerySSEAndSubmit(t *testing.T) {
	cfg := testConfig(t)
	manager := acpbridge.NewManager(cfg)
	defer manager.Close()
	handler := New(cfg, manager)

	server := httptest.NewServer(handler)
	defer server.Close()

	root := repoRoot(t)
	body := fmt.Sprintf(`{"requestId":"req_http","runId":"run_http","chatId":"chat_http","agentKey":"fake","message":"needs permission","params":{"cwd":%q}}`, root)
	resp, err := http.Post(server.URL+"/api/query", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("post query: %v", err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Content-Type"); !strings.Contains(got, "text/event-stream") {
		t.Fatalf("content type = %q, want event stream", got)
	}

	reader := bufio.NewReader(resp.Body)
	awaitingID := ""
	var seen []string
	var events []platform.EventData
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		event, ok := readSSEEvent(t, reader)
		if !ok {
			continue
		}
		seen = append(seen, event.Type)
		events = append(events, event)
		if event.Type == "awaiting.ask" {
			awaitingID, _ = event.Payload["awaitingId"].(string)
			break
		}
	}
	if awaitingID == "" {
		t.Fatalf("expected awaiting.ask, seen %#v", seen)
	}

	steerBody := `{"runId":"run_http","message":"after steer"}`
	steerResp, err := http.Post(server.URL+"/api/steer", "application/json", strings.NewReader(steerBody))
	if err != nil {
		t.Fatalf("post steer: %v", err)
	}
	defer steerResp.Body.Close()
	if steerResp.StatusCode != http.StatusOK {
		t.Fatalf("steer status = %d", steerResp.StatusCode)
	}

	submitBody := `{"runId":"run_http","awaitingId":"` + awaitingID + `","params":[{"id":"allow","decision":"approve"}]}`
	submitResp, err := http.Post(server.URL+"/api/submit", "application/json", strings.NewReader(submitBody))
	if err != nil {
		t.Fatalf("post submit: %v", err)
	}
	defer submitResp.Body.Close()
	if submitResp.StatusCode != http.StatusOK {
		t.Fatalf("submit status = %d", submitResp.StatusCode)
	}

	done := false
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read remaining sse: %v", err)
		}
		if strings.TrimSpace(line) == "data: [DONE]" {
			done = true
			break
		}
		if strings.HasPrefix(line, "data: {") {
			event := decodeEventLine(t, line)
			seen = append(seen, event.Type)
			events = append(events, event)
		}
	}
	if !done {
		t.Fatalf("expected done sentinel, seen %#v", seen)
	}
	for _, want := range []string{"request.steer", "request.submit", "awaiting.answer", "run.complete"} {
		if !contains(seen, want) {
			t.Fatalf("missing %s, seen %#v", want, seen)
		}
	}
	assertContentEventOrder(t, events, "run_http",
		"awaiting.ask",
		"request.steer",
		"request.submit",
		"awaiting.answer",
		"run.complete",
	)
}

func TestModelsEndpointUsesCodexDebugModels(t *testing.T) {
	cfg := testConfig(t)
	fakeCodex := filepath.Join(t.TempDir(), "fake-codex.sh")
	script := strings.Join([]string{
		"#!/bin/sh",
		"printf '%s' '{\"models\":[{\"slug\":\"gpt-5.5\",\"display_name\":\"GPT-5.5\",\"context_window\":272000,\"supported_reasoning_levels\":[{\"effort\":\"medium\"}],\"additional_speed_tiers\":[\"fast\"],\"service_tiers\":[{\"id\":\"priority\"}],\"visibility\":\"list\"},{\"slug\":\"gpt-5.3\",\"display_name\":\"GPT-5.3\",\"context_window\":128000,\"supported_reasoning_levels\":[{\"effort\":\"medium\"}],\"service_tiers\":[{\"id\":\"flex\"}],\"visibility\":\"list\"},{\"slug\":\"hidden-model\",\"display_name\":\"Hidden\",\"visibility\":\"hidden\"}]}'",
	}, "\n")
	if err := os.WriteFile(fakeCodex, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake codex: %v", err)
	}
	cfg.Backends[0].Command = config.SelfBackendCommand
	cfg.Backends[0].Args = []string{config.CodexBackendModeArg, "-backend", "app-server", "-codex", fakeCodex}

	manager := acpbridge.NewManager(cfg)
	defer manager.Close()
	handler := New(cfg, manager)
	server := httptest.NewServer(handler)
	defer server.Close()

	resp, err := http.Get(server.URL + "/api/models")
	if err != nil {
		t.Fatalf("get models: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("models status = %d: %s", resp.StatusCode, string(body))
	}
	var decoded platform.APIResponse[platform.ModelCatalogResponse]
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode models response: %v", err)
	}
	if len(decoded.Data.Models) != 2 {
		t.Fatalf("models = %#v", decoded.Data.Models)
	}
	first := decoded.Data.Models[0]
	if first.Key != "gpt-5.5" || first.Name != "GPT-5.5" || first.ModelID != "gpt-5.5" || first.ContextWindow != 272000 || !first.IsReasoner {
		t.Fatalf("first model = %#v", first)
	}
	if strings.Join(first.ServiceTiers, ",") != "FAST" {
		t.Fatalf("first service tiers = %#v", first.ServiceTiers)
	}
	second := decoded.Data.Models[1]
	if strings.Join(second.ServiceTiers, ",") != "FLEX" {
		t.Fatalf("second service tiers = %#v", second.ServiceTiers)
	}
}

func TestAccessLevelEndpointResolvesPendingApproval(t *testing.T) {
	cfg := testConfig(t)
	manager := acpbridge.NewManager(cfg)
	defer manager.Close()
	handler := New(cfg, manager)

	server := httptest.NewServer(handler)
	defer server.Close()

	root := repoRoot(t)
	body := fmt.Sprintf(`{"requestId":"req_access_http","runId":"run_access_http","chatId":"chat_access_http","agentKey":"fake","message":"needs permission","accessLevel":"default","params":{"cwd":%q}}`, root)
	resp, err := http.Post(server.URL+"/api/query", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("post query: %v", err)
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)

	awaiting := false
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		event, ok := readSSEEvent(t, reader)
		if ok && event.Type == "awaiting.ask" {
			awaiting = true
			break
		}
	}
	if !awaiting {
		t.Fatalf("expected awaiting.ask")
	}

	accessResp, err := http.Post(server.URL+"/api/access-level", "application/json", strings.NewReader(`{"runId":"run_access_http","accessLevel":"auto_approve"}`))
	if err != nil {
		t.Fatalf("post access-level: %v", err)
	}
	defer accessResp.Body.Close()
	if accessResp.StatusCode != http.StatusOK {
		t.Fatalf("access status = %d", accessResp.StatusCode)
	}
	var decoded platform.APIResponse[platform.AccessLevelResponse]
	if err := json.NewDecoder(accessResp.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode access response: %v", err)
	}
	if !decoded.Data.Accepted || decoded.Data.AccessLevel != "auto_approve" {
		t.Fatalf("access response = %#v", decoded.Data)
	}

	done := false
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read remaining sse: %v", err)
		}
		if strings.TrimSpace(line) == "data: [DONE]" {
			done = true
			break
		}
	}
	if !done {
		t.Fatalf("expected stream done after access-level update")
	}
}

func TestQueryWebSocketSubmitAndSteer(t *testing.T) {
	cfg := testConfig(t)
	manager := acpbridge.NewManager(cfg)
	defer manager.Close()
	handler := New(cfg, manager)

	server := httptest.NewServer(handler)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws"
	conn, _, err := gws.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	var connected map[string]any
	if err := conn.ReadJSON(&connected); err != nil {
		t.Fatalf("read connected frame: %v", err)
	}

	root := repoRoot(t)
	queryPayload := fmt.Sprintf(`{"requestId":"req_ws","runId":"run_ws","chatId":"chat_ws","agentKey":"fake","message":"needs permission","params":{"cwd":%q}}`, root)
	if err := conn.WriteJSON(requestFrame{Frame: "request", Type: "request.query", ID: "req_ws", Payload: json.RawMessage(queryPayload)}); err != nil {
		t.Fatalf("write websocket query: %v", err)
	}

	awaitingID := ""
	var seen []string
	var events []platform.EventData
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		frame := readWSFrame(t, conn)
		if frame.Event == nil {
			continue
		}
		seen = append(seen, frame.Event.Type)
		events = append(events, *frame.Event)
		if frame.Event.Type == "awaiting.ask" {
			awaitingID, _ = frame.Event.Payload["awaitingId"].(string)
			break
		}
	}
	if awaitingID == "" {
		t.Fatalf("expected awaiting.ask, seen %#v", seen)
	}

	steerPayload := `{"runId":"run_ws","message":"after steer"}`
	if err := conn.WriteJSON(requestFrame{Frame: "request", Type: "request.steer", ID: "steer_ws", Payload: json.RawMessage(steerPayload)}); err != nil {
		t.Fatalf("write websocket steer: %v", err)
	}
	steerResponseSeen := false
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		frame := readWSFrame(t, conn)
		if frame.Event != nil {
			seen = append(seen, frame.Event.Type)
			events = append(events, *frame.Event)
			continue
		}
		if frame.Frame == "response" && frame.ID == "steer_ws" {
			steerResponseSeen = true
			break
		}
	}
	if !steerResponseSeen {
		t.Fatalf("expected steer response, seen %#v", seen)
	}

	submitPayload := `{"runId":"run_ws","awaitingId":"` + awaitingID + `","params":[{"id":"allow","decision":"approve"}]}`
	if err := conn.WriteJSON(requestFrame{Frame: "request", Type: "request.submit", ID: "submit_ws", Payload: json.RawMessage(submitPayload)}); err != nil {
		t.Fatalf("write websocket submit: %v", err)
	}
	submitResponseSeen := false
	done := false
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		frame := readWSFrame(t, conn)
		if frame.Frame == "response" && frame.ID == "submit_ws" {
			submitResponseSeen = true
			continue
		}
		if frame.Event != nil {
			seen = append(seen, frame.Event.Type)
			events = append(events, *frame.Event)
			continue
		}
		if frame.Frame == "stream" && frame.Reason == "done" {
			done = true
			break
		}
	}
	if !submitResponseSeen || !done {
		t.Fatalf("expected submit response and done stream, response=%v done=%v seen=%#v", submitResponseSeen, done, seen)
	}
	for _, want := range []string{"request.steer", "request.submit", "awaiting.answer", "run.complete"} {
		if !contains(seen, want) {
			t.Fatalf("missing %s, seen %#v", want, seen)
		}
	}
	assertContentEventOrder(t, events, "run_ws",
		"awaiting.ask",
		"request.steer",
		"request.submit",
		"awaiting.answer",
		"run.complete",
	)
}

type testWSFrame struct {
	Frame  string              `json:"frame"`
	ID     string              `json:"id"`
	Reason string              `json:"reason"`
	Event  *platform.EventData `json:"event"`
}

func readWSFrame(t *testing.T, conn *gws.Conn) testWSFrame {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	var frame testWSFrame
	if err := conn.ReadJSON(&frame); err != nil {
		t.Fatalf("read websocket frame: %v", err)
	}
	return frame
}

func readSSEEvent(t *testing.T, reader *bufio.Reader) (platform.EventData, bool) {
	t.Helper()
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read sse: %v", err)
	}
	if !strings.HasPrefix(line, "data: {") {
		return platform.EventData{}, false
	}
	return decodeEventLine(t, line), true
}

func decodeEventLine(t *testing.T, line string) platform.EventData {
	t.Helper()
	raw := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
	var event platform.EventData
	if err := json.Unmarshal([]byte(raw), &event); err != nil {
		t.Fatalf("decode event %q: %v", raw, err)
	}
	return event
}

func testConfig(t *testing.T) config.Config {
	t.Helper()
	cfg := config.Config{
		ListenAddr:     "127.0.0.1:0",
		DefaultBackend: "fake",
		Backends: []config.BackendConfig{{
			Key:     "fake",
			Command: "go",
			Args:    []string{"run", "./internal/testagent"},
		}},
	}
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("normalize config: %v", err)
	}
	return cfg
}

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("repo root: %v", err)
	}
	return root
}

func contains(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

func assertContentEventOrder(t *testing.T, events []platform.EventData, runID string, expected ...string) {
	t.Helper()
	positions := make([]int, 0, len(expected))
	searchFrom := 0
	for _, want := range expected {
		idx := eventPosition(events, want, searchFrom)
		if idx < 0 {
			t.Fatalf("missing event %s in %#v", want, eventTypes(events))
		}
		if runID != "" {
			eventRunID, _ := events[idx].Payload["runId"].(string)
			if eventRunID != "" && eventRunID != runID {
				t.Fatalf("event %s runId = %q, want %q", want, eventRunID, runID)
			}
		}
		positions = append(positions, idx)
		searchFrom = idx + 1
	}
	for idx := 1; idx < len(positions); idx++ {
		if positions[idx-1] >= positions[idx] {
			t.Fatalf("events out of order: %v at positions %v, types %#v", expected, positions, eventTypes(events))
		}
	}
}

func eventPosition(events []platform.EventData, want string, start int) int {
	eventType, contentID, hasContentID := strings.Cut(want, ":")
	for idx := start; idx < len(events); idx++ {
		event := events[idx]
		if event.Type != eventType {
			continue
		}
		if hasContentID {
			gotContentID, _ := event.Payload["contentId"].(string)
			if gotContentID != contentID {
				continue
			}
		}
		return idx
	}
	return -1
}

func eventTypes(events []platform.EventData) []string {
	out := make([]string, 0, len(events))
	for _, event := range events {
		out = append(out, event.Type)
	}
	return out
}
