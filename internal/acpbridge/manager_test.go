package acpbridge

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/coder/acp-go-sdk"

	"proxy-acp-codex/internal/config"
	"proxy-acp-codex/internal/platform"
)

type recordingSink struct {
	mu     sync.Mutex
	events []platform.EventData
}

func (s *recordingSink) Publish(event platform.EventData) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	return nil
}

func (s *recordingSink) snapshot() []platform.EventData {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]platform.EventData(nil), s.events...)
}

func TestBackendCommandPathResolvesRelativePaths(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)

	got, err := backendCommandPath("./bin/custom-acp")
	if err != nil {
		t.Fatalf("resolve command: %v", err)
	}
	want := filepath.Join(dir, "bin", "custom-acp")
	if got != want {
		t.Fatalf("command = %q, want %q", got, want)
	}
}

func TestBackendCommandPathResolvesSelfCommand(t *testing.T) {
	got, err := backendCommandPath(config.SelfBackendCommand)
	if err != nil {
		t.Fatalf("resolve command: %v", err)
	}
	want, err := os.Executable()
	if err != nil {
		t.Fatalf("current executable: %v", err)
	}
	if got != want {
		t.Fatalf("command = %q, want current executable %q", got, want)
	}
}

func TestBackendCommandPathLeavesPathLookupCommands(t *testing.T) {
	got, err := backendCommandPath("custom-acp")
	if err != nil {
		t.Fatalf("resolve command: %v", err)
	}
	if got != "custom-acp" {
		t.Fatalf("command = %q, want PATH lookup command", got)
	}
}

func TestManagerExecuteMapsACPUpdatesToPlatformEvents(t *testing.T) {
	m := NewManager(testConfig(t))
	defer m.Close()

	sink := &recordingSink{}
	err := m.Execute(context.Background(), platform.QueryRequest{
		RequestID: "req_1",
		RunID:     "run_1",
		ChatID:    "chat_1",
		AgentKey:  "fake",
		Message:   "world",
		Params:    testParams(t),
	}, sink)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	events := sink.snapshot()
	types := eventTypes(events)
	for _, want := range []string{
		"request.query",
		"run.start",
		"reasoning.start",
		"reasoning.delta",
		"content.start",
		"content.delta",
		"tool.start",
		"tool.args",
		"tool.end",
		"tool.result",
		"content.end",
		"reasoning.end",
		"run.complete",
	} {
		if !contains(types, want) {
			t.Fatalf("missing event %s in %#v", want, types)
		}
	}
	if got := events[len(events)-1].Type; got != "run.complete" {
		t.Fatalf("last event = %s, want run.complete", got)
	}
}

func TestManagerExecuteRequiresCWDParam(t *testing.T) {
	m := NewManager(testConfig(t))
	defer m.Close()

	err := m.Execute(context.Background(), platform.QueryRequest{
		RequestID: "req_missing_cwd",
		RunID:     "run_missing_cwd",
		ChatID:    "chat_missing_cwd",
		AgentKey:  "fake",
		Message:   "world",
	}, &recordingSink{})
	if err == nil || !strings.Contains(err.Error(), "params.cwd is required") {
		t.Fatalf("error = %v, want missing cwd error", err)
	}
}

func TestRequestModelPrefersModelID(t *testing.T) {
	got := requestModel(platform.QueryRequest{Model: &platform.ModelOptions{Key: "gpt-5", ModelID: "gpt-5-codex"}})
	if got != "gpt-5-codex" {
		t.Fatalf("model = %q, want modelId", got)
	}
	got = requestModel(platform.QueryRequest{Model: &platform.ModelOptions{Key: "gpt-5"}})
	if got != "gpt-5" {
		t.Fatalf("model = %q, want key fallback", got)
	}
}

func TestBackendArgsWithModel(t *testing.T) {
	base := []string{config.CodexBackendModeArg, "-backend", "app-server"}
	got := backendArgsWithModel(base, "gpt-5-codex")
	want := []string{config.CodexBackendModeArg, "-backend", "app-server", "-model", "gpt-5-codex"}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("args = %#v, want %#v", got, want)
	}
	withoutModel := backendArgsWithModel(base, "")
	if strings.Join(withoutModel, "\x00") != strings.Join(base, "\x00") {
		t.Fatalf("args without model = %#v, want %#v", withoutModel, base)
	}
}

func TestManagerSubmitResolvesACPPermission(t *testing.T) {
	m := NewManager(testConfig(t))
	defer m.Close()

	sink := &recordingSink{}
	done := make(chan error, 1)
	go func() {
		done <- m.Execute(context.Background(), platform.QueryRequest{
			RequestID: "req_2",
			RunID:     "run_2",
			ChatID:    "chat_2",
			AgentKey:  "fake",
			Message:   "needs permission",
			Params:    testParams(t),
		}, sink)
	}()

	var awaitingID string
	for i := 0; i < 200; i++ {
		for _, event := range sink.snapshot() {
			if event.Type == "awaiting.ask" {
				awaitingID, _ = event.Payload["awaitingId"].(string)
				break
			}
		}
		if awaitingID != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if awaitingID == "" {
		t.Fatalf("expected awaiting.ask, got %#v", eventTypes(sink.snapshot()))
	}
	resp, ok := m.Submit(platform.SubmitRequest{
		RunID:      "run_2",
		AwaitingID: awaitingID,
		Params:     []map[string]any{{"id": "allow", "decision": "approve"}},
	})
	if !ok || !resp.Accepted {
		t.Fatalf("submit response = %#v ok=%v", resp, ok)
	}
	if err := <-done; err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !contains(eventTypes(sink.snapshot()), "awaiting.answer") {
		t.Fatalf("expected awaiting.answer, got %#v", eventTypes(sink.snapshot()))
	}
}

func TestManagerSteerQueuesFollowUpPromptBeforeRunComplete(t *testing.T) {
	m := NewManager(testConfig(t))
	defer m.Close()

	sink := &recordingSink{}
	done := make(chan error, 1)
	go func() {
		done <- m.Execute(context.Background(), platform.QueryRequest{
			RequestID: "req_steer",
			RunID:     "run_steer",
			ChatID:    "chat_steer",
			AgentKey:  "fake",
			Message:   "needs permission",
			Params:    testParams(t),
		}, sink)
	}()

	var awaitingID string
	for i := 0; i < 200; i++ {
		for _, event := range sink.snapshot() {
			if event.Type == "awaiting.ask" {
				awaitingID, _ = event.Payload["awaitingId"].(string)
				break
			}
		}
		if awaitingID != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if awaitingID == "" {
		t.Fatalf("expected awaiting.ask, got %#v", eventTypes(sink.snapshot()))
	}

	steer := m.Steer(platform.SteerRequest{RunID: "run_steer", ChatID: "chat_steer", Message: "after steer"})
	if !steer.Accepted || steer.SteerID == "" {
		t.Fatalf("steer response = %#v", steer)
	}
	resp, ok := m.Submit(platform.SubmitRequest{
		RunID:      "run_steer",
		AwaitingID: awaitingID,
		Params:     []map[string]any{{"id": "allow", "decision": "approve"}},
	})
	if !ok || !resp.Accepted {
		t.Fatalf("submit response = %#v ok=%v", resp, ok)
	}
	if err := <-done; err != nil {
		t.Fatalf("execute: %v", err)
	}

	types := eventTypes(sink.snapshot())
	if !contains(types, "request.steer") {
		t.Fatalf("expected request.steer, got %#v", types)
	}
	if !containsDelta(sink.snapshot(), "after steer") {
		t.Fatalf("expected queued steer prompt output, got %#v", sink.snapshot())
	}
	assertEventOrder(t, types, "request.steer", "run.complete")
}

func TestManagerSubmitResolvesQuestionAwaiting(t *testing.T) {
	m := NewManager(testConfig(t))
	defer m.Close()

	sink := &recordingSink{}
	done := make(chan error, 1)
	go func() {
		done <- m.Execute(context.Background(), platform.QueryRequest{
			RequestID: "req_3",
			RunID:     "run_3",
			ChatID:    "chat_3",
			AgentKey:  "fake",
			Message:   "needs question",
			Params:    testParams(t),
		}, sink)
	}()

	var awaiting platform.EventData
	for i := 0; i < 200; i++ {
		for _, event := range sink.snapshot() {
			if event.Type == "awaiting.ask" && event.Payload["mode"] == "question" {
				awaiting = event
				break
			}
		}
		if awaiting.Type != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if awaiting.Type == "" {
		t.Fatalf("expected question awaiting.ask, got %#v", eventTypes(sink.snapshot()))
	}
	if got := awaiting.Payload["viewportKey"]; got != "question" {
		t.Fatalf("viewportKey = %#v", got)
	}
	questions, ok := awaiting.Payload["questions"].([]map[string]any)
	if !ok || len(questions) != 1 {
		t.Fatalf("questions = %#v", awaiting.Payload["questions"])
	}
	awaitingID, _ := awaiting.Payload["awaitingId"].(string)
	resp, ok := m.Submit(platform.SubmitRequest{
		RunID:      "run_3",
		AwaitingID: awaitingID,
		Params:     []map[string]any{{"answer": "提升代码质量"}},
	})
	if !ok || !resp.Accepted {
		t.Fatalf("submit response = %#v ok=%v", resp, ok)
	}
	if err := <-done; err != nil {
		t.Fatalf("execute: %v", err)
	}
	var answer platform.EventData
	var submit platform.EventData
	for _, event := range sink.snapshot() {
		if event.Type == "request.submit" {
			submit = event
		}
		if event.Type == "awaiting.answer" && event.Payload["mode"] == "question" {
			answer = event
		}
	}
	if submit.Type == "" {
		t.Fatalf("expected request.submit, got %#v", eventTypes(sink.snapshot()))
	}
	if answer.Type == "" {
		t.Fatalf("expected question awaiting.answer, got %#v", eventTypes(sink.snapshot()))
	}
	answers, ok := answer.Payload["answers"].([]map[string]any)
	if !ok || len(answers) != 1 || answers[0]["answer"] != "提升代码质量" {
		t.Fatalf("answers = %#v", answer.Payload["answers"])
	}
	if got := answers[0]["id"]; got != "work_focus" {
		t.Fatalf("answer question id = %#v", got)
	}

	events := sink.snapshot()
	assertContentEventOrder(t, events, "run_3",
		"content.start:run_3_c_1",
		"content.end:run_3_c_1",
		"awaiting.ask",
		"request.submit",
		"awaiting.answer",
		"content.start:run_3_c_2",
		"content.end:run_3_c_2",
		"run.complete",
	)
}

func TestManagerSubmitDismissesQuestionAwaiting(t *testing.T) {
	m := NewManager(testConfig(t))
	defer m.Close()

	sink := &recordingSink{}
	done := make(chan error, 1)
	go func() {
		done <- m.Execute(context.Background(), platform.QueryRequest{
			RequestID: "req_4",
			RunID:     "run_4",
			ChatID:    "chat_4",
			AgentKey:  "fake",
			Message:   "needs question",
			Params:    testParams(t),
		}, sink)
	}()

	var awaitingID string
	for i := 0; i < 200; i++ {
		for _, event := range sink.snapshot() {
			if event.Type == "awaiting.ask" && event.Payload["mode"] == "question" {
				awaitingID, _ = event.Payload["awaitingId"].(string)
				break
			}
		}
		if awaitingID != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if awaitingID == "" {
		t.Fatalf("expected question awaiting.ask, got %#v", eventTypes(sink.snapshot()))
	}
	resp, ok := m.Submit(platform.SubmitRequest{RunID: "run_4", AwaitingID: awaitingID, Params: []map[string]any{}})
	if !ok || !resp.Accepted {
		t.Fatalf("submit response = %#v ok=%v", resp, ok)
	}
	if err := <-done; err != nil {
		t.Fatalf("execute: %v", err)
	}
	for _, event := range sink.snapshot() {
		if event.Type != "awaiting.answer" || event.Payload["mode"] != "question" {
			continue
		}
		if event.Payload["status"] != "error" {
			t.Fatalf("dismiss status = %#v", event.Payload["status"])
		}
		errorPayload, _ := event.Payload["error"].(map[string]any)
		if errorPayload["code"] != "user_dismissed" {
			t.Fatalf("dismiss error = %#v", errorPayload)
		}
		return
	}
	t.Fatalf("expected dismissed awaiting.answer, got %#v", eventTypes(sink.snapshot()))
}

func TestBridgeClientWriteTextFile(t *testing.T) {
	client := testBridgeClient(t)
	path := filepath.Join(t.TempDir(), "note.txt")
	if _, err := client.WriteTextFile(context.Background(), acp.WriteTextFileRequest{
		Path:    path,
		Content: "hello codex",
	}); err != nil {
		t.Fatalf("write text file: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read written file: %v", err)
	}
	if string(data) != "hello codex" {
		t.Fatalf("content = %q", data)
	}
}

func TestBridgeClientTerminalLifecycle(t *testing.T) {
	client := testBridgeClient(t)
	resp, err := client.CreateTerminal(context.Background(), acp.CreateTerminalRequest{
		Command: "sh",
		Args:    []string{"-c", "printf hello"},
	})
	if err != nil {
		t.Fatalf("create terminal: %v", err)
	}
	status, err := client.WaitForTerminalExit(context.Background(), acp.WaitForTerminalExitRequest{TerminalId: resp.TerminalId})
	if err != nil {
		t.Fatalf("wait terminal: %v", err)
	}
	if status.ExitCode == nil || *status.ExitCode != 0 {
		t.Fatalf("exit status = %#v", status)
	}
	output, err := client.TerminalOutput(context.Background(), acp.TerminalOutputRequest{TerminalId: resp.TerminalId})
	if err != nil {
		t.Fatalf("terminal output: %v", err)
	}
	if output.Output != "hello" || output.Truncated {
		t.Fatalf("output = %#v", output)
	}
	if _, err := client.ReleaseTerminal(context.Background(), acp.ReleaseTerminalRequest{TerminalId: resp.TerminalId}); err != nil {
		t.Fatalf("release terminal: %v", err)
	}
}

func TestBridgeClientKillTerminal(t *testing.T) {
	client := testBridgeClient(t)
	resp, err := client.CreateTerminal(context.Background(), acp.CreateTerminalRequest{
		Command: "sh",
		Args:    []string{"-c", "sleep 5"},
	})
	if err != nil {
		t.Fatalf("create terminal: %v", err)
	}
	if _, err := client.KillTerminal(context.Background(), acp.KillTerminalRequest{TerminalId: resp.TerminalId}); err != nil {
		t.Fatalf("kill terminal: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	status, err := client.WaitForTerminalExit(ctx, acp.WaitForTerminalExitRequest{TerminalId: resp.TerminalId})
	if err != nil {
		t.Fatalf("wait killed terminal: %v", err)
	}
	if status.ExitCode == nil && status.Signal == nil {
		t.Fatalf("expected killed status, got %#v", status)
	}
}

func TestBridgeClientTerminalOutputLimitPreservesUTF8(t *testing.T) {
	client := testBridgeClient(t)
	resp, err := client.CreateTerminal(context.Background(), acp.CreateTerminalRequest{
		Command:         "sh",
		Args:            []string{"-c", "printf '你好世界'"},
		OutputByteLimit: intPtr(7),
	})
	if err != nil {
		t.Fatalf("create terminal: %v", err)
	}
	if _, err := client.WaitForTerminalExit(context.Background(), acp.WaitForTerminalExitRequest{TerminalId: resp.TerminalId}); err != nil {
		t.Fatalf("wait terminal: %v", err)
	}
	output, err := client.TerminalOutput(context.Background(), acp.TerminalOutputRequest{TerminalId: resp.TerminalId})
	if err != nil {
		t.Fatalf("terminal output: %v", err)
	}
	if !output.Truncated {
		t.Fatalf("expected truncated output")
	}
	if !utf8.ValidString(output.Output) {
		t.Fatalf("output is not valid utf8: %q", output.Output)
	}
	if strings.Contains(output.Output, string(utf8.RuneError)) {
		t.Fatalf("output contains replacement rune: %q", output.Output)
	}
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

func testBridgeClient(t *testing.T) *bridgeClient {
	t.Helper()
	dir := t.TempDir()
	cfg := config.BackendConfig{
		Key:     config.DefaultCodexBackendKey,
		Command: "codex-cli-acp",
		Args:    []string{"-codex", "codex"},
	}
	writes := true
	terminal := true
	reads := true
	cfg.Capabilities.Fs.ReadTextFile = &reads
	cfg.Capabilities.Fs.WriteTextFile = &writes
	cfg.Capabilities.Terminal = &terminal
	sess := &backendSession{cfg: cfg, cwd: dir}
	return &bridgeClient{session: sess, terminals: map[string]*terminalProcess{}}
}

func testParams(t *testing.T) map[string]any {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("repo root: %v", err)
	}
	return map[string]any{"cwd": root}
}

func intPtr(v int) *int {
	return &v
}

func eventTypes(events []platform.EventData) []string {
	out := make([]string, 0, len(events))
	for _, event := range events {
		out = append(out, event.Type)
	}
	return out
}

func contains(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

func containsDelta(events []platform.EventData, want string) bool {
	for _, event := range events {
		if event.Type == "content.delta" && event.Payload["delta"] == want {
			return true
		}
	}
	return false
}

func assertEventOrder(t *testing.T, events []string, before string, after string) {
	t.Helper()
	beforeIndex := -1
	afterIndex := -1
	for idx, event := range events {
		if event == before && beforeIndex < 0 {
			beforeIndex = idx
		}
		if event == after && afterIndex < 0 {
			afterIndex = idx
		}
	}
	if beforeIndex < 0 || afterIndex < 0 || beforeIndex >= afterIndex {
		t.Fatalf("expected %s before %s, got %#v", before, after, events)
	}
}

func assertContentEventOrder(t *testing.T, events []platform.EventData, runID string, expected ...string) {
	t.Helper()
	positions := make([]int, 0, len(expected))
	for _, want := range expected {
		idx := eventPosition(events, want)
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
	}
	for idx := 1; idx < len(positions); idx++ {
		if positions[idx-1] >= positions[idx] {
			t.Fatalf("events out of order: %v at positions %v, types %#v", expected, positions, eventTypes(events))
		}
	}
}

func eventPosition(events []platform.EventData, want string) int {
	eventType, contentID, hasContentID := strings.Cut(want, ":")
	for idx, event := range events {
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
