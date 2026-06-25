package acpbridge

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
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

func TestPermissionApprovalsFormatsCommandAndExecpolicyOption(t *testing.T) {
	tool := acp.ToolCallUpdate{
		ToolCallId: "cmd_1",
		RawInput: map[string]any{
			"command": "cat /Users/joe/live_full_access_probe2.txt",
			"reason":  "Write the requested probe file outside the workspace and verify its contents.",
		},
	}
	options := []acp.PermissionOption{
		{OptionId: "accept", Name: "Allow once", Kind: acp.PermissionOptionKindAllowOnce},
		{OptionId: "decision_json:abc", Name: "Allow once", Kind: acp.PermissionOptionKindAllowOnce},
		{OptionId: "decline", Name: "Reject", Kind: acp.PermissionOptionKindRejectOnce},
	}

	approvals := permissionApprovals(tool, options)
	if len(approvals) != 1 {
		t.Fatalf("approvals len = %d, want 1", len(approvals))
	}
	approval := approvals[0]
	if got := approval["command"]; got != "cat /Users/joe/live_full_access_probe2.txt" {
		t.Fatalf("command = %#v", got)
	}
	if got := approval["description"]; got != "Write the requested probe file outside the workspace and verify its contents." {
		t.Fatalf("description = %#v", got)
	}
	rawOptions, _ := approval["options"].([]map[string]any)
	if len(rawOptions) != 1 {
		t.Fatalf("options len = %d, want 1", len(rawOptions))
	}
	if got := rawOptions[0]["decision"]; got != "approve" {
		t.Fatalf("first decision = %#v, want approve", got)
	}
}

func TestPermissionOutcomeFromSubmitApprovePrefersExecpolicyAmendment(t *testing.T) {
	outcome := permissionOutcomeFromSubmit([]map[string]any{{
		"id":       "cmd_2",
		"decision": "approve",
	}}, []acp.PermissionOption{
		{OptionId: "accept", Name: "Allow once", Kind: acp.PermissionOptionKindAllowOnce},
		{OptionId: "decision_json:abc", Name: "Allow once", Kind: acp.PermissionOptionKindAllowOnce},
	})
	if outcome.Selected == nil || outcome.Selected.OptionId != "decision_json:abc" {
		t.Fatalf("outcome = %#v, want decision_json:abc", outcome)
	}
}

func TestTurnSplitsReasoningByThoughtMessageID(t *testing.T) {
	m := NewManager(config.Config{})
	defer m.Close()

	sink := &recordingSink{}
	turn := newTurn(platform.QueryRequest{
		RunID:    "run_reasoning",
		ChatID:   "chat_reasoning",
		AgentKey: "fake",
	}, sink, m)

	id1 := "reasoning_item_1"
	id2 := "reasoning_item_2"
	if err := turn.handleUpdate(acp.SessionUpdate{
		AgentThoughtChunk: &acp.SessionUpdateAgentThoughtChunk{
			Content:       acp.TextBlock("first"),
			MessageId:     &id1,
			SessionUpdate: "agent_thought_chunk",
		},
	}); err != nil {
		t.Fatalf("first update: %v", err)
	}
	if err := turn.handleUpdate(acp.SessionUpdate{
		AgentThoughtChunk: &acp.SessionUpdateAgentThoughtChunk{
			Content:       acp.TextBlock("second"),
			MessageId:     &id2,
			SessionUpdate: "agent_thought_chunk",
		},
	}); err != nil {
		t.Fatalf("second update: %v", err)
	}
	turn.closeOpenTextStreams()

	events := sink.snapshot()
	if got, want := eventTypes(events), []string{
		"reasoning.start",
		"reasoning.delta",
		"reasoning.end",
		"reasoning.start",
		"reasoning.delta",
		"reasoning.end",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("event types = %#v, want %#v", got, want)
	}
	if got, _ := events[0].Payload["reasoningId"].(string); got != id1 {
		t.Fatalf("first reasoning id = %q, want %q", got, id1)
	}
	if got, _ := events[2].Payload["reasoningId"].(string); got != id1 {
		t.Fatalf("first reasoning end id = %q, want %q", got, id1)
	}
	if got, _ := events[3].Payload["reasoningId"].(string); got != id2 {
		t.Fatalf("second reasoning id = %q, want %q", got, id2)
	}
	if got, _ := events[5].Payload["reasoningId"].(string); got != id2 {
		t.Fatalf("second reasoning end id = %q, want %q", got, id2)
	}
}

func TestTurnAutoSegmentsReasoningAroundToolUse(t *testing.T) {
	m := NewManager(config.Config{})
	defer m.Close()

	sink := &recordingSink{}
	turn := newTurn(platform.QueryRequest{
		RunID:    "run_reasoning_tool",
		ChatID:   "chat_reasoning_tool",
		AgentKey: "fake",
	}, sink, m)

	reasoningID := "reasoning_item_1"
	if err := turn.handleUpdate(acp.SessionUpdate{
		AgentThoughtChunk: &acp.SessionUpdateAgentThoughtChunk{
			Content:       acp.TextBlock("before tool"),
			MessageId:     &reasoningID,
			SessionUpdate: "agent_thought_chunk",
		},
	}); err != nil {
		t.Fatalf("first reasoning update: %v", err)
	}
	if err := turn.handleUpdate(acp.SessionUpdate{
		ToolCall: &acp.SessionUpdateToolCall{
			ToolCallId: "tool_1",
			Title:      "Run command",
			Kind:       acp.ToolKindExecute,
		},
	}); err != nil {
		t.Fatalf("tool update: %v", err)
	}
	if err := turn.handleUpdate(acp.SessionUpdate{
		AgentThoughtChunk: &acp.SessionUpdateAgentThoughtChunk{
			Content:       acp.TextBlock("after tool"),
			MessageId:     &reasoningID,
			SessionUpdate: "agent_thought_chunk",
		},
	}); err != nil {
		t.Fatalf("second reasoning update: %v", err)
	}
	turn.closeOpenTextStreams()

	events := sink.snapshot()
	if got, want := eventTypes(events), []string{
		"reasoning.start",
		"reasoning.delta",
		"reasoning.end",
		"tool.start",
		"reasoning.start",
		"reasoning.delta",
		"reasoning.end",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("event types = %#v, want %#v", got, want)
	}
	if got, _ := events[0].Payload["reasoningId"].(string); got != reasoningID {
		t.Fatalf("first segment id = %q, want %q", got, reasoningID)
	}
	if got, _ := events[4].Payload["reasoningId"].(string); got != reasoningID+"_segment_2" {
		t.Fatalf("second segment id = %q, want %q", got, reasoningID+"_segment_2")
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

func TestRequestReasoningEffortNormalizesPlatformValue(t *testing.T) {
	got := requestReasoningEffort(platform.QueryRequest{Model: &platform.ModelOptions{ReasoningEffort: "HIGH"}})
	if got != "high" {
		t.Fatalf("reasoning effort = %q, want high", got)
	}
	got = requestReasoningEffort(platform.QueryRequest{Model: &platform.ModelOptions{ReasoningEffort: "MEDIUM"}})
	if got != "medium" {
		t.Fatalf("reasoning effort = %q, want medium", got)
	}
	got = requestReasoningEffort(platform.QueryRequest{Model: &platform.ModelOptions{ReasoningEffort: "LOW"}})
	if got != "low" {
		t.Fatalf("reasoning effort = %q, want low", got)
	}
	got = requestReasoningEffort(platform.QueryRequest{Model: &platform.ModelOptions{ReasoningEffort: "EXTRA_HIGH"}})
	if got != "xhigh" {
		t.Fatalf("reasoning effort = %q, want xhigh", got)
	}
	got = requestReasoningEffort(platform.QueryRequest{Model: &platform.ModelOptions{ReasoningEffort: "NONE"}})
	if got != "" {
		t.Fatalf("reasoning effort = %q, want empty for NONE", got)
	}
}

func TestRequestServiceTierNormalizesPlatformValue(t *testing.T) {
	got := requestServiceTier(platform.QueryRequest{Model: &platform.ModelOptions{ServiceTier: "FAST"}})
	if got != "fast" {
		t.Fatalf("service tier = %q, want fast", got)
	}
	got = requestServiceTier(platform.QueryRequest{Model: &platform.ModelOptions{ServiceTier: "STANDARD"}})
	if got != "" {
		t.Fatalf("standard service tier = %q, want empty", got)
	}
	got = requestServiceTier(platform.QueryRequest{Params: map[string]any{"serviceTier": "priority"}})
	if got != "fast" {
		t.Fatalf("params service tier = %q, want fast", got)
	}
}

func TestRequestAccessLevelMapsToCodexPolicy(t *testing.T) {
	opts := requestCodexSessionOptions(platform.QueryRequest{})
	if opts.accessLevel != "" || opts.approvalPolicy != "" || opts.sandboxMode != "" {
		t.Fatalf("empty access options = %#v", opts)
	}

	req := platform.QueryRequest{AccessLevel: "auto_approve"}
	opts = requestCodexSessionOptions(req)
	if opts.accessLevel != "auto_approve" || opts.approvalPolicy != "on-failure" || opts.sandboxMode != "workspace-write" {
		t.Fatalf("auto approve options = %#v", opts)
	}

	req = platform.QueryRequest{AccessLevel: "full_access"}
	opts = requestCodexSessionOptions(req)
	if opts.accessLevel != "full_access" || opts.approvalPolicy != "never" || opts.sandboxMode != "danger-full-access" {
		t.Fatalf("full access options = %#v", opts)
	}

	req = platform.QueryRequest{Params: map[string]any{"accessLevel": "default"}}
	opts = requestCodexSessionOptions(req)
	if opts.accessLevel != "default" || opts.approvalPolicy != "on-request" || opts.sandboxMode != "workspace-write" {
		t.Fatalf("default options = %#v", opts)
	}
}

func TestRequestCodexSessionOptionsAllowExplicitCodexOverrides(t *testing.T) {
	opts := requestCodexSessionOptions(platform.QueryRequest{
		AccessLevel: "default",
		Params: map[string]any{
			"approvalPolicy": "never",
			"sandboxMode":    "danger-full-access",
		},
	})
	if opts.approvalPolicy != "never" || opts.sandboxMode != "danger-full-access" {
		t.Fatalf("options = %#v", opts)
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

func TestBackendArgsWithReasoningEffort(t *testing.T) {
	base := []string{config.CodexBackendModeArg, "-backend", "app-server"}
	got := backendArgsWithModelOptions(base, codexSessionOptions{model: "gpt-5.5", reasoningEffort: "high", serviceTier: "fast"})
	want := []string{config.CodexBackendModeArg, "-backend", "app-server", "-model", "gpt-5.5", "-model-reasoning-effort", "high", "-service-tier", "fast"}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("args = %#v, want %#v", got, want)
	}
}

func TestBackendArgsWithAccessPolicy(t *testing.T) {
	base := []string{config.CodexBackendModeArg, "-backend", "app-server"}
	got := backendArgsWithSessionOptions(base, codexSessionOptions{approvalPolicy: "never", sandboxMode: "danger-full-access"})
	want := []string{config.CodexBackendModeArg, "-backend", "app-server", "-approval-policy", "never", "-sandbox-mode", "danger-full-access"}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("args = %#v, want %#v", got, want)
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
	awaiting := findEvent(sink.snapshot(), "awaiting.ask")
	assertEventIdentity(t, awaiting, "run_2", "chat_2", "fake")
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
	assertEventIdentity(t, findEvent(sink.snapshot(), "request.submit"), "run_2", "chat_2", "fake")
	assertEventIdentity(t, findEvent(sink.snapshot(), "awaiting.answer"), "run_2", "chat_2", "fake")
}

func TestManagerUpdateAccessLevelResolvesPendingApproval(t *testing.T) {
	m := NewManager(testConfig(t))
	defer m.Close()

	sink := &recordingSink{}
	done := make(chan error, 1)
	go func() {
		done <- m.Execute(context.Background(), platform.QueryRequest{
			RequestID:   "req_access",
			RunID:       "run_access",
			ChatID:      "chat_access",
			AgentKey:    "fake",
			Message:     "needs permission",
			AccessLevel: "default",
			Params:      testParams(t),
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

	resp, ok := m.UpdateAccessLevel(platform.AccessLevelRequest{RunID: "run_access", AccessLevel: "auto_approve"})
	if !ok || !resp.Accepted || resp.PreviousAccessLevel != "default" || resp.AccessLevel != "auto_approve" {
		t.Fatalf("access response = %#v ok=%v", resp, ok)
	}
	if err := <-done; err != nil {
		t.Fatalf("execute: %v", err)
	}
	answer := findEvent(sink.snapshot(), "awaiting.answer")
	if answer.Type == "" || answer.Payload["status"] != "answered" {
		t.Fatalf("expected answered approval, got %#v", sink.snapshot())
	}
}

func TestManagerUpdateAccessLevelRejectsInvalidValue(t *testing.T) {
	m := NewManager(testConfig(t))
	resp, ok := m.UpdateAccessLevel(platform.AccessLevelRequest{RunID: "run_missing", AccessLevel: "root"})
	if ok || resp.Accepted || resp.Status != "invalid" {
		t.Fatalf("access response = %#v ok=%v", resp, ok)
	}
}

func TestPermissionOutcomePrefersAllowAlwaysForRuleDecision(t *testing.T) {
	outcome := permissionOutcomeFromSubmit([]map[string]any{{"decision": "approve_rule_run"}}, []acp.PermissionOption{
		{OptionId: "allow_once", Name: "Allow once", Kind: acp.PermissionOptionKindAllowOnce},
		{OptionId: "allow_session", Name: "Allow for session", Kind: acp.PermissionOptionKindAllowAlways},
		{OptionId: "reject", Name: "Reject", Kind: acp.PermissionOptionKindRejectOnce},
	})
	if outcome.Selected == nil || outcome.Selected.OptionId != "allow_session" {
		t.Fatalf("outcome = %#v, want allow_session", outcome)
	}
}

func TestPermissionOutcomeRuleDecisionOverridesCollapsedApprovalID(t *testing.T) {
	outcome := permissionOutcomeFromSubmit([]map[string]any{{"id": "allow_once", "decision": "approve_rule_run"}}, []acp.PermissionOption{
		{OptionId: "allow_once", Name: "Allow once", Kind: acp.PermissionOptionKindAllowOnce},
		{OptionId: "allow_session", Name: "Allow for session", Kind: acp.PermissionOptionKindAllowAlways},
		{OptionId: "reject", Name: "Reject", Kind: acp.PermissionOptionKindRejectOnce},
	})
	if outcome.Selected == nil || outcome.Selected.OptionId != "allow_session" {
		t.Fatalf("outcome = %#v, want allow_session", outcome)
	}
}

func TestPermissionOutcomeFullAccessPrefersExecpolicyAmendment(t *testing.T) {
	outcome := permissionOutcomeFromAccessLevel("full_access", []acp.PermissionOption{
		{OptionId: "accept", Name: "Allow once", Kind: acp.PermissionOptionKindAllowOnce},
		{OptionId: acp.PermissionOptionId("decision_json:abc"), Name: "Allow once", Kind: acp.PermissionOptionKindAllowOnce},
		{OptionId: "decline", Name: "Reject", Kind: acp.PermissionOptionKindRejectOnce},
	})
	if outcome.Selected == nil || outcome.Selected.OptionId != "decision_json:abc" {
		t.Fatalf("outcome = %#v, want decision_json:abc", outcome)
	}
}

func TestPermissionApprovalsCollapseRejectOption(t *testing.T) {
	title := "Run command"
	approvals := permissionApprovals(acp.ToolCallUpdate{
		ToolCallId: "tool_1",
		Title:      &title,
	}, []acp.PermissionOption{
		{OptionId: "accept", Name: "Allow once", Kind: acp.PermissionOptionKindAllowOnce},
		{OptionId: "decline", Name: "Reject", Kind: acp.PermissionOptionKindRejectOnce},
	})
	if len(approvals) != 1 {
		t.Fatalf("approvals = %#v, want one approval", approvals)
	}
	options, ok := approvals[0]["options"].([]map[string]any)
	if !ok {
		t.Fatalf("approval options = %#v", approvals[0]["options"])
	}
	if len(options) != 1 {
		t.Fatalf("approval options = %#v, want allow only", options)
	}
	if options[0]["decision"] != "approve" || options[0]["label"] != "同意" {
		t.Fatalf("first option = %#v, want normalized approve option", options[0])
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
		"awaiting.ask",
		"request.submit",
		"awaiting.answer",
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
	resp := createApprovedTerminal(t, client, acp.CreateTerminalRequest{
		Command: "sh",
		Args:    []string{"-c", "printf hello"},
	})
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
	resp := createApprovedTerminal(t, client, acp.CreateTerminalRequest{
		Command: "sh",
		Args:    []string{"-c", "sleep 5"},
	})
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
	resp := createApprovedTerminal(t, client, acp.CreateTerminalRequest{
		Command:         "sh",
		Args:            []string{"-c", "printf '你好世界'"},
		OutputByteLimit: intPtr(7),
	})
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

func createApprovedTerminal(t *testing.T, client *bridgeClient, params acp.CreateTerminalRequest) acp.CreateTerminalResponse {
	t.Helper()
	m := NewManager(config.Config{})
	sink := &recordingSink{}
	runID := "run_terminal_" + time.Now().UTC().Format("20060102150405.000000000")
	turn := newTurn(platform.QueryRequest{RunID: runID, ChatID: "chat_terminal", AgentKey: "fake"}, sink, m)

	client.session.mu.Lock()
	client.session.active = turn
	client.session.mu.Unlock()
	defer func() {
		client.session.mu.Lock()
		if client.session.active == turn {
			client.session.active = nil
		}
		client.session.mu.Unlock()
	}()

	type result struct {
		resp acp.CreateTerminalResponse
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := client.CreateTerminal(context.Background(), params)
		done <- result{resp: resp, err: err}
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
		t.Fatalf("expected terminal awaiting.ask, got %#v", eventTypes(sink.snapshot()))
	}

	submit, _ := m.Submit(platform.SubmitRequest{
		RunID:      runID,
		AwaitingID: awaitingID,
		Params:     []map[string]any{{"id": "allow", "decision": "approve"}},
	})
	if !submit.Accepted {
		t.Fatalf("submit = %#v", submit)
	}

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("create terminal: %v", got.err)
		}
		types := eventTypes(sink.snapshot())
		if !contains(types, "awaiting.answer") {
			t.Fatalf("expected awaiting.answer, got %#v", types)
		}
		return got.resp
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for terminal creation")
		return acp.CreateTerminalResponse{}
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

func findEvent(events []platform.EventData, eventType string) platform.EventData {
	for _, event := range events {
		if event.Type == eventType {
			return event
		}
	}
	return platform.EventData{}
}

func assertEventIdentity(t *testing.T, event platform.EventData, runID string, chatID string, agentKey string) {
	t.Helper()
	if event.Type == "" {
		t.Fatalf("missing event for identity assertion")
	}
	if got, _ := event.Payload["runId"].(string); got != runID {
		t.Fatalf("%s runId = %q, want %q", event.Type, got, runID)
	}
	if got, _ := event.Payload["chatId"].(string); got != chatID {
		t.Fatalf("%s chatId = %q, want %q", event.Type, got, chatID)
	}
	if got, _ := event.Payload["agentKey"].(string); got != agentKey {
		t.Fatalf("%s agentKey = %q, want %q", event.Type, got, agentKey)
	}
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
