package codexacp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
)

func TestParseLineCapturesThreadID(t *testing.T) {
	parser := &codexStreamParser{}

	parsed, err := parser.parseLine([]byte(`{"type":"thread.started","thread_id":"019e631d-4038-7942-a981-d01d07d9d633"}`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.threadID != "019e631d-4038-7942-a981-d01d07d9d633" {
		t.Fatalf("threadID = %q", parsed.threadID)
	}
	if len(parsed.chunks) != 0 {
		t.Fatalf("chunks = %#v", parsed.chunks)
	}
}

func TestParseLineEmitsTextDeltas(t *testing.T) {
	parser := &codexStreamParser{}

	parsed, err := parser.parseLine([]byte(`{"type":"agent_message.delta","delta":"hel"}`))
	if err != nil {
		t.Fatalf("parse first delta: %v", err)
	}
	if !reflect.DeepEqual(parsed.chunks, []string{"hel"}) {
		t.Fatalf("first chunks = %#v", parsed.chunks)
	}

	parsed, err = parser.parseLine([]byte(`{"type":"response.output_text.delta","text":"lo"}`))
	if err != nil {
		t.Fatalf("parse second delta: %v", err)
	}
	if !reflect.DeepEqual(parsed.chunks, []string{"lo"}) {
		t.Fatalf("second chunks = %#v", parsed.chunks)
	}

	parsed, err = parser.parseLine([]byte(`{"type":"item.completed","item":{"id":"item_0","type":"agent_message","text":" done"}}`))
	if err != nil {
		t.Fatalf("parse completed item: %v", err)
	}
	if !reflect.DeepEqual(parsed.chunks, []string{" done"}) {
		t.Fatalf("completed chunks = %#v", parsed.chunks)
	}
}

func TestParseLineIgnoresMalformedAndLifecycleEvents(t *testing.T) {
	parser := &codexStreamParser{}

	for _, line := range [][]byte{
		[]byte(`Reading additional input from stdin...`),
		[]byte(`{"type":"turn.started"}`),
		[]byte(`{"type":"tool.started","id":"tool_1"}`),
	} {
		parsed, err := parser.parseLine(line)
		if err != nil {
			t.Fatalf("parse %s: %v", line, err)
		}
		if parsed.threadID != "" || len(parsed.chunks) != 0 {
			t.Fatalf("parsed %s = %#v", line, parsed)
		}
	}
}

func TestParseLineReturnsErrorMessage(t *testing.T) {
	parser := &codexStreamParser{}

	parsed, err := parser.parseLine([]byte(`{"type":"error","message":"boom"}`))
	if !errors.Is(err, errCodexCLI) {
		t.Fatalf("error = %v", err)
	}
	if !reflect.DeepEqual(parsed.chunks, []string{"boom"}) {
		t.Fatalf("chunks = %#v", parsed.chunks)
	}
}

func TestParseLineIgnoresTransientReconnectError(t *testing.T) {
	parser := &codexStreamParser{}

	for _, line := range [][]byte{
		[]byte(`{"type":"error","message":"Reconnecting... 2/5 (stream disconnected before completion: Connection refused (os error 61))"}`),
		[]byte(`{"type":"error","message":"Reconnecting... 2/5 (request timed out)"}`),
	} {
		parsed, err := parser.parseLine(line)
		if err != nil {
			t.Fatalf("parse %s: %v", line, err)
		}
		if parsed.threadID != "" || len(parsed.chunks) != 0 {
			t.Fatalf("parsed %s = %#v", line, parsed)
		}
	}
}

func TestCodexArgsStartAndResume(t *testing.T) {
	args := codexArgs(nil, "/tmp/work", "", "hi")
	want := []string{"exec", "--json", "--skip-git-repo-check", "-C", "/tmp/work", "hi"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("start args = %#v, want %#v", args, want)
	}

	args = codexArgs([]string{"--model", "gpt-5"}, "/tmp/work", "thread_1", "again")
	want = []string{"exec", "resume", "--model", "gpt-5", "--json", "--skip-git-repo-check", "thread_1", "again"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("resume args = %#v, want %#v", args, want)
	}
	if strings.Join(args, "\x00") == "" {
		t.Fatalf("args should not be empty")
	}
}

func TestApplyCodexModelArgs(t *testing.T) {
	execArgs, appArgs := applyCodexModelArgs(backendAppServer, []string{"--permission-mode", "ask"}, []string{"--enable", "network_proxy"}, "gpt-5-codex", "high")
	if !reflect.DeepEqual(execArgs, []string{"--permission-mode", "ask"}) {
		t.Fatalf("exec args changed for app-server backend: %#v", execArgs)
	}
	if !reflect.DeepEqual(appArgs, []string{"--enable", "network_proxy", "-c", "model=gpt-5-codex", "-c", "model_reasoning_effort=high"}) {
		t.Fatalf("app args = %#v", appArgs)
	}

	execArgs, appArgs = applyCodexModelArgs(backendExecJSON, []string{"--permission-mode", "ask"}, []string{"--enable", "network_proxy"}, "gpt-5-codex", "low")
	if !reflect.DeepEqual(execArgs, []string{"--permission-mode", "ask", "--model", "gpt-5-codex", "-c", "model_reasoning_effort=low"}) {
		t.Fatalf("exec args = %#v", execArgs)
	}
	if !reflect.DeepEqual(appArgs, []string{"--enable", "network_proxy"}) {
		t.Fatalf("app args changed for exec-json backend: %#v", appArgs)
	}
}

func TestApplyCodexRuntimeArgsIncludesAccessPolicy(t *testing.T) {
	execArgs, appArgs := applyCodexRuntimeArgs(backendAppServer, nil, []string{"--enable", "network_proxy"}, codexRuntimeOptions{
		model:           "gpt-5-codex",
		reasoningEffort: "high",
		serviceTier:     "fast",
		approvalPolicy:  "never",
		sandboxMode:     "danger-full-access",
	})
	if len(execArgs) != 0 {
		t.Fatalf("exec args changed for app-server backend: %#v", execArgs)
	}
	wantApp := []string{
		"--enable", "network_proxy",
		"-c", "model=gpt-5-codex",
		"-c", "model_reasoning_effort=high",
		"-c", "service_tier=fast",
		"-c", "approval_policy=never",
		"-c", "sandbox_mode=danger-full-access",
	}
	if !reflect.DeepEqual(appArgs, wantApp) {
		t.Fatalf("app args = %#v, want %#v", appArgs, wantApp)
	}

	execArgs, appArgs = applyCodexRuntimeArgs(backendExecJSON, nil, []string{"--ignored"}, codexRuntimeOptions{
		model:           "gpt-5-codex",
		reasoningEffort: "low",
		serviceTier:     "flex",
		approvalPolicy:  "on-failure",
		sandboxMode:     "workspace-write",
	})
	wantExec := []string{"--model", "gpt-5-codex", "-c", "model_reasoning_effort=low", "-c", "service_tier=flex", "-c", "approval_policy=on-failure", "--sandbox", "workspace-write"}
	if !reflect.DeepEqual(execArgs, wantExec) {
		t.Fatalf("exec args = %#v, want %#v", execArgs, wantExec)
	}
	if !reflect.DeepEqual(appArgs, []string{"--ignored"}) {
		t.Fatalf("app args changed for exec-json backend: %#v", appArgs)
	}
}

func TestRunCodexCapturesThreadIDAndSupportsResumeArgs(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "argv.log")
	fakeCodex := filepath.Join(dir, "codex")
	script := "#!/bin/sh\nprintf '%s\\n' \"$*\" >> " + shellQuote(logPath) + "\nprintf '%s\\n' '{\"type\":\"thread.started\",\"thread_id\":\"thread_1\"}'\n"
	if err := os.WriteFile(fakeCodex, []byte(script), 0o700); err != nil {
		t.Fatalf("write fake codex: %v", err)
	}

	sessionID := acp.SessionId("sess_test")
	a := &agent{codexCommand: fakeCodex, active: map[acp.SessionId]*exec.Cmd{}, cancelled: map[acp.SessionId]bool{}}

	first, err := a.runCodex(context.Background(), sessionID, dir, codexArgs(nil, dir, "", "hi"))
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	if first.threadID != "thread_1" {
		t.Fatalf("first threadID = %q", first.threadID)
	}

	second, err := a.runCodex(context.Background(), sessionID, dir, codexArgs(nil, dir, first.threadID, "again"))
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if second.threadID != "thread_1" {
		t.Fatalf("second threadID = %q", second.threadID)
	}

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read argv log: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	want := []string{
		"exec --json --skip-git-repo-check -C " + dir + " hi",
		"exec resume --json --skip-git-repo-check thread_1 again",
	}
	if !reflect.DeepEqual(lines, want) {
		t.Fatalf("argv log = %#v, want %#v", lines, want)
	}
}

func TestAppServerSessionStreamsDeltasAndSuppressesCompletedDuplicate(t *testing.T) {
	dir := t.TempDir()
	fakeCodex := writeAppServerHelper(t, dir)
	peer := &fakePeer{}

	session, err := startAppServerSession(context.Background(), fakeCodex, []string{"--fake-flag"}, codexRuntimeOptions{}, dir, acp.SessionId("sess_test"), peer)
	if err != nil {
		t.Fatalf("start app-server session: %v", err)
	}
	defer session.close()

	stop, err := session.prompt(context.Background(), "say hello")
	if err != nil {
		t.Fatalf("prompt: %v", err)
	}
	if stop != acp.StopReasonEndTurn {
		t.Fatalf("stop = %q", stop)
	}
	if got, want := peer.messages, []string{"he", "llo"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("messages = %#v, want %#v", got, want)
	}
	if got, want := peer.thoughts, []string{"thinking"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("thoughts = %#v, want %#v", got, want)
	}
}

func TestAppServerThreadStartIncludesModelAndAccessOptions(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "params.log")
	fakeCodex := writeAppServerHelper(t, dir)
	t.Setenv("FAKE_APP_SERVER_LOG", logPath)
	t.Setenv("FAKE_APP_SERVER_LOG_PARAMS", "1")

	session, err := startAppServerSession(context.Background(), fakeCodex, nil, codexRuntimeOptions{
		model:           "gpt-5-codex",
		reasoningEffort: "high",
		approvalPolicy:  "never",
		sandboxMode:     "danger-full-access",
	}, dir, acp.SessionId("sess_test"), &fakePeer{})
	if err != nil {
		t.Fatalf("start app-server session: %v", err)
	}
	defer session.close()

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	var threadStart map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		method, raw, ok := strings.Cut(line, " ")
		if !ok || method != "thread/start" {
			continue
		}
		if err := json.Unmarshal([]byte(raw), &threadStart); err != nil {
			t.Fatalf("decode thread/start params: %v", err)
		}
	}
	if threadStart == nil {
		t.Fatalf("thread/start params not logged: %q", data)
	}
	if threadStart["model"] != "gpt-5-codex" || threadStart["approvalPolicy"] != "never" || threadStart["sandbox"] != "danger-full-access" {
		t.Fatalf("thread/start params = %#v", threadStart)
	}
	config, ok := threadStart["config"].(map[string]any)
	if !ok || config["model_reasoning_effort"] != "high" {
		t.Fatalf("thread/start config = %#v", threadStart["config"])
	}
}

func TestAppServerCompletedItemFallback(t *testing.T) {
	peer := &fakePeer{}
	session := &appServerSession{sessionID: "sess_test", conn: peer}

	session.handleNotification("item/completed", []byte(`{"item":{"type":"agentMessage","id":"item_1","text":"fallback"}}`))
	if got, want := peer.messages, []string{"fallback"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("messages = %#v, want %#v", got, want)
	}

	peer = &fakePeer{}
	session = &appServerSession{sessionID: "sess_test", conn: peer, seenMessageDelta: map[string]bool{"item_1": true}}
	session.handleNotification("item/completed", []byte(`{"item":{"type":"agentMessage","id":"item_1","text":"duplicate"}}`))
	if len(peer.messages) != 0 {
		t.Fatalf("messages = %#v, want no duplicate", peer.messages)
	}
}

func TestAppServerCancelSendsInterrupt(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "methods.log")
	fakeCodex := writeAppServerHelper(t, dir)
	t.Setenv("FAKE_APP_SERVER_SCENARIO", "wait")
	t.Setenv("FAKE_APP_SERVER_LOG", logPath)
	peer := &fakePeer{}

	session, err := startAppServerSession(context.Background(), fakeCodex, nil, codexRuntimeOptions{}, dir, acp.SessionId("sess_test"), peer)
	if err != nil {
		t.Fatalf("start app-server session: %v", err)
	}
	defer session.close()

	result := make(chan appTurnResult, 1)
	go func() {
		stop, err := session.prompt(context.Background(), "wait")
		result <- appTurnResult{stop: stop, err: err}
	}()
	waitForActiveTurn(t, session)
	if err := session.cancel(context.Background()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	got := <-result
	if got.err != nil {
		t.Fatalf("prompt err = %v", got.err)
	}
	if got.stop != acp.StopReasonCancelled {
		t.Fatalf("stop = %q", got.stop)
	}
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	if !strings.Contains(string(data), "turn/interrupt") {
		t.Fatalf("methods log = %q, want turn/interrupt", data)
	}
}

func TestAppServerCommandApprovalMapsToACPPermission(t *testing.T) {
	peer := &fakePeer{permissionResponse: selectedPermission("acceptForSession")}
	session := &appServerSession{sessionID: "sess_test", conn: peer}

	result, err := session.handleCommandApproval([]byte(`{"itemId":"cmd_1","command":"go test ./...","availableDecisions":["accept","acceptForSession","decline"]}`))
	if err != nil {
		t.Fatalf("approval: %v", err)
	}
	payload, ok := result.(map[string]any)
	if !ok || payload["decision"] != "acceptForSession" {
		t.Fatalf("result = %#v", result)
	}
	if len(peer.permissionRequests) != 1 {
		t.Fatalf("permission requests = %#v", peer.permissionRequests)
	}
	req := peer.permissionRequests[0]
	if req.ToolCall.ToolCallId != "cmd_1" {
		t.Fatalf("tool call id = %q", req.ToolCall.ToolCallId)
	}
	if got := len(req.Options); got != 3 {
		t.Fatalf("options len = %d", got)
	}
}

func TestAppServerRequestUserInputMapsToACPQuestion(t *testing.T) {
	peer := &fakePeer{permissionResponse: selectedQuestionPermission([]map[string]any{
		{"id": "hitl_status", "answer": "已触发"},
	})}
	session := &appServerSession{sessionID: "sess_test", conn: peer}

	result, err := session.handleServerRequest("item/tool/requestUserInput", []byte(`{
		"itemId":"tool_1",
		"threadId":"thread_1",
		"turnId":"turn_1",
		"questions":[{
			"id":"hitl_status",
			"header":"HITL 状态",
			"question":"Codex HITL 是否已经触发？",
			"options":[
				{"label":"已触发","description":"Codex HITL 已触发"},
				{"label":"未触发","description":"Codex HITL 未触发"}
			]
		}]
	}`))
	if err != nil {
		t.Fatalf("request user input: %v", err)
	}

	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("result = %#v", result)
	}
	answers, ok := payload["answers"].(map[string]any)
	if !ok {
		t.Fatalf("answers = %#v", payload["answers"])
	}
	answer, ok := answers["hitl_status"].(map[string]any)
	if !ok {
		t.Fatalf("hitl_status answer = %#v", answers["hitl_status"])
	}
	if got, want := answer["answers"], []string{"已触发"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("answer values = %#v, want %#v", got, want)
	}

	if len(peer.permissionRequests) != 1 {
		t.Fatalf("permission requests = %#v", peer.permissionRequests)
	}
	req := peer.permissionRequests[0]
	if req.ToolCall.ToolCallId != "tool_1" {
		t.Fatalf("tool call id = %q", req.ToolCall.ToolCallId)
	}
	raw, ok := req.ToolCall.RawInput.(map[string]any)
	if !ok {
		t.Fatalf("raw input = %#v", req.ToolCall.RawInput)
	}
	if raw["mode"] != "question" {
		t.Fatalf("mode = %#v", raw["mode"])
	}
	questions, ok := raw["questions"].([]map[string]any)
	if !ok || len(questions) != 1 {
		t.Fatalf("questions = %#v", raw["questions"])
	}
	if questions[0]["question"] != "Codex HITL 是否已经触发？" {
		t.Fatalf("question = %#v", questions[0])
	}
}

func TestAppServerMcpElicitationMapsToACPQuestion(t *testing.T) {
	peer := &fakePeer{permissionResponse: selectedQuestionPermission([]map[string]any{
		{"id": "choice", "answer": "red"},
		{"id": "note", "answer": "继续"},
	})}
	session := &appServerSession{sessionID: "sess_test", conn: peer}

	result, err := session.handleServerRequest("mcpServer/elicitation/request", []byte(`{
		"serverName":"demo",
		"threadId":"thread_1",
		"turnId":"turn_1",
		"mode":"form",
		"message":"请选择后继续",
		"requestedSchema":{
			"type":"object",
			"required":["choice"],
			"properties":{
				"choice":{"type":"string","title":"颜色","description":"选择颜色","enum":["red","blue"]},
				"note":{"type":"string","title":"备注","description":"补充说明"}
			}
		}
	}`))
	if err != nil {
		t.Fatalf("elicitation: %v", err)
	}

	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("result = %#v", result)
	}
	if payload["action"] != "accept" {
		t.Fatalf("action = %#v", payload["action"])
	}
	content, ok := payload["content"].(map[string]any)
	if !ok {
		t.Fatalf("content = %#v", payload["content"])
	}
	if content["choice"] != "red" || content["note"] != "继续" {
		t.Fatalf("content = %#v", content)
	}

	if len(peer.permissionRequests) != 1 {
		t.Fatalf("permission requests = %#v", peer.permissionRequests)
	}
	raw, ok := peer.permissionRequests[0].ToolCall.RawInput.(map[string]any)
	if !ok || raw["mode"] != "question" {
		t.Fatalf("raw input = %#v", peer.permissionRequests[0].ToolCall.RawInput)
	}
	questions, ok := raw["questions"].([]map[string]any)
	if !ok || len(questions) != 2 {
		t.Fatalf("questions = %#v", raw["questions"])
	}
	if questions[0]["id"] != "choice" || questions[1]["id"] != "note" {
		t.Fatalf("questions = %#v", questions)
	}
}

func TestAppServerPermissionsApprovalRejectsToEmptyProfile(t *testing.T) {
	peer := &fakePeer{permissionResponse: acp.RequestPermissionResponse{Outcome: acp.RequestPermissionOutcome{Cancelled: &acp.RequestPermissionOutcomeCancelled{}}}}
	session := &appServerSession{sessionID: "sess_test", conn: peer}

	result, err := session.handlePermissionsApproval([]byte(`{"itemId":"perm_1","reason":"needs network","permissions":{"network":{"enabled":true},"fileSystem":null}}`))
	if err != nil {
		t.Fatalf("approval: %v", err)
	}
	payload, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("result = %#v", result)
	}
	if payload["scope"] != "turn" || payload["strictAutoReview"] != true {
		t.Fatalf("payload = %#v", payload)
	}
	permissions, _ := payload["permissions"].(map[string]any)
	if len(permissions) != 0 {
		t.Fatalf("permissions = %#v, want empty", permissions)
	}
}

func TestAppServerCoreItemEventsMapToACPUpdates(t *testing.T) {
	peer := &fakePeer{}
	session := &appServerSession{sessionID: "sess_test", conn: peer}

	session.handleNotification("item/started", []byte(`{"item":{"type":"commandExecution","id":"cmd_1","status":"inProgress","command":"bash -lc \"go test ./...\"","cwd":"/tmp/work"}}`))
	session.handleNotification("item/commandExecution/outputDelta", []byte(`{"itemId":"cmd_1","delta":"ok\n"}`))
	session.handleNotification("item/completed", []byte(`{"item":{"type":"commandExecution","id":"cmd_1","status":"completed","command":"bash -lc \"go test ./...\"","cwd":"/tmp/work","exitCode":0}}`))
	session.handleNotification("item/started", []byte(`{"item":{"type":"mcpToolCall","id":"mcp_1","status":"inProgress","server":"demo","tool":"lookup","arguments":{"q":"x"}}}`))
	session.handleNotification("item/mcpToolCall/progress", []byte(`{"itemId":"mcp_1","message":"loading"}`))
	session.handleNotification("item/completed", []byte(`{"item":{"type":"mcpToolCall","id":"mcp_1","status":"completed","server":"demo","tool":"lookup","result":{"ok":true},"error":null}}`))
	session.handleNotification("turn/plan/updated", []byte(`{"plan":[{"step":"Inspect","status":"completed"},{"step":"Patch","status":"inProgress"}]}`))
	session.handleNotification("thread/tokenUsage/updated", []byte(`{"tokenUsage":{"last":{"totalTokens":42},"modelContextWindow":1000}}`))

	if got := len(peer.toolStarts); got != 2 {
		t.Fatalf("tool starts = %d, want 2", got)
	}
	if peer.toolStarts[0].ToolCallId != "cmd_1" || peer.toolStarts[0].Kind != acp.ToolKindExecute {
		t.Fatalf("command tool start = %#v", peer.toolStarts[0])
	}
	if peer.toolStarts[0].Title != `go test ./...` {
		t.Fatalf("command title = %q", peer.toolStarts[0].Title)
	}
	if got := len(peer.toolUpdates); got != 2 {
		t.Fatalf("tool updates = %d, want 2", got)
	}
	commandOutput, ok := peer.toolUpdates[0].RawOutput.(map[string]any)
	if !ok {
		t.Fatalf("command raw output = %#v", peer.toolUpdates[0].RawOutput)
	}
	if commandOutput["formatted_output"] != "ok\n" || commandOutput["exitCode"] != 0 {
		t.Fatalf("command output = %#v", commandOutput)
	}
	mcpOutput, ok := peer.toolUpdates[1].RawOutput.(map[string]any)
	if !ok {
		t.Fatalf("mcp raw output = %#v", peer.toolUpdates[1].RawOutput)
	}
	if progress, ok := mcpOutput["progress"].([]string); !ok || !reflect.DeepEqual(progress, []string{"loading"}) {
		t.Fatalf("mcp progress = %#v", mcpOutput["progress"])
	}
	if len(peer.plans) != 1 || len(peer.plans[0].Entries) != 2 {
		t.Fatalf("plans = %#v", peer.plans)
	}
	if peer.plans[0].Entries[1].Status != acp.PlanEntryStatusInProgress {
		t.Fatalf("plan status = %#v", peer.plans[0].Entries)
	}
	if len(peer.usages) != 1 || peer.usages[0].Used != 42 || peer.usages[0].Size != 1000 {
		t.Fatalf("usage = %#v", peer.usages)
	}
}

func TestAppServerImageViewDoesNotEmitDuplicateCompletion(t *testing.T) {
	peer := &fakePeer{}
	session := &appServerSession{sessionID: "sess_test", conn: peer}

	session.handleNotification("item/started", []byte(`{"item":{"type":"imageView","id":"img_1","path":"/tmp/a.png"}}`))
	session.handleNotification("item/completed", []byte(`{"item":{"type":"imageView","id":"img_1","path":"/tmp/a.png"}}`))

	if got := len(peer.toolStarts); got != 1 {
		t.Fatalf("tool starts = %d, want 1", got)
	}
	if peer.toolStarts[0].Status != acp.ToolCallStatusCompleted {
		t.Fatalf("image view status = %q", peer.toolStarts[0].Status)
	}
	if got := len(peer.toolUpdates); got != 0 {
		t.Fatalf("tool updates = %d, want no duplicate completion", got)
	}
}

func TestAppServerFileChangeMapsToWebclientFileEditSummary(t *testing.T) {
	peer := &fakePeer{}
	session := &appServerSession{sessionID: "sess_test", conn: peer}

	session.handleNotification("item/started", []byte(`{"item":{"type":"fileChange","id":"edit_1","status":"inProgress","changes":[{"path":"/tmp/app.go","diff":"@@\n-old\n+new\n+extra"}]}}`))
	session.handleNotification("item/completed", []byte(`{"item":{"type":"fileChange","id":"edit_1","status":"completed","changes":[{"path":"/tmp/app.go","diff":"@@\n-old\n+new\n+extra"}]}}`))

	if len(peer.toolStarts) != 1 {
		t.Fatalf("tool starts = %#v", peer.toolStarts)
	}
	if peer.toolStarts[0].Title != "file_edit" || peer.toolStarts[0].Kind != "" {
		t.Fatalf("file tool start = %#v", peer.toolStarts[0])
	}
	if len(peer.toolUpdates) != 1 {
		t.Fatalf("tool updates = %#v", peer.toolUpdates)
	}
	output, ok := peer.toolUpdates[0].RawOutput.(map[string]any)
	if !ok {
		t.Fatalf("raw output = %#v", peer.toolUpdates[0].RawOutput)
	}
	if output["filePath"] != "/tmp/app.go" {
		t.Fatalf("filePath = %#v", output["filePath"])
	}
	stats, ok := output["lineStats"].(map[string]any)
	if !ok {
		t.Fatalf("lineStats = %#v", output["lineStats"])
	}
	if stats["addedLines"] != 2 || stats["deletedLines"] != 1 || stats["editedLines"] != 1 {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestAppServerCommandActionsMapToReadAndSearchTools(t *testing.T) {
	peer := &fakePeer{}
	session := &appServerSession{sessionID: "sess_test", conn: peer}

	session.handleNotification("item/started", []byte(`{"item":{"type":"commandExecution","id":"read_1","status":"inProgress","cwd":"/tmp/work","commandActions":[{"type":"read","path":"/tmp/work/main.go"}]}}`))
	session.handleNotification("item/started", []byte(`{"item":{"type":"commandExecution","id":"search_1","status":"inProgress","cwd":"/tmp/work","commandActions":[{"type":"search","query":"TODO","path":"/tmp/work"}]}}`))

	if len(peer.toolStarts) != 2 {
		t.Fatalf("tool starts = %#v", peer.toolStarts)
	}
	if peer.toolStarts[0].Kind != acp.ToolKindRead || peer.toolStarts[0].Title != "Read file '/tmp/work/main.go'" {
		t.Fatalf("read start = %#v", peer.toolStarts[0])
	}
	if len(peer.toolStarts[0].Locations) != 1 || peer.toolStarts[0].Locations[0].Path != "/tmp/work/main.go" {
		t.Fatalf("read locations = %#v", peer.toolStarts[0].Locations)
	}
	if peer.toolStarts[1].Kind != acp.ToolKindSearch || peer.toolStarts[1].Title != "Search for 'TODO' in /tmp/work" {
		t.Fatalf("search start = %#v", peer.toolStarts[1])
	}
}

type fakePeer struct {
	mu                 sync.Mutex
	messages           []string
	thoughts           []string
	toolStarts         []acp.SessionUpdateToolCall
	toolUpdates        []acp.SessionToolCallUpdate
	plans              []acp.SessionUpdatePlan
	usages             []acp.SessionUsageUpdate
	permissionRequests []acp.RequestPermissionRequest
	permissionResponse acp.RequestPermissionResponse
}

func (f *fakePeer) SessionUpdate(_ context.Context, params acp.SessionNotification) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if params.Update.AgentMessageChunk != nil {
		f.messages = append(f.messages, contentText(params.Update.AgentMessageChunk.Content))
	}
	if params.Update.AgentThoughtChunk != nil {
		f.thoughts = append(f.thoughts, contentText(params.Update.AgentThoughtChunk.Content))
	}
	if params.Update.ToolCall != nil {
		f.toolStarts = append(f.toolStarts, *params.Update.ToolCall)
	}
	if params.Update.ToolCallUpdate != nil {
		f.toolUpdates = append(f.toolUpdates, *params.Update.ToolCallUpdate)
	}
	if params.Update.Plan != nil {
		f.plans = append(f.plans, *params.Update.Plan)
	}
	if params.Update.UsageUpdate != nil {
		f.usages = append(f.usages, *params.Update.UsageUpdate)
	}
	return nil
}

func (f *fakePeer) RequestPermission(_ context.Context, params acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.permissionRequests = append(f.permissionRequests, params)
	if f.permissionResponse.Outcome.Selected == nil && f.permissionResponse.Outcome.Cancelled == nil {
		return selectedPermission("accept"), nil
	}
	return f.permissionResponse, nil
}

func contentText(block acp.ContentBlock) string {
	if block.Text == nil {
		return ""
	}
	return block.Text.Text
}

func selectedPermission(id string) acp.RequestPermissionResponse {
	return acp.RequestPermissionResponse{Outcome: acp.RequestPermissionOutcome{
		Selected: &acp.RequestPermissionOutcomeSelected{OptionId: acp.PermissionOptionId(id)},
	}}
}

func selectedQuestionPermission(answers []map[string]any) acp.RequestPermissionResponse {
	return acp.RequestPermissionResponse{Outcome: acp.RequestPermissionOutcome{
		Selected: &acp.RequestPermissionOutcomeSelected{
			OptionId: "submitted",
			Meta: map[string]any{
				"answers": answers,
			},
		},
	}}
}

func writeAppServerHelper(t *testing.T, dir string) string {
	t.Helper()
	fakeCodex := filepath.Join(dir, "codex")
	script := "#!/bin/sh\nGO_WANT_HELPER_PROCESS=appserver " + shellQuote(os.Args[0]) + " -test.run=TestHelperProcess -- \"$@\"\n"
	if err := os.WriteFile(fakeCodex, []byte(script), 0o700); err != nil {
		t.Fatalf("write fake codex: %v", err)
	}
	return fakeCodex
}

func waitForActiveTurn(t *testing.T, session *appServerSession) {
	t.Helper()
	for i := 0; i < 200; i++ {
		session.mu.Lock()
		active := session.activeTurnID != ""
		session.mu.Unlock()
		if active {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for active turn")
}

func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "appserver" {
		return
	}
	runFakeAppServer()
	os.Exit(0)
}

func runFakeAppServer() {
	scanner := bufio.NewScanner(os.Stdin)
	writer := bufio.NewWriter(os.Stdout)
	defer writer.Flush()
	scenario := os.Getenv("FAKE_APP_SERVER_SCENARIO")
	for scanner.Scan() {
		var msg jsonRPCMessage
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			continue
		}
		if logPath := os.Getenv("FAKE_APP_SERVER_LOG"); logPath != "" {
			f, _ := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
			if f != nil {
				if os.Getenv("FAKE_APP_SERVER_LOG_PARAMS") != "" {
					_, _ = f.WriteString(msg.Method + " " + string(msg.Params) + "\n")
				} else {
					_, _ = f.WriteString(msg.Method + "\n")
				}
				_ = f.Close()
			}
		}
		switch msg.Method {
		case "initialize":
			writeRPC(writer, map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(msg.ID), "result": map[string]any{"userAgent": "fake", "codexHome": "/tmp", "platformFamily": "unix", "platformOs": "macos"}})
		case "thread/start":
			writeRPC(writer, map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(msg.ID), "result": map[string]any{"thread": map[string]any{"id": "thread_1"}}})
		case "turn/start":
			writeRPC(writer, map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(msg.ID), "result": map[string]any{"turn": map[string]any{"id": "turn_1", "status": "inProgress"}}})
			writeRPC(writer, map[string]any{"jsonrpc": "2.0", "method": "turn/started", "params": map[string]any{"threadId": "thread_1", "turn": map[string]any{"id": "turn_1", "status": "inProgress"}}})
			if scenario == "wait" {
				continue
			}
			writeRPC(writer, map[string]any{"jsonrpc": "2.0", "method": "item/agentMessage/delta", "params": map[string]any{"threadId": "thread_1", "turnId": "turn_1", "itemId": "item_1", "delta": "he"}})
			writeRPC(writer, map[string]any{"jsonrpc": "2.0", "method": "item/reasoning/textDelta", "params": map[string]any{"threadId": "thread_1", "turnId": "turn_1", "itemId": "reason_1", "delta": "thinking"}})
			writeRPC(writer, map[string]any{"jsonrpc": "2.0", "method": "item/agentMessage/delta", "params": map[string]any{"threadId": "thread_1", "turnId": "turn_1", "itemId": "item_1", "delta": "llo"}})
			writeRPC(writer, map[string]any{"jsonrpc": "2.0", "method": "item/completed", "params": map[string]any{"threadId": "thread_1", "turnId": "turn_1", "item": map[string]any{"type": "agentMessage", "id": "item_1", "text": "hello"}}})
			writeRPC(writer, map[string]any{"jsonrpc": "2.0", "method": "turn/completed", "params": map[string]any{"threadId": "thread_1", "turn": map[string]any{"id": "turn_1", "status": "completed"}}})
		case "turn/interrupt":
			writeRPC(writer, map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(msg.ID), "result": map[string]any{}})
			writeRPC(writer, map[string]any{"jsonrpc": "2.0", "method": "turn/completed", "params": map[string]any{"threadId": "thread_1", "turn": map[string]any{"id": "turn_1", "status": "interrupted"}}})
		}
	}
}

func writeRPC(w *bufio.Writer, payload map[string]any) {
	data, _ := json.Marshal(payload)
	_, _ = w.Write(data)
	_ = w.WriteByte('\n')
	_ = w.Flush()
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}
