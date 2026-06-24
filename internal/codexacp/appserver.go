package codexacp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/coder/acp-go-sdk"
)

type acpPeer interface {
	SessionUpdate(context.Context, acp.SessionNotification) error
	RequestPermission(context.Context, acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error)
}

type appServerSession struct {
	sessionID acp.SessionId
	cwd       string
	threadID  string
	options   codexRuntimeOptions
	cmd       *exec.Cmd
	rpc       *jsonRPCClient
	conn      acpPeer

	mu                 sync.Mutex
	activeTurnID       string
	turnDone           chan appTurnResult
	seenMessageDelta   map[string]bool
	seenReasoningDelta map[string]bool
	commandOutputs     map[string]*strings.Builder
	mcpProgress        map[string][]string
	completedOnStart   map[string]bool
	closed             bool
}

type appTurnResult struct {
	stop acp.StopReason
	err  error
}

func startAppServerSession(ctx context.Context, codexCommand string, appArgs []string, options codexRuntimeOptions, cwd string, sessionID acp.SessionId, conn acpPeer) (*appServerSession, error) {
	args := []string{"app-server", "--listen", "stdio://"}
	args = append(args, appArgs...)

	cmd := exec.CommandContext(context.Background(), codexCommand, args...)
	cmd.Dir = cwd
	cmd.Env = os.Environ()
	cmd.Stderr = os.Stderr
	prepareChildProcess(cmd)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	session := &appServerSession{
		sessionID: sessionID,
		cwd:       cwd,
		options:   options,
		cmd:       cmd,
		conn:      conn,
	}
	rpc := newJSONRPCClient(stdin, stdout)
	rpc.onNotification = session.handleNotification
	rpc.onRequest = session.handleServerRequest
	session.rpc = rpc

	if err := cmd.Start(); err != nil {
		return nil, err
	}
	rpc.start()

	go func() {
		_ = cmd.Wait()
		rpc.closeWithError(io.EOF)
	}()

	if err := session.initialize(ctx); err != nil {
		session.close()
		return nil, err
	}
	if err := session.startThread(ctx); err != nil {
		session.close()
		return nil, err
	}
	return session, nil
}

func (s *appServerSession) initialize(ctx context.Context) error {
	var response struct {
		UserAgent string `json:"userAgent"`
	}
	return s.rpc.call(ctx, "initialize", map[string]any{
		"clientInfo": map[string]any{
			"name":    "proxy-acp-codex",
			"title":   "proxy-acp-codex",
			"version": "0.1.0",
		},
		"capabilities": map[string]any{
			"experimentalApi":    true,
			"requestAttestation": false,
		},
	}, &response)
}

func (s *appServerSession) startThread(ctx context.Context) error {
	var response struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	params := map[string]any{
		"cwd":                    s.cwd,
		"experimentalRawEvents":  false,
		"persistExtendedHistory": false,
		"ephemeral":              true,
	}
	if model := strings.TrimSpace(s.options.model); model != "" {
		params["model"] = model
	}
	config := map[string]any{}
	if effort := normalizeCodexReasoningEffort(s.options.reasoningEffort); effort != "" {
		config["model_reasoning_effort"] = effort
	}
	if serviceTier := normalizeCodexServiceTier(s.options.serviceTier); serviceTier != "" {
		config["service_tier"] = serviceTier
	}
	if len(config) > 0 {
		params["config"] = config
	}
	if approvalPolicy := normalizeCodexApprovalPolicy(s.options.approvalPolicy); approvalPolicy != "" {
		params["approvalPolicy"] = approvalPolicy
	}
	if sandboxMode := normalizeCodexSandboxMode(s.options.sandboxMode); sandboxMode != "" {
		params["sandbox"] = sandboxMode
	}
	if err := s.rpc.call(ctx, "thread/start", params, &response); err != nil {
		return err
	}
	if strings.TrimSpace(response.Thread.ID) == "" {
		return fmt.Errorf("codex app-server thread/start returned empty thread id")
	}
	s.threadID = response.Thread.ID
	return nil
}

func (s *appServerSession) prompt(ctx context.Context, text string) (acp.StopReason, error) {
	done := make(chan appTurnResult, 1)
	s.mu.Lock()
	if s.turnDone != nil {
		s.mu.Unlock()
		return "", fmt.Errorf("codex app-server turn already in progress")
	}
	s.activeTurnID = ""
	s.turnDone = done
	s.seenMessageDelta = map[string]bool{}
	s.seenReasoningDelta = map[string]bool{}
	s.mu.Unlock()

	var response struct {
		Turn appServerTurn `json:"turn"`
	}
	err := s.rpc.call(ctx, "turn/start", map[string]any{
		"threadId": s.threadID,
		"cwd":      s.cwd,
		"input": []map[string]any{{
			"type":          "text",
			"text":          text,
			"text_elements": []any{},
		}},
	}, &response)
	if err != nil {
		s.clearTurn(done)
		return "", err
	}
	if response.Turn.ID != "" {
		s.setActiveTurnID(response.Turn.ID)
	}
	if result, ok := resultFromTurn(response.Turn); ok {
		s.clearTurn(done)
		return result.stop, result.err
	}

	select {
	case result := <-done:
		return result.stop, result.err
	case <-ctx.Done():
		_ = s.cancel(context.Background())
		return acp.StopReasonCancelled, nil
	}
}

func (s *appServerSession) cancel(ctx context.Context) error {
	s.mu.Lock()
	threadID := s.threadID
	turnID := s.activeTurnID
	done := s.turnDone
	s.mu.Unlock()
	if threadID == "" || turnID == "" {
		if done != nil {
			s.finishTurn(turnID, appTurnResult{stop: acp.StopReasonCancelled})
		}
		return nil
	}
	err := s.rpc.call(ctx, "turn/interrupt", map[string]any{
		"threadId": threadID,
		"turnId":   turnID,
	}, nil)
	s.finishTurn(turnID, appTurnResult{stop: acp.StopReasonCancelled})
	return err
}

func (s *appServerSession) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	cmd := s.cmd
	rpc := s.rpc
	s.mu.Unlock()
	if rpc != nil {
		rpc.closeWithError(io.EOF)
	}
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}

func (s *appServerSession) handleNotification(method string, params json.RawMessage) {
	switch method {
	case "thread/started":
		var payload struct {
			Thread struct {
				ID string `json:"id"`
			} `json:"thread"`
		}
		if json.Unmarshal(params, &payload) == nil && payload.Thread.ID != "" && s.threadID == "" {
			s.threadID = payload.Thread.ID
		}
	case "turn/started":
		var payload struct {
			Turn appServerTurn `json:"turn"`
		}
		if json.Unmarshal(params, &payload) == nil && payload.Turn.ID != "" {
			s.setActiveTurnID(payload.Turn.ID)
		}
	case "item/agentMessage/delta":
		var payload struct {
			ItemID string `json:"itemId"`
			Delta  string `json:"delta"`
		}
		if json.Unmarshal(params, &payload) == nil && payload.Delta != "" {
			s.markMessageDelta(payload.ItemID)
			s.sendUpdate(acp.UpdateAgentMessageText(payload.Delta))
		}
	case "item/reasoning/textDelta", "item/reasoning/summaryTextDelta":
		var payload struct {
			ItemID string `json:"itemId"`
			Delta  string `json:"delta"`
		}
		if json.Unmarshal(params, &payload) == nil && payload.Delta != "" {
			s.markReasoningDelta(payload.ItemID)
			s.sendUpdate(acp.UpdateAgentThoughtText(payload.Delta))
		}
	case "item/started":
		var payload struct {
			Item json.RawMessage `json:"item"`
		}
		if json.Unmarshal(params, &payload) == nil {
			if item, raw, ok := decodeAppServerItem(payload.Item); ok {
				s.handleStartedItem(item, raw)
			}
		}
	case "item/commandExecution/outputDelta":
		var payload struct {
			ItemID string `json:"itemId"`
			Delta  string `json:"delta"`
		}
		if json.Unmarshal(params, &payload) == nil && payload.ItemID != "" {
			s.appendCommandOutput(payload.ItemID, payload.Delta)
		}
	case "item/commandExecution/terminalInteraction":
		var payload struct {
			ItemID string `json:"itemId"`
			Stdin  string `json:"stdin"`
		}
		if json.Unmarshal(params, &payload) == nil && payload.ItemID != "" && payload.Stdin != "" {
			s.appendCommandOutput(payload.ItemID, "\n"+payload.Stdin+"\n")
		}
	case "item/mcpToolCall/progress":
		var payload struct {
			ItemID  string `json:"itemId"`
			Message string `json:"message"`
		}
		if json.Unmarshal(params, &payload) == nil && payload.ItemID != "" && strings.TrimSpace(payload.Message) != "" {
			s.appendMCPProgress(payload.ItemID, payload.Message)
		}
	case "turn/plan/updated":
		s.handlePlanUpdated(params)
	case "thread/tokenUsage/updated":
		s.handleTokenUsageUpdated(params)
	case "item/completed":
		var payload struct {
			Item json.RawMessage `json:"item"`
		}
		if json.Unmarshal(params, &payload) == nil {
			if item, raw, ok := decodeAppServerItem(payload.Item); ok {
				s.handleCompletedItem(item, raw)
			}
		}
	case "turn/completed":
		var payload struct {
			Turn appServerTurn `json:"turn"`
		}
		if json.Unmarshal(params, &payload) == nil {
			result, ok := resultFromTurn(payload.Turn)
			if !ok {
				result = appTurnResult{stop: acp.StopReasonEndTurn}
			}
			s.finishTurn(payload.Turn.ID, result)
		}
	case "error":
		var payload struct {
			TurnID string `json:"turnId"`
			Error  struct {
				Message           string `json:"message"`
				AdditionalDetails string `json:"additionalDetails"`
			} `json:"error"`
		}
		if json.Unmarshal(params, &payload) == nil {
			msg := firstNonBlank(payload.Error.Message, payload.Error.AdditionalDetails, "codex app-server error")
			s.finishTurn(payload.TurnID, appTurnResult{err: errors.New(msg)})
		}
	}
}

func (s *appServerSession) handleStartedItem(item appServerItem, raw map[string]any) {
	switch item.Type {
	case "fileChange", "commandExecution", "mcpToolCall", "dynamicToolCall", "webSearch", "imageView", "imageGeneration":
		opts := []acp.ToolCallStartOpt{
			acp.WithStartKind(itemToolKind(item)),
			acp.WithStartStatus(itemToolStatus(item.Status, false)),
			acp.WithStartRawInput(itemRawInput(item, raw)),
		}
		if locations := itemLocations(item, raw); len(locations) > 0 {
			opts = append(opts, acp.WithStartLocations(locations))
		}
		if item.Type == "imageView" {
			opts = append(opts,
				acp.WithStartStatus(acp.ToolCallStatusCompleted),
				acp.WithStartRawOutput(itemRawOutput(item, raw, "")),
			)
			s.markCompletedOnStart(item.ID)
		}
		s.sendUpdate(acp.StartToolCall(acp.ToolCallId(item.ID), itemToolTitle(item), opts...))
	}
}

func (s *appServerSession) handleCompletedItem(item appServerItem, raw map[string]any) {
	switch item.Type {
	case "agentMessage":
		if item.Text != "" && !s.hasMessageDelta(item.ID) {
			s.sendUpdate(acp.UpdateAgentMessageText(item.Text))
		}
	case "reasoning":
		if !s.hasReasoningDelta(item.ID) {
			text := strings.Join(append(item.Summary, item.Content...), "\n")
			if text != "" {
				s.sendUpdate(acp.UpdateAgentThoughtText(text))
			}
		}
	case "fileChange", "commandExecution", "mcpToolCall", "dynamicToolCall", "webSearch", "imageView", "imageGeneration":
		if s.consumeCompletedOnStart(item.ID) {
			return
		}
		output := itemRawOutput(item, raw, s.commandOutput(item.ID))
		if item.Type == "mcpToolCall" {
			if out, ok := output.(map[string]any); ok {
				if progress := s.mcpProgressFor(item.ID); len(progress) > 0 {
					out["progress"] = progress
				}
			}
		}
		opts := []acp.ToolCallUpdateOpt{
			acp.WithUpdateTitle(itemToolTitle(item)),
			acp.WithUpdateStatus(itemToolStatus(item.Status, true)),
			acp.WithUpdateRawOutput(output),
		}
		if kind := itemToolKind(item); kind != "" {
			opts = append(opts, acp.WithUpdateKind(kind))
		}
		s.sendUpdate(acp.UpdateToolCall(acp.ToolCallId(item.ID), opts...))
		s.clearToolBuffers(item.ID)
	}
}

func (s *appServerSession) handlePlanUpdated(params json.RawMessage) {
	var payload struct {
		Plan []struct {
			Step     string `json:"step"`
			Content  string `json:"content"`
			Status   string `json:"status"`
			Priority string `json:"priority"`
		} `json:"plan"`
	}
	if json.Unmarshal(params, &payload) != nil || len(payload.Plan) == 0 {
		return
	}
	entries := make([]acp.PlanEntry, 0, len(payload.Plan))
	for _, item := range payload.Plan {
		content := firstNonBlank(item.Step, item.Content)
		if content == "" {
			continue
		}
		entries = append(entries, acp.PlanEntry{
			Content:  content,
			Priority: planPriority(item.Priority),
			Status:   planStatus(item.Status),
		})
	}
	if len(entries) > 0 {
		s.sendUpdate(acp.UpdatePlan(entries...))
	}
}

func (s *appServerSession) handleTokenUsageUpdated(params json.RawMessage) {
	var payload struct {
		TokenUsage struct {
			Last struct {
				TotalTokens int `json:"totalTokens"`
			} `json:"last"`
			ModelContextWindow int `json:"modelContextWindow"`
		} `json:"tokenUsage"`
	}
	if json.Unmarshal(params, &payload) != nil {
		return
	}
	if payload.TokenUsage.Last.TotalTokens <= 0 || payload.TokenUsage.ModelContextWindow <= 0 {
		return
	}
	s.sendUpdate(acp.SessionUpdate{UsageUpdate: &acp.SessionUsageUpdate{
		Used: payload.TokenUsage.Last.TotalTokens,
		Size: payload.TokenUsage.ModelContextWindow,
	}})
}

func (s *appServerSession) handleServerRequest(method string, params json.RawMessage) (any, error) {
	switch method {
	case "item/commandExecution/requestApproval":
		return s.handleCommandApproval(params)
	case "item/fileChange/requestApproval":
		return s.handleFileChangeApproval(params)
	case "item/permissions/requestApproval":
		return s.handlePermissionsApproval(params)
	case "item/tool/requestUserInput":
		return s.handleRequestUserInput(params)
	case "mcpServer/elicitation/request":
		return s.handleMCPElicitation(params)
	default:
		return nil, &jsonRPCError{Code: -32601, Message: "unsupported codex app-server request: " + method}
	}
}

func (s *appServerSession) handleCommandApproval(params json.RawMessage) (any, error) {
	payload := map[string]any{}
	if err := json.Unmarshal(params, &payload); err != nil {
		return nil, err
	}
	id := firstNonBlank(stringField(payload, "approvalId"), stringField(payload, "itemId"), "command")
	command := firstNonBlank(stringField(payload, "command"), id)
	title := "Run command: " + command
	kind := acp.ToolKindExecute
	status := acp.ToolCallStatusPending
	options := commandPermissionOptions(payload["availableDecisions"])
	response, err := s.conn.RequestPermission(context.Background(), acp.RequestPermissionRequest{
		SessionId: s.sessionID,
		ToolCall: acp.ToolCallUpdate{
			ToolCallId: acp.ToolCallId(id),
			Title:      &title,
			Kind:       &kind,
			Status:     &status,
			RawInput:   payload,
		},
		Options: options,
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{"decision": commandDecisionFromOutcome(response.Outcome)}, nil
}

func (s *appServerSession) handleFileChangeApproval(params json.RawMessage) (any, error) {
	payload := map[string]any{}
	if err := json.Unmarshal(params, &payload); err != nil {
		return nil, err
	}
	id := firstNonBlank(stringField(payload, "itemId"), "fileChange")
	title := firstNonBlank(stringField(payload, "reason"), "Apply file changes")
	kind := acp.ToolKindEdit
	status := acp.ToolCallStatusPending
	response, err := s.conn.RequestPermission(context.Background(), acp.RequestPermissionRequest{
		SessionId: s.sessionID,
		ToolCall: acp.ToolCallUpdate{
			ToolCallId: acp.ToolCallId(id),
			Title:      &title,
			Kind:       &kind,
			Status:     &status,
			RawInput:   payload,
		},
		Options: []acp.PermissionOption{
			{OptionId: "accept", Name: "Allow once", Kind: acp.PermissionOptionKindAllowOnce},
			{OptionId: "acceptForSession", Name: "Allow for session", Kind: acp.PermissionOptionKindAllowAlways},
			{OptionId: "decline", Name: "Reject", Kind: acp.PermissionOptionKindRejectOnce},
		},
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{"decision": fileDecisionFromOutcome(response.Outcome)}, nil
}

func (s *appServerSession) handlePermissionsApproval(params json.RawMessage) (any, error) {
	payload := map[string]any{}
	if err := json.Unmarshal(params, &payload); err != nil {
		return nil, err
	}
	id := firstNonBlank(stringField(payload, "itemId"), "permissions")
	title := firstNonBlank(stringField(payload, "reason"), "Permissions request")
	kind := acp.ToolKindExecute
	status := acp.ToolCallStatusPending
	response, err := s.conn.RequestPermission(context.Background(), acp.RequestPermissionRequest{
		SessionId: s.sessionID,
		ToolCall: acp.ToolCallUpdate{
			ToolCallId: acp.ToolCallId(id),
			Title:      &title,
			Kind:       &kind,
			Status:     &status,
			RawInput:   payload,
		},
		Options: []acp.PermissionOption{
			{OptionId: "approved-for-session", Name: "Allow for session", Kind: acp.PermissionOptionKindAllowAlways},
			{OptionId: "approved", Name: "Allow once", Kind: acp.PermissionOptionKindAllowOnce},
			{OptionId: "abort", Name: "Reject", Kind: acp.PermissionOptionKindRejectOnce},
		},
	})
	if err != nil {
		return nil, err
	}
	permissions, _ := payload["permissions"].(map[string]any)
	if permissions == nil {
		permissions = map[string]any{}
	}
	if response.Outcome.Selected != nil {
		switch response.Outcome.Selected.OptionId {
		case "approved-for-session":
			return map[string]any{"permissions": permissions, "scope": "session", "strictAutoReview": false}, nil
		case "approved":
			return map[string]any{"permissions": permissions, "scope": "turn", "strictAutoReview": false}, nil
		}
	}
	return map[string]any{"permissions": map[string]any{}, "scope": "turn", "strictAutoReview": true}, nil
}

func (s *appServerSession) handleRequestUserInput(params json.RawMessage) (any, error) {
	payload := map[string]any{}
	if err := json.Unmarshal(params, &payload); err != nil {
		return nil, err
	}
	id := firstNonBlank(stringField(payload, "itemId"), "question")
	title := "Answer question"
	kind := acp.ToolKindExecute
	status := acp.ToolCallStatusPending
	questions := questionMaps(payload["questions"])
	response, err := s.conn.RequestPermission(context.Background(), acp.RequestPermissionRequest{
		SessionId: s.sessionID,
		ToolCall: acp.ToolCallUpdate{
			ToolCallId: acp.ToolCallId(id),
			Title:      &title,
			Kind:       &kind,
			Status:     &status,
			RawInput: map[string]any{
				"mode":      "question",
				"questions": questions,
			},
		},
		Options: questionPermissionOptions(),
	})
	if err != nil {
		return nil, err
	}
	if response.Outcome.Cancelled != nil {
		return map[string]any{"answers": map[string]any{}}, nil
	}
	return map[string]any{"answers": appServerQuestionAnswers(response.Outcome)}, nil
}

func (s *appServerSession) handleMCPElicitation(params json.RawMessage) (any, error) {
	payload := map[string]any{}
	if err := json.Unmarshal(params, &payload); err != nil {
		return nil, err
	}
	id := firstNonBlank(stringField(payload, "itemId"), stringField(payload, "serverName"), "elicitation")
	title := firstNonBlank(stringField(payload, "message"), "MCP elicitation")
	kind := acp.ToolKindExecute
	status := acp.ToolCallStatusPending
	questions := elicitationQuestions(payload)
	response, err := s.conn.RequestPermission(context.Background(), acp.RequestPermissionRequest{
		SessionId: s.sessionID,
		ToolCall: acp.ToolCallUpdate{
			ToolCallId: acp.ToolCallId(id),
			Title:      &title,
			Kind:       &kind,
			Status:     &status,
			RawInput: map[string]any{
				"mode":      "question",
				"questions": questions,
			},
		},
		Options: questionPermissionOptions(),
	})
	if err != nil {
		return nil, err
	}
	if response.Outcome.Cancelled != nil {
		return map[string]any{"action": "decline"}, nil
	}
	return map[string]any{"action": "accept", "content": elicitationContent(response.Outcome)}, nil
}

func (s *appServerSession) sendUpdate(update acp.SessionUpdate) {
	if s.conn == nil {
		return
	}
	_ = s.conn.SessionUpdate(context.Background(), acp.SessionNotification{
		SessionId: s.sessionID,
		Update:    update,
	})
}

func (s *appServerSession) setActiveTurnID(turnID string) {
	s.mu.Lock()
	if s.turnDone != nil && s.activeTurnID == "" {
		s.activeTurnID = turnID
	}
	s.mu.Unlock()
}

func (s *appServerSession) finishTurn(turnID string, result appTurnResult) {
	s.mu.Lock()
	if s.turnDone == nil {
		s.mu.Unlock()
		return
	}
	if s.activeTurnID != "" && turnID != "" && s.activeTurnID != turnID {
		s.mu.Unlock()
		return
	}
	if s.activeTurnID == "" {
		s.activeTurnID = turnID
	}
	done := s.turnDone
	s.turnDone = nil
	s.activeTurnID = ""
	s.mu.Unlock()

	select {
	case done <- result:
	default:
	}
}

func (s *appServerSession) clearTurn(done chan appTurnResult) {
	s.mu.Lock()
	if s.turnDone == done {
		s.turnDone = nil
		s.activeTurnID = ""
	}
	s.mu.Unlock()
}

func (s *appServerSession) markMessageDelta(itemID string) {
	s.mu.Lock()
	if s.seenMessageDelta == nil {
		s.seenMessageDelta = map[string]bool{}
	}
	s.seenMessageDelta[itemID] = true
	s.mu.Unlock()
}

func (s *appServerSession) hasMessageDelta(itemID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seenMessageDelta[itemID]
}

func (s *appServerSession) markReasoningDelta(itemID string) {
	s.mu.Lock()
	if s.seenReasoningDelta == nil {
		s.seenReasoningDelta = map[string]bool{}
	}
	s.seenReasoningDelta[itemID] = true
	s.mu.Unlock()
}

func (s *appServerSession) hasReasoningDelta(itemID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seenReasoningDelta[itemID]
}

func (s *appServerSession) appendCommandOutput(itemID string, delta string) {
	if delta == "" {
		return
	}
	s.mu.Lock()
	if s.commandOutputs == nil {
		s.commandOutputs = map[string]*strings.Builder{}
	}
	buf := s.commandOutputs[itemID]
	if buf == nil {
		buf = &strings.Builder{}
		s.commandOutputs[itemID] = buf
	}
	buf.WriteString(delta)
	s.mu.Unlock()
}

func (s *appServerSession) commandOutput(itemID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.commandOutputs == nil || s.commandOutputs[itemID] == nil {
		return ""
	}
	return s.commandOutputs[itemID].String()
}

func (s *appServerSession) appendMCPProgress(itemID string, message string) {
	message = strings.TrimSpace(message)
	if message == "" {
		return
	}
	s.mu.Lock()
	if s.mcpProgress == nil {
		s.mcpProgress = map[string][]string{}
	}
	s.mcpProgress[itemID] = append(s.mcpProgress[itemID], message)
	s.mu.Unlock()
}

func (s *appServerSession) mcpProgressFor(itemID string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.mcpProgress[itemID]) == 0 {
		return nil
	}
	return append([]string(nil), s.mcpProgress[itemID]...)
}

func (s *appServerSession) clearToolBuffers(itemID string) {
	s.mu.Lock()
	if s.commandOutputs != nil {
		delete(s.commandOutputs, itemID)
	}
	if s.mcpProgress != nil {
		delete(s.mcpProgress, itemID)
	}
	s.mu.Unlock()
}

func (s *appServerSession) markCompletedOnStart(itemID string) {
	s.mu.Lock()
	if s.completedOnStart == nil {
		s.completedOnStart = map[string]bool{}
	}
	s.completedOnStart[itemID] = true
	s.mu.Unlock()
}

func (s *appServerSession) consumeCompletedOnStart(itemID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.completedOnStart[itemID] {
		return false
	}
	delete(s.completedOnStart, itemID)
	return true
}

type appServerTurn struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Error  *struct {
		Message           string `json:"message"`
		AdditionalDetails string `json:"additionalDetails"`
	} `json:"error"`
}

type appServerItem struct {
	Type             string           `json:"type"`
	ID               string           `json:"id"`
	Text             string           `json:"text"`
	Summary          []string         `json:"summary"`
	Content          []string         `json:"content"`
	Status           string           `json:"status"`
	Cwd              string           `json:"cwd"`
	Command          string           `json:"command"`
	CommandActions   []map[string]any `json:"commandActions"`
	AggregatedOutput string           `json:"aggregatedOutput"`
	ExitCode         *int             `json:"exitCode"`
	Changes          []map[string]any `json:"changes"`
	Server           string           `json:"server"`
	Tool             string           `json:"tool"`
	Arguments        any              `json:"arguments"`
	Result           any              `json:"result"`
	Error            any              `json:"error"`
	Query            string           `json:"query"`
	Action           map[string]any   `json:"action"`
	Path             string           `json:"path"`
	SavedPath        string           `json:"savedPath"`
	RevisedPrompt    string           `json:"revisedPrompt"`
}

func decodeAppServerItem(raw json.RawMessage) (appServerItem, map[string]any, bool) {
	if len(raw) == 0 {
		return appServerItem{}, nil, false
	}
	var item appServerItem
	if err := json.Unmarshal(raw, &item); err != nil || item.ID == "" || item.Type == "" {
		return appServerItem{}, nil, false
	}
	var rawMap map[string]any
	_ = json.Unmarshal(raw, &rawMap)
	if rawMap == nil {
		rawMap = map[string]any{}
	}
	return item, rawMap, true
}

func itemToolKind(item appServerItem) acp.ToolKind {
	switch item.Type {
	case "fileChange":
		return ""
	case "commandExecution":
		if action := singleCommandAction(item); action != nil {
			switch actionType(action) {
			case "read", "listFiles":
				return acp.ToolKindRead
			case "search":
				return acp.ToolKindSearch
			}
		}
		return acp.ToolKindExecute
	case "mcpToolCall", "dynamicToolCall":
		return acp.ToolKindExecute
	case "webSearch":
		return acp.ToolKindSearch
	case "imageView":
		return acp.ToolKindRead
	case "reasoning":
		return acp.ToolKindThink
	default:
		return acp.ToolKindOther
	}
}

func itemToolStatus(status string, complete bool) acp.ToolCallStatus {
	switch strings.TrimSpace(status) {
	case "inProgress", "in_progress", "running", "generating":
		return acp.ToolCallStatusInProgress
	case "completed", "approved":
		return acp.ToolCallStatusCompleted
	case "failed", "declined", "denied", "aborted", "timedOut":
		return acp.ToolCallStatusFailed
	}
	if complete {
		return acp.ToolCallStatusCompleted
	}
	return acp.ToolCallStatusPending
}

func itemToolTitle(item appServerItem) string {
	switch item.Type {
	case "fileChange":
		return "file_edit"
	case "commandExecution":
		if action := singleCommandAction(item); action != nil {
			return commandActionTitle(action)
		}
		if command := strings.TrimSpace(item.Command); command != "" {
			return stripShellPrefix(command)
		}
		return "Run command"
	case "mcpToolCall":
		return "mcp." + firstNonBlank(item.Server, "server") + "." + firstNonBlank(item.Tool, "tool")
	case "dynamicToolCall":
		return firstNonBlank(item.Tool, "Dynamic tool")
	case "webSearch":
		return webSearchTitle(item)
	case "imageView":
		return "View image " + firstNonBlank(item.Path, item.ID)
	case "imageGeneration":
		return "Image generation"
	default:
		return firstNonBlank(item.Type, "tool")
	}
}

func itemRawInput(item appServerItem, raw map[string]any) any {
	switch item.Type {
	case "commandExecution":
		input := map[string]any{"command": item.Command, "cwd": item.Cwd}
		if len(item.CommandActions) > 0 {
			input["commandActions"] = item.CommandActions
		}
		return input
	case "mcpToolCall":
		return map[string]any{"server": item.Server, "tool": item.Tool, "arguments": item.Arguments}
	case "dynamicToolCall":
		return map[string]any{"tool": item.Tool, "arguments": item.Arguments}
	case "fileChange":
		return map[string]any{"changes": item.Changes}
	case "webSearch":
		return map[string]any{"query": item.Query, "action": item.Action}
	case "imageView":
		return map[string]any{"path": item.Path}
	case "imageGeneration":
		return map[string]any{"id": item.ID}
	default:
		return raw
	}
}

func itemRawOutput(item appServerItem, raw map[string]any, commandOutput string) any {
	switch item.Type {
	case "commandExecution":
		output := firstNonBlank(item.AggregatedOutput, commandOutput)
		result := map[string]any{"formatted_output": output, "output": output}
		if item.ExitCode != nil {
			result["exitCode"] = *item.ExitCode
			result["exit_code"] = *item.ExitCode
		}
		return result
	case "mcpToolCall":
		return map[string]any{"result": item.Result, "error": item.Error}
	case "dynamicToolCall":
		return map[string]any{"result": item.Result, "error": item.Error}
	case "fileChange":
		output := map[string]any{"changes": item.Changes, "status": item.Status}
		if summary := fileChangeSummary(item.Changes); summary != nil {
			for key, value := range summary {
				output[key] = value
			}
		}
		return output
	case "imageGeneration":
		return map[string]any{
			"status":        item.Status,
			"revisedPrompt": item.RevisedPrompt,
			"result":        item.Result,
			"savedPath":     item.SavedPath,
		}
	default:
		if len(raw) > 0 {
			return raw
		}
		return map[string]any{"status": item.Status}
	}
}

func itemLocations(item appServerItem, raw map[string]any) []acp.ToolCallLocation {
	switch item.Type {
	case "commandExecution":
		if action := singleCommandAction(item); action != nil {
			if path := strings.TrimSpace(fmt.Sprint((*action)["path"])); path != "" {
				return []acp.ToolCallLocation{{Path: path}}
			}
		}
	case "imageView":
		if strings.TrimSpace(item.Path) != "" {
			return []acp.ToolCallLocation{{Path: item.Path}}
		}
	case "fileChange":
		seen := map[string]bool{}
		var locations []acp.ToolCallLocation
		for _, change := range item.Changes {
			path := strings.TrimSpace(fmt.Sprint(change["path"]))
			if path == "" || seen[path] {
				continue
			}
			seen[path] = true
			locations = append(locations, acp.ToolCallLocation{Path: path})
		}
		return locations
	}
	if path := strings.TrimSpace(fmt.Sprint(raw["path"])); path != "" {
		return []acp.ToolCallLocation{{Path: path}}
	}
	return nil
}

func singleCommandAction(item appServerItem) *map[string]any {
	if len(item.CommandActions) != 1 {
		return nil
	}
	return &item.CommandActions[0]
}

func actionType(action *map[string]any) string {
	if action == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint((*action)["type"]))
}

func commandActionTitle(action *map[string]any) string {
	if action == nil {
		return "Run command"
	}
	path := strings.TrimSpace(fmt.Sprint((*action)["path"]))
	query := strings.TrimSpace(fmt.Sprint((*action)["query"]))
	command := strings.TrimSpace(fmt.Sprint((*action)["command"]))
	switch actionType(action) {
	case "read":
		if path != "" {
			return "Read file '" + path + "'"
		}
		return "Read file"
	case "search":
		if query != "" && path != "" {
			return "Search for '" + query + "' in " + path
		}
		if query != "" {
			return "Search for '" + query + "'"
		}
		if path != "" {
			return "Search in '" + path + "'"
		}
		return "Search"
	case "listFiles":
		if path != "" {
			return "List files in '" + path + "'"
		}
		return "List files"
	case "unknown":
		if command != "" {
			return stripShellPrefix(command)
		}
	}
	return "Run command"
}

func fileChangeSummary(changes []map[string]any) map[string]any {
	if len(changes) != 1 {
		return nil
	}
	change := changes[0]
	path := strings.TrimSpace(fmt.Sprint(change["path"]))
	if path == "" {
		return nil
	}
	added, deleted := diffLineStats(fmt.Sprint(change["diff"]))
	edited := minInt(added, deleted)
	return map[string]any{
		"filePath": path,
		"lineStats": map[string]any{
			"addedLines":   added,
			"deletedLines": deleted,
			"editedLines":  edited,
		},
	}
}

func diffLineStats(diff string) (int, int) {
	added := 0
	deleted := 0
	for _, line := range strings.Split(diff, "\n") {
		if strings.HasPrefix(line, "+++") || strings.HasPrefix(line, "---") {
			continue
		}
		if strings.HasPrefix(line, "+") {
			added++
			continue
		}
		if strings.HasPrefix(line, "-") {
			deleted++
		}
	}
	return added, deleted
}

func minInt(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func webSearchTitle(item appServerItem) string {
	if len(item.Action) > 0 {
		actionType, _ := item.Action["type"].(string)
		switch actionType {
		case "openPage":
			return "Open page: " + strings.TrimSpace(fmt.Sprint(item.Action["url"]))
		case "findInPage":
			return "Find in page"
		}
	}
	if query := strings.TrimSpace(item.Query); query != "" {
		return "Web search: " + query
	}
	return "Web search"
}

func planPriority(priority string) acp.PlanEntryPriority {
	switch strings.TrimSpace(priority) {
	case "high":
		return acp.PlanEntryPriorityHigh
	case "low":
		return acp.PlanEntryPriorityLow
	default:
		return acp.PlanEntryPriorityMedium
	}
}

func planStatus(status string) acp.PlanEntryStatus {
	switch strings.TrimSpace(status) {
	case "inProgress", "in_progress":
		return acp.PlanEntryStatusInProgress
	case "completed", "done":
		return acp.PlanEntryStatusCompleted
	default:
		return acp.PlanEntryStatusPending
	}
}

func stripShellPrefix(command string) string {
	command = strings.TrimSpace(command)
	for _, prefix := range []string{"/bin/bash -lc ", "bash -lc ", "/bin/sh -lc ", "sh -lc "} {
		if strings.HasPrefix(command, prefix) {
			return strings.Trim(strings.TrimSpace(strings.TrimPrefix(command, prefix)), `"'`)
		}
	}
	return command
}

func resultFromTurn(turn appServerTurn) (appTurnResult, bool) {
	switch turn.Status {
	case "completed":
		return appTurnResult{stop: acp.StopReasonEndTurn}, true
	case "interrupted":
		return appTurnResult{stop: acp.StopReasonCancelled}, true
	case "failed":
		msg := "codex app-server turn failed"
		if turn.Error != nil {
			msg = firstNonBlank(turn.Error.Message, turn.Error.AdditionalDetails, msg)
		}
		return appTurnResult{err: errors.New(msg)}, true
	default:
		return appTurnResult{}, false
	}
}

func commandPermissionOptions(raw any) []acp.PermissionOption {
	decisions := decisionStrings(raw)
	if len(decisions) == 0 {
		decisions = []string{"accept", "acceptForSession", "decline"}
	}
	options := make([]acp.PermissionOption, 0, len(decisions))
	for _, decision := range decisions {
		switch decision {
		case "accept":
			options = append(options, acp.PermissionOption{OptionId: "accept", Name: "Allow once", Kind: acp.PermissionOptionKindAllowOnce})
		case "acceptForSession":
			options = append(options, acp.PermissionOption{OptionId: "acceptForSession", Name: "Allow for session", Kind: acp.PermissionOptionKindAllowAlways})
		case "decline", "cancel":
			options = append(options, acp.PermissionOption{OptionId: acp.PermissionOptionId(decision), Name: "Reject", Kind: acp.PermissionOptionKindRejectOnce})
		}
	}
	if len(options) == 0 {
		options = append(options, acp.PermissionOption{OptionId: "decline", Name: "Reject", Kind: acp.PermissionOptionKindRejectOnce})
	}
	return options
}

func decisionStrings(raw any) []string {
	items, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if value, ok := item.(string); ok {
			out = append(out, value)
		}
	}
	return out
}

func questionPermissionOptions() []acp.PermissionOption {
	return []acp.PermissionOption{
		{OptionId: "submitted", Name: "Submit answer", Kind: acp.PermissionOptionKindAllowOnce},
		{OptionId: "cancelled", Name: "Cancel", Kind: acp.PermissionOptionKindRejectOnce},
	}
}

func questionMaps(raw any) []map[string]any {
	switch items := raw.(type) {
	case []map[string]any:
		return cloneMapSlice(items)
	case []any:
		out := make([]map[string]any, 0, len(items))
		for _, item := range items {
			if question, ok := item.(map[string]any); ok {
				out = append(out, cloneAnyMap(question))
			}
		}
		return out
	default:
		return nil
	}
}

func elicitationQuestions(payload map[string]any) []map[string]any {
	schema, _ := payload["requestedSchema"].(map[string]any)
	properties, _ := schema["properties"].(map[string]any)
	if len(properties) == 0 {
		return nil
	}

	keys := orderedSchemaKeys(schema, properties)
	questions := make([]map[string]any, 0, len(keys))
	for _, key := range keys {
		prop, _ := properties[key].(map[string]any)
		if prop == nil {
			continue
		}
		question := map[string]any{
			"id":       key,
			"type":     "text",
			"header":   firstNonBlank(stringField(prop, "title"), key),
			"question": firstNonBlank(stringField(prop, "description"), stringField(prop, "title"), key),
		}
		if options := enumOptions(prop["enum"]); len(options) > 0 {
			question["type"] = "select"
			question["options"] = options
		}
		questions = append(questions, question)
	}
	return questions
}

func orderedSchemaKeys(schema map[string]any, properties map[string]any) []string {
	seen := map[string]bool{}
	keys := make([]string, 0, len(properties))
	for _, key := range stringSlice(schema["required"]) {
		if _, ok := properties[key]; ok && !seen[key] {
			keys = append(keys, key)
			seen[key] = true
		}
	}
	rest := make([]string, 0, len(properties))
	for key := range properties {
		if !seen[key] {
			rest = append(rest, key)
		}
	}
	sort.Strings(rest)
	keys = append(keys, rest...)
	return keys
}

func enumOptions(raw any) []map[string]any {
	values := stringSlice(raw)
	if len(values) == 0 {
		return nil
	}
	options := make([]map[string]any, 0, len(values))
	for _, value := range values {
		options = append(options, map[string]any{"label": value, "description": value})
	}
	return options
}

func appServerQuestionAnswers(outcome acp.RequestPermissionOutcome) map[string]any {
	answers := questionAnswerMaps(outcome)
	result := make(map[string]any, len(answers))
	for _, answer := range answers {
		id, _ := answer["id"].(string)
		if id == "" {
			continue
		}
		result[id] = map[string]any{"answers": answerValues(answer)}
	}
	return result
}

func elicitationContent(outcome acp.RequestPermissionOutcome) map[string]any {
	answers := questionAnswerMaps(outcome)
	content := make(map[string]any, len(answers))
	for _, answer := range answers {
		id, _ := answer["id"].(string)
		if id == "" {
			continue
		}
		content[id] = answerValue(answer)
	}
	return content
}

func questionAnswerMaps(outcome acp.RequestPermissionOutcome) []map[string]any {
	if outcome.Selected == nil || outcome.Selected.Meta == nil {
		return nil
	}
	return questionMaps(outcome.Selected.Meta["answers"])
}

func answerValues(answer map[string]any) []string {
	if values, ok := answer["answers"]; ok {
		return stringSlice(values)
	}
	if value, ok := answer["answer"]; ok {
		return []string{fmt.Sprint(value)}
	}
	return nil
}

func answerValue(answer map[string]any) any {
	if value, ok := answer["answer"]; ok {
		return value
	}
	values := answerValues(answer)
	if len(values) == 1 {
		return values[0]
	}
	return values
}

func stringSlice(raw any) []string {
	switch values := raw.(type) {
	case []string:
		return append([]string(nil), values...)
	case []any:
		out := make([]string, 0, len(values))
		for _, value := range values {
			if text, ok := value.(string); ok {
				out = append(out, text)
			}
		}
		return out
	default:
		return nil
	}
}

func cloneMapSlice(in []map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(in))
	for _, item := range in {
		out = append(out, cloneAnyMap(item))
	}
	return out
}

func cloneAnyMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func commandDecisionFromOutcome(outcome acp.RequestPermissionOutcome) any {
	if outcome.Selected != nil {
		switch outcome.Selected.OptionId {
		case "accept", "acceptForSession", "decline", "cancel":
			return string(outcome.Selected.OptionId)
		}
	}
	return "cancel"
}

func fileDecisionFromOutcome(outcome acp.RequestPermissionOutcome) string {
	if outcome.Selected != nil {
		switch outcome.Selected.OptionId {
		case "accept", "acceptForSession", "decline", "cancel":
			return string(outcome.Selected.OptionId)
		}
	}
	return "cancel"
}

func stringField(m map[string]any, key string) string {
	value, _ := m[key].(string)
	return value
}

type jsonRPCMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *jsonRPCError   `json:"error,omitempty"`
}

type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *jsonRPCError) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

type jsonRPCResult struct {
	result json.RawMessage
	err    error
}

type jsonRPCClient struct {
	in  io.Reader
	out io.Writer

	onNotification func(method string, params json.RawMessage)
	onRequest      func(method string, params json.RawMessage) (any, error)

	nextID  atomic.Int64
	writeMu sync.Mutex
	mu      sync.Mutex
	pending map[string]chan jsonRPCResult
	closed  bool
}

func newJSONRPCClient(out io.Writer, in io.Reader) *jsonRPCClient {
	return &jsonRPCClient{
		in:      in,
		out:     out,
		pending: map[string]chan jsonRPCResult{},
	}
}

func (c *jsonRPCClient) start() {
	go c.readLoop()
}

func (c *jsonRPCClient) call(ctx context.Context, method string, params any, result any) error {
	id := "req_" + strconv.FormatInt(c.nextID.Add(1), 10)
	idBytes, _ := json.Marshal(id)
	key := string(idBytes)
	ch := make(chan jsonRPCResult, 1)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return io.ErrClosedPipe
	}
	c.pending[key] = ch
	c.mu.Unlock()

	if err := c.write(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  method,
		"params":  params,
	}); err != nil {
		c.removePending(key)
		return err
	}

	select {
	case response := <-ch:
		if response.err != nil {
			return response.err
		}
		if result == nil || len(response.result) == 0 {
			return nil
		}
		return json.Unmarshal(response.result, result)
	case <-ctx.Done():
		c.removePending(key)
		return ctx.Err()
	}
}

func (c *jsonRPCClient) readLoop() {
	scanner := bufio.NewScanner(c.in)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var msg jsonRPCMessage
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			continue
		}
		c.handleMessage(msg)
	}
	if err := scanner.Err(); err != nil {
		c.closeWithError(err)
	} else {
		c.closeWithError(io.EOF)
	}
}

func (c *jsonRPCClient) handleMessage(msg jsonRPCMessage) {
	if msg.Method != "" && len(msg.ID) > 0 {
		c.handleRequest(msg)
		return
	}
	if msg.Method != "" {
		if c.onNotification != nil {
			c.onNotification(msg.Method, msg.Params)
		}
		return
	}
	if len(msg.ID) > 0 {
		key := string(msg.ID)
		ch := c.removePending(key)
		if ch == nil {
			return
		}
		if msg.Error != nil {
			ch <- jsonRPCResult{err: msg.Error}
			return
		}
		ch <- jsonRPCResult{result: msg.Result}
	}
}

func (c *jsonRPCClient) handleRequest(msg jsonRPCMessage) {
	var result any
	var err error
	if c.onRequest == nil {
		err = &jsonRPCError{Code: -32601, Message: "method not found"}
	} else {
		result, err = c.onRequest(msg.Method, msg.Params)
	}
	if err != nil {
		code := -32603
		if rpcErr := (*jsonRPCError)(nil); errors.As(err, &rpcErr) {
			code = rpcErr.Code
		}
		_ = c.write(map[string]any{
			"jsonrpc": "2.0",
			"id":      json.RawMessage(msg.ID),
			"error": map[string]any{
				"code":    code,
				"message": err.Error(),
			},
		})
		return
	}
	_ = c.write(map[string]any{
		"jsonrpc": "2.0",
		"id":      json.RawMessage(msg.ID),
		"result":  result,
	})
}

func (c *jsonRPCClient) write(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err = c.out.Write(data)
	return err
}

func (c *jsonRPCClient) removePending(key string) chan jsonRPCResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := c.pending[key]
	delete(c.pending, key)
	return ch
}

func (c *jsonRPCClient) closeWithError(err error) {
	if err == nil {
		err = io.EOF
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	pending := c.pending
	c.pending = map[string]chan jsonRPCResult{}
	c.mu.Unlock()
	for _, ch := range pending {
		ch <- jsonRPCResult{err: err}
	}
}
