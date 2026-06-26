package acpbridge

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/acp-go-sdk"

	"proxy-acp-codex/internal/platform"
)

const approvalOptionJSONPrefix = "decision_json:"

type turn struct {
	req  platform.QueryRequest
	sink EventSink
	mgr  *Manager

	seq atomic.Int64

	mu                  sync.Mutex
	contentOpen         bool
	contentID           string
	contentSeq          int
	activeReasoningBase string
	activeReasoningID   string
	reasoningFallbackID string
	reasoningSegments   map[string]int
	planSeen            bool
	planningID          string
	planningText        strings.Builder
	planningEnded       bool
	awaitedPlanningID   string
	toolStarted         map[string]bool
	toolEnded           map[string]bool
	steers              []platform.SteerRequest
	acceptingSteer      bool
	interrupted         bool
}

type pendingPermission struct {
	runID      string
	awaitingID string
	mode       string
	options    []acp.PermissionOption
	questions  []map[string]any
	response   chan acp.RequestPermissionOutcome
	planReply  chan planDecision
	done       chan struct{}
}

type planDecision struct {
	Decision string
	Reason   string
	Params   []map[string]any
}

func newTurn(req platform.QueryRequest, sink EventSink, mgr *Manager) *turn {
	return &turn{
		req:                 req,
		sink:                sink,
		mgr:                 mgr,
		reasoningFallbackID: stableID(req.RunID, "reasoning"),
		reasoningSegments:   map[string]int{},
		toolStarted:         map[string]bool{},
		toolEnded:           map[string]bool{},
		acceptingSteer:      true,
	}
}

func (t *turn) emit(eventType string, payload map[string]any) {
	if payload == nil {
		payload = map[string]any{}
	}
	t.addEventContext(payload)
	event := platform.NewEvent(t.seq.Add(1), eventType, payload)
	if err := t.sink.Publish(event); err != nil {
		// The HTTP/WS caller owns connection cancellation; publishing errors
		// are intentionally not fatal inside ACP callbacks.
		return
	}
}

func (t *turn) addEventContext(payload map[string]any) {
	setIfMissing := func(key string, value string) {
		if strings.TrimSpace(value) == "" {
			return
		}
		if existing, ok := payload[key]; ok && strings.TrimSpace(fmt.Sprint(existing)) != "" {
			return
		}
		payload[key] = value
	}
	setIfMissing("runId", t.req.RunID)
	setIfMissing("chatId", t.req.ChatID)
	setIfMissing("agentKey", t.req.AgentKey)
	setIfMissing("teamId", t.req.TeamID)
}

func (t *turn) emitRunError(err error) {
	if err == nil {
		return
	}
	t.emit("run.error", map[string]any{
		"runId": t.req.RunID,
		"error": map[string]any{
			"code":    "acp_error",
			"message": err.Error(),
		},
	})
}

func (t *turn) enqueueSteer(req platform.SteerRequest) platform.SteerResponse {
	if strings.TrimSpace(req.SteerID) == "" {
		req.SteerID = time.Now().UTC().Format("20060102150405.000000000")
	}
	t.mu.Lock()
	if t.interrupted || !t.acceptingSteer {
		t.mu.Unlock()
		return platform.SteerResponse{
			Accepted: false,
			Status:   "unmatched",
			RunID:    req.RunID,
			SteerID:  req.SteerID,
			Detail:   "ACP run is not accepting steer",
		}
	}
	t.steers = append(t.steers, req)
	t.mu.Unlock()
	t.emit("request.steer", map[string]any{
		"requestId": req.RequestID,
		"chatId":    firstNonBlank(req.ChatID, t.req.ChatID),
		"runId":     t.req.RunID,
		"steerId":   req.SteerID,
		"message":   req.Message,
	})
	return platform.SteerResponse{
		Accepted: true,
		Status:   "accepted",
		RunID:    req.RunID,
		SteerID:  req.SteerID,
		Detail:   "ACP steer queued",
	}
}

func (t *turn) nextSteer() (platform.SteerRequest, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.interrupted {
		t.acceptingSteer = false
		return platform.SteerRequest{}, false
	}
	if len(t.steers) == 0 {
		t.acceptingSteer = false
		return platform.SteerRequest{}, false
	}
	next := t.steers[0]
	copy(t.steers, t.steers[1:])
	t.steers = t.steers[:len(t.steers)-1]
	return next, true
}

func (t *turn) interrupt() {
	t.mu.Lock()
	t.interrupted = true
	t.acceptingSteer = false
	t.steers = nil
	t.mu.Unlock()
}

func (t *turn) handleUpdate(update acp.SessionUpdate) error {
	switch {
	case update.AgentMessageChunk != nil:
		text := contentBlockText(update.AgentMessageChunk.Content)
		if text != "" {
			t.closeActiveReasoning()
			contentID := t.ensureContent()
			t.emit("content.delta", map[string]any{"contentId": contentID, "delta": text})
		}
	case update.AgentThoughtChunk != nil:
		if t.handleInternalUsageSnapshotUpdate(*update.AgentThoughtChunk) {
			return nil
		}
		if t.handleInternalPlanningUpdate(*update.AgentThoughtChunk) {
			return nil
		}
		text := contentBlockText(update.AgentThoughtChunk.Content)
		if text != "" {
			reasoningID := t.ensureReasoning(t.reasoningIDForChunk(*update.AgentThoughtChunk))
			t.emit("reasoning.delta", map[string]any{"reasoningId": reasoningID, "delta": text})
		}
	case update.ToolCall != nil:
		t.closeActiveReasoning()
		t.handleToolStart(*update.ToolCall)
	case update.ToolCallUpdate != nil:
		t.closeActiveReasoning()
		t.handleToolUpdate(*update.ToolCallUpdate)
	case update.Plan != nil:
		t.closeActiveReasoning()
		eventType := "plan.create"
		if t.planSeen {
			eventType = "plan.update"
		}
		t.planSeen = true
		t.emit(eventType, map[string]any{
			"planId": t.req.RunID + "_plan",
			"chatId": t.req.ChatID,
			"plan":   planPayload(update.Plan.Entries),
		})
	case update.UsageUpdate != nil:
		t.emit("debug.postCall", map[string]any{
			"runId":  t.req.RunID,
			"chatId": t.req.ChatID,
			"data":   map[string]any{"contextWindow": update.UsageUpdate.Size, "currentContextSize": update.UsageUpdate.Used},
		})
	}
	return nil
}

func (t *turn) handleInternalUsageSnapshotUpdate(chunk acp.SessionUpdateAgentThoughtChunk) bool {
	if chunk.Meta == nil {
		return false
	}
	eventType := strings.TrimSpace(fmt.Sprint(chunk.Meta[platform.ACPMetaEventType]))
	if eventType != "usage.snapshot" {
		return false
	}
	payload := normalizedUsageSnapshotPayload(chunk.Meta[platform.ACPMetaUsageSnapshot])
	if len(payload) == 0 {
		return true
	}
	t.closeActiveReasoning()
	t.emit("usage.snapshot", payload)
	return true
}

func (t *turn) handleInternalPlanningUpdate(chunk acp.SessionUpdateAgentThoughtChunk) bool {
	if chunk.Meta == nil {
		return false
	}
	eventType := strings.TrimSpace(fmt.Sprint(chunk.Meta[platform.ACPMetaEventType]))
	if eventType == "" {
		return false
	}
	switch eventType {
	case "planning.start", "planning.delta", "planning.end":
	default:
		return false
	}
	planningID := strings.TrimSpace(fmt.Sprint(chunk.Meta[platform.ACPMetaPlanningID]))
	if planningID == "" || planningID == "<nil>" {
		return false
	}
	t.closeActiveReasoning()
	delta := ""
	if eventType == "planning.delta" {
		delta = contentBlockText(chunk.Content)
	}
	t.recordPlanningEvent(eventType, planningID, delta)
	payload := map[string]any{"planningId": planningID}
	if eventType == "planning.delta" {
		payload["delta"] = delta
	}
	t.emit(eventType, payload)
	return true
}

func normalizedUsageSnapshotPayload(value any) map[string]any {
	raw, ok := anyMap(value)
	if !ok {
		return nil
	}
	out := map[string]any{}
	if model, ok := normalizedStringMap(raw["model"]); ok {
		out["model"] = model
	}
	if contextWindow, ok := normalizedContextWindow(raw["contextWindow"]); ok {
		out["contextWindow"] = contextWindow
	}
	if usage, ok := normalizedUsageSnapshotUsage(raw["usage"]); ok {
		out["usage"] = usage
	}
	return out
}

func normalizedUsageSnapshotUsage(value any) (map[string]any, bool) {
	raw, ok := anyMap(value)
	if !ok {
		return nil, false
	}
	out := map[string]any{}
	for _, key := range []string{"current", "run", "chat"} {
		if stats, ok := normalizedUsageStats(raw[key]); ok {
			out[key] = stats
		}
	}
	return out, len(out) > 0
}

func normalizedUsageStats(value any) (map[string]any, bool) {
	raw, ok := anyMap(value)
	if !ok {
		return nil, false
	}
	out := map[string]any{}
	for _, key := range []string{"modelKey", "reasoningEffort"} {
		if text := strings.TrimSpace(fmt.Sprint(raw[key])); text != "" && text != "<nil>" {
			out[key] = text
		}
	}
	for _, key := range []string{"promptTokens", "completionTokens", "totalTokens", "llmChatCompletionCount", "toolCallCount"} {
		if number, ok := intFromAny(raw[key]); ok {
			out[key] = number
		}
	}
	if details, ok := normalizedUsageTokenDetails(raw["promptTokensDetails"], []string{"cacheHitTokens", "cacheMissTokens"}); ok {
		out["promptTokensDetails"] = details
	}
	if details, ok := normalizedUsageTokenDetails(raw["completionTokensDetails"], []string{"reasoningTokens"}); ok {
		out["completionTokensDetails"] = details
	}
	return out, len(out) > 0
}

func normalizedContextWindow(value any) (map[string]any, bool) {
	raw, ok := anyMap(value)
	if !ok {
		return nil, false
	}
	out := map[string]any{}
	for _, key := range []string{"modelKey", "reasoningEffort"} {
		if text := strings.TrimSpace(fmt.Sprint(raw[key])); text != "" && text != "<nil>" {
			out[key] = text
		}
	}
	for _, key := range []string{"maxSize", "currentSize", "estimatedNextCallSize"} {
		if number, ok := intFromAny(raw[key]); ok {
			out[key] = number
		}
	}
	return out, len(out) > 0
}

func normalizedUsageTokenDetails(value any, keys []string) (map[string]any, bool) {
	raw, ok := anyMap(value)
	if !ok {
		return nil, false
	}
	out := map[string]any{}
	for _, key := range keys {
		if number, ok := intFromAny(raw[key]); ok {
			out[key] = number
		}
	}
	return out, len(out) > 0
}

func normalizedStringMap(value any) (map[string]any, bool) {
	raw, ok := anyMap(value)
	if !ok {
		return nil, false
	}
	out := map[string]any{}
	for key, value := range raw {
		if text := strings.TrimSpace(fmt.Sprint(value)); text != "" && text != "<nil>" {
			out[key] = text
		}
	}
	return out, len(out) > 0
}

func anyMap(value any) (map[string]any, bool) {
	if value == nil {
		return nil, false
	}
	if typed, ok := value.(map[string]any); ok {
		return typed, true
	}
	return nil, false
}

func intFromAny(value any) (int, bool) {
	switch typed := value.(type) {
	case nil:
		return 0, false
	case int:
		return typed, true
	case int8:
		return int(typed), true
	case int16:
		return int(typed), true
	case int32:
		return int(typed), true
	case int64:
		return int(typed), true
	case uint:
		return int(typed), true
	case uint8:
		return int(typed), true
	case uint16:
		return int(typed), true
	case uint32:
		return int(typed), true
	case uint64:
		return int(typed), true
	case float32:
		return int(typed), true
	case float64:
		return int(typed), true
	case json.Number:
		n, err := typed.Int64()
		if err != nil {
			return 0, false
		}
		return int(n), true
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(typed))
		if err != nil {
			return 0, false
		}
		return n, true
	default:
		return 0, false
	}
}

func (t *turn) recordPlanningEvent(eventType string, planningID string, delta string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.planningID != planningID {
		t.planningID = planningID
		t.planningText.Reset()
		t.planningEnded = false
	}
	switch eventType {
	case "planning.start":
		t.planningText.Reset()
		t.planningEnded = false
		if t.awaitedPlanningID == planningID {
			t.awaitedPlanningID = ""
		}
	case "planning.delta":
		t.planningText.WriteString(delta)
	case "planning.end":
		t.planningEnded = true
	}
}

func (t *turn) pendingPlanApproval() (string, string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	planningID := strings.TrimSpace(t.planningID)
	if planningID == "" || !t.planningEnded || t.awaitedPlanningID == planningID {
		return "", "", false
	}
	planText := strings.TrimSpace(t.planningText.String())
	if planText == "" {
		return "", "", false
	}
	t.awaitedPlanningID = planningID
	return planningID, planText, true
}

func (t *turn) ensureContent() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.contentOpen {
		return t.contentID
	}
	t.contentSeq++
	t.contentID = stableID(t.req.RunID, fmt.Sprintf("c_%d", t.contentSeq))
	t.contentOpen = true
	t.emit("content.start", map[string]any{"contentId": t.contentID, "runId": t.req.RunID})
	return t.contentID
}

func (t *turn) ensureReasoning(reasoningID string) string {
	baseID := strings.TrimSpace(reasoningID)
	if baseID == "" {
		baseID = t.reasoningFallbackID
	}

	t.mu.Lock()
	currentID := t.activeReasoningID
	currentBase := t.activeReasoningBase
	if currentID != "" && currentBase == baseID {
		t.mu.Unlock()
		return currentID
	}
	if t.reasoningSegments == nil {
		t.reasoningSegments = map[string]int{}
	}
	t.reasoningSegments[baseID]++
	segment := t.reasoningSegments[baseID]
	nextID := baseID
	if segment > 1 {
		nextID = fmt.Sprintf("%s_segment_%d", baseID, segment)
	}
	t.activeReasoningBase = baseID
	t.activeReasoningID = nextID
	t.mu.Unlock()

	if currentID != "" {
		t.emit("reasoning.end", map[string]any{"reasoningId": currentID})
	}
	t.emit("reasoning.start", map[string]any{"reasoningId": nextID, "runId": t.req.RunID})
	return nextID
}

func (t *turn) reasoningIDForChunk(chunk acp.SessionUpdateAgentThoughtChunk) string {
	if chunk.MessageId != nil && strings.TrimSpace(*chunk.MessageId) != "" {
		return strings.TrimSpace(*chunk.MessageId)
	}
	if chunk.Meta != nil {
		if itemID := strings.TrimSpace(fmt.Sprint(chunk.Meta["codexItemId"])); itemID != "" && itemID != "<nil>" {
			return itemID
		}
	}
	return t.reasoningFallbackID
}

func (t *turn) closeActiveReasoning() {
	t.mu.Lock()
	reasoningID := t.activeReasoningID
	t.activeReasoningID = ""
	t.activeReasoningBase = ""
	t.mu.Unlock()
	if reasoningID != "" {
		t.emit("reasoning.end", map[string]any{"reasoningId": reasoningID})
	}
}

func (t *turn) closeOpenStreams() {
	t.closeOpenTextStreams()
	t.closeOpenTools()
}

func (t *turn) closeOpenTextStreams() {
	t.mu.Lock()
	contentOpen := t.contentOpen
	contentID := t.contentID
	reasoningID := t.activeReasoningID
	t.contentOpen = false
	t.activeReasoningID = ""
	t.activeReasoningBase = ""
	t.mu.Unlock()
	if reasoningID != "" {
		t.emit("reasoning.end", map[string]any{"reasoningId": reasoningID})
	}
	if contentOpen {
		t.emit("content.end", map[string]any{"contentId": contentID})
	}
}

func (t *turn) handleToolStart(tool acp.SessionUpdateToolCall) {
	toolID := string(tool.ToolCallId)
	if toolID == "" {
		return
	}
	t.mu.Lock()
	already := t.toolStarted[toolID]
	if !already {
		t.toolStarted[toolID] = true
	}
	t.mu.Unlock()
	if already {
		return
	}
	t.emit("tool.start", map[string]any{
		"toolId":          toolID,
		"runId":           t.req.RunID,
		"toolName":        toolName(tool.Title, tool.Kind),
		"toolLabel":       tool.Title,
		"toolDescription": tool.Title,
	})
	if tool.RawInput != nil {
		t.emit("tool.args", map[string]any{"toolId": toolID, "delta": marshalAny(tool.RawInput), "chunkIndex": 0})
	}
	if tool.Status == acp.ToolCallStatusCompleted || tool.Status == acp.ToolCallStatusFailed {
		t.endTool(toolID)
		t.emitToolResult(toolID, tool.RawOutput, tool.Status)
	}
}

func (t *turn) handleToolUpdate(update acp.SessionToolCallUpdate) {
	toolID := string(update.ToolCallId)
	if toolID == "" {
		return
	}
	t.mu.Lock()
	started := t.toolStarted[toolID]
	t.mu.Unlock()
	if !started {
		title := toolID
		if update.Title != nil {
			title = *update.Title
		}
		kind := acp.ToolKindOther
		if update.Kind != nil {
			kind = *update.Kind
		}
		t.handleToolStart(acp.SessionUpdateToolCall{ToolCallId: update.ToolCallId, Title: title, Kind: kind, RawInput: update.RawInput})
	}
	if update.RawInput != nil {
		t.emit("tool.args", map[string]any{"toolId": toolID, "delta": marshalAny(update.RawInput), "chunkIndex": 1})
	}
	if update.Status != nil && (*update.Status == acp.ToolCallStatusCompleted || *update.Status == acp.ToolCallStatusFailed) {
		result := update.RawOutput
		if result == nil && len(update.Content) > 0 {
			result = toolContentPayload(update.Content)
		}
		t.endTool(toolID)
		t.emitToolResult(toolID, result, *update.Status)
	}
}

func (t *turn) endTool(toolID string) {
	t.mu.Lock()
	if t.toolEnded[toolID] {
		t.mu.Unlock()
		return
	}
	t.toolEnded[toolID] = true
	t.mu.Unlock()
	t.emit("tool.end", map[string]any{"toolId": toolID})
}

func (t *turn) emitToolResult(toolID string, result any, status acp.ToolCallStatus) {
	payload := map[string]any{"toolId": toolID, "result": result}
	if status == acp.ToolCallStatusFailed {
		payload["error"] = "ACP tool call failed"
		payload["exitCode"] = 1
	}
	t.emit("tool.result", payload)
}

func (t *turn) closeOpenTools() {
	t.mu.Lock()
	ids := make([]string, 0, len(t.toolStarted))
	for id := range t.toolStarted {
		if !t.toolEnded[id] {
			ids = append(ids, id)
			t.toolEnded[id] = true
		}
	}
	t.mu.Unlock()
	for _, id := range ids {
		t.emit("tool.end", map[string]any{"toolId": id})
	}
}

func (t *turn) requestPermission(ctx context.Context, params acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	awaitingID := stableID(t.req.RunID, "perm_"+string(params.ToolCall.ToolCallId))
	mode := "approval"
	questions := questionPayload(params.ToolCall.RawInput)
	if len(questions) > 0 {
		mode = "question"
	}
	if mode == "approval" {
		if accessLevel := t.mgr.runAccessLevel(t.req.RunID); accessLevelAllowsAutoApproval(accessLevel) {
			return acp.RequestPermissionResponse{Outcome: permissionOutcomeFromAccessLevel(accessLevel, params.Options)}, nil
		}
	}
	pending := &pendingPermission{
		runID:      t.req.RunID,
		awaitingID: awaitingID,
		mode:       mode,
		options:    params.Options,
		questions:  questions,
		response:   make(chan acp.RequestPermissionOutcome, 1),
		done:       make(chan struct{}),
	}
	t.mgr.registerAwaiting(pending)
	defer func() {
		close(pending.done)
		t.mgr.unregisterAwaiting(awaitingID, pending)
	}()

	t.closeOpenTextStreams()

	if mode == "question" {
		t.emit("awaiting.ask", map[string]any{
			"awaitingId":   awaitingID,
			"mode":         "question",
			"viewportType": "builtin",
			"viewportKey":  "question",
			"timeout":      int64(120000),
			"runId":        t.req.RunID,
			"questions":    questions,
		})
	} else {
		t.emit("awaiting.ask", map[string]any{
			"awaitingId":   awaitingID,
			"mode":         "approval",
			"viewportType": "builtin",
			"viewportKey":  "approval",
			"timeout":      int64(120000),
			"runId":        t.req.RunID,
			"approvals":    permissionApprovals(params.ToolCall, params.Options),
		})
	}

	select {
	case outcome := <-pending.response:
		if mode == "question" {
			paramsPayload, _ := outcomeMeta(outcome, "params").([]map[string]any)
			answers, _ := outcomeMeta(outcome, "answers").([]map[string]any)
			if paramsPayload == nil {
				paramsPayload = []map[string]any{}
			}
			t.emit("request.submit", map[string]any{"runId": t.req.RunID, "chatId": t.req.ChatID, "awaitingId": awaitingID, "params": paramsPayload})
			if outcome.Cancelled != nil {
				t.emit("awaiting.answer", map[string]any{
					"awaitingId": awaitingID,
					"mode":       "question",
					"status":     "error",
					"error":      map[string]any{"code": "user_dismissed", "message": "用户取消了该问题"},
				})
			} else {
				t.emit("awaiting.answer", map[string]any{"awaitingId": awaitingID, "mode": "question", "status": "answered", "answers": answers})
			}
		} else {
			t.emit("request.submit", map[string]any{"runId": t.req.RunID, "chatId": t.req.ChatID, "awaitingId": awaitingID, "params": outcomePayload(outcome)})
			t.emit("awaiting.answer", map[string]any{"awaitingId": awaitingID, "mode": "approval", "status": "answered", "approvals": outcomePayload(outcome)})
		}
		return acp.RequestPermissionResponse{Outcome: outcome}, nil
	case <-ctx.Done():
		outcome := acp.RequestPermissionOutcome{Cancelled: &acp.RequestPermissionOutcomeCancelled{}}
		t.emit("awaiting.answer", map[string]any{
			"awaitingId": awaitingID,
			"mode":       mode,
			"status":     "error",
			"error":      map[string]any{"code": "timeout", "message": ctx.Err().Error()},
		})
		return acp.RequestPermissionResponse{Outcome: outcome}, nil
	}
}

func (t *turn) requestPlanApproval(ctx context.Context, planningID string, planText string) (planDecision, bool) {
	awaitingID := stableID(t.req.RunID, "plan_"+planningID)
	pending := &pendingPermission{
		runID:      t.req.RunID,
		awaitingID: awaitingID,
		mode:       "plan",
		planReply:  make(chan planDecision, 1),
		done:       make(chan struct{}),
	}
	t.mgr.registerAwaiting(pending)
	defer func() {
		close(pending.done)
		t.mgr.unregisterAwaiting(awaitingID, pending)
	}()

	t.closeOpenTextStreams()
	t.emit("awaiting.ask", map[string]any{
		"awaitingId":   awaitingID,
		"mode":         "plan",
		"viewportType": "builtin",
		"viewportKey":  "plan",
		"timeout":      int64(0),
		"runId":        t.req.RunID,
		"plan": map[string]any{
			"id":         awaitingID,
			"planningId": planningID,
			"title":      "实施此计划？",
			"text":       planText,
		},
	})

	select {
	case decision := <-pending.planReply:
		if decision.Params == nil {
			decision.Params = []map[string]any{}
		}
		t.emit("request.submit", map[string]any{"runId": t.req.RunID, "chatId": t.req.ChatID, "awaitingId": awaitingID, "params": decision.Params})
		if decision.Decision == "approve" || decision.Decision == "reject" {
			plan := map[string]any{
				"id":         awaitingID,
				"planningId": planningID,
				"decision":   decision.Decision,
			}
			if strings.TrimSpace(decision.Reason) != "" {
				plan["reason"] = strings.TrimSpace(decision.Reason)
			}
			t.emit("awaiting.answer", map[string]any{"awaitingId": awaitingID, "mode": "plan", "status": "answered", "plan": plan})
			return decision, true
		}
		t.emit("awaiting.answer", map[string]any{
			"awaitingId": awaitingID,
			"mode":       "plan",
			"status":     "error",
			"error":      map[string]any{"code": "user_dismissed", "message": "用户取消了计划确认"},
		})
		return decision, false
	case <-ctx.Done():
		t.emit("awaiting.answer", map[string]any{
			"awaitingId": awaitingID,
			"mode":       "plan",
			"status":     "error",
			"error":      map[string]any{"code": "timeout", "message": ctx.Err().Error()},
		})
		return planDecision{}, false
	}
}

func contentBlockText(block acp.ContentBlock) string {
	switch {
	case block.Text != nil:
		return block.Text.Text
	case block.ResourceLink != nil:
		return block.ResourceLink.Uri
	case block.Resource != nil:
		data, _ := json.Marshal(block.Resource.Resource)
		return string(data)
	default:
		data, _ := json.Marshal(block)
		return string(data)
	}
}

func toolContentPayload(items []acp.ToolCallContent) []any {
	out := make([]any, 0, len(items))
	for _, item := range items {
		switch {
		case item.Content != nil:
			out = append(out, contentBlockText(item.Content.Content))
		case item.Diff != nil:
			out = append(out, map[string]any{"path": item.Diff.Path, "oldText": item.Diff.OldText, "newText": item.Diff.NewText})
		case item.Terminal != nil:
			out = append(out, map[string]any{"terminalId": item.Terminal.TerminalId})
		}
	}
	return out
}

func planPayload(entries []acp.PlanEntry) []map[string]any {
	out := make([]map[string]any, 0, len(entries))
	for idx, entry := range entries {
		out = append(out, map[string]any{
			"id":       fmt.Sprintf("plan_%d", idx+1),
			"content":  entry.Content,
			"priority": string(entry.Priority),
			"status":   string(entry.Status),
		})
	}
	return out
}

func toolName(title string, kind acp.ToolKind) string {
	if kind != "" {
		return string(kind)
	}
	title = strings.TrimSpace(strings.ToLower(title))
	if title == "" {
		return "tool"
	}
	return strings.ReplaceAll(title, " ", "_")
}

func stableID(runID string, suffix string) string {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		runID = "run"
	}
	return runID + "_" + suffix
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func permissionCommand(update acp.ToolCallUpdate) string {
	if payload, ok := update.RawInput.(map[string]any); ok {
		if command := strings.TrimSpace(permissionRawString(payload["command"])); command != "" {
			return command
		}
		if reason := strings.TrimSpace(permissionRawString(payload["reason"])); reason != "" {
			return reason
		}
	}
	if update.RawInput != nil {
		return marshalAny(update.RawInput)
	}
	if update.Title != nil {
		return *update.Title
	}
	return string(update.ToolCallId)
}

func permissionApprovals(tool acp.ToolCallUpdate, options []acp.PermissionOption) []map[string]any {
	approvalOptions := make([]map[string]any, 0, 1)
	id := ""
	for _, option := range options {
		label, decision, meta := approvalOptionPresentation(option)
		if decision == "reject" {
			continue
		}
		if id == "" {
			id = string(option.OptionId)
		}
		entry := map[string]any{"label": label, "decision": decision}
		if meta != "" {
			entry["description"] = meta
		}
		approvalOptions = append(approvalOptions, entry)
		break
	}
	if id == "" && len(options) > 0 {
		id = string(options[0].OptionId)
	}
	return []map[string]any{{
		"id":            id,
		"command":       permissionCommand(tool),
		"description":   permissionDescription(tool),
		"options":       approvalOptions,
		"allowFreeText": true,
	}}
}

func permissionOutcomeFromSubmit(params []map[string]any, options []acp.PermissionOption) acp.RequestPermissionOutcome {
	if len(params) == 0 {
		return acp.RequestPermissionOutcome{Cancelled: &acp.RequestPermissionOutcomeCancelled{}}
	}
	decision, _ := params[0]["decision"].(string)
	id, _ := params[0]["id"].(string)
	if strings.HasPrefix(strings.ToLower(decision), "reject") {
		return acp.RequestPermissionOutcome{Cancelled: &acp.RequestPermissionOutcomeCancelled{}}
	}
	if strings.EqualFold(decision, "approve") {
		if optionID := permissionOptionIDByPrefix(options, approvalOptionJSONPrefix); optionID != "" {
			id = optionID
		}
	}
	if strings.EqualFold(decision, "approve_rule_run") || strings.EqualFold(decision, "approve_rule_session") {
		if optionID := permissionOptionIDByKind(options, acp.PermissionOptionKindAllowAlways); optionID != "" {
			id = optionID
		}
	}
	if id == "" && strings.EqualFold(decision, "approve") {
		id = permissionOptionIDByKind(options, acp.PermissionOptionKindAllowOnce)
	}
	if id == "" {
		for _, option := range options {
			if option.Kind == acp.PermissionOptionKindAllowOnce || option.Kind == acp.PermissionOptionKindAllowAlways {
				id = string(option.OptionId)
				break
			}
		}
	}
	if id == "" && len(options) > 0 {
		id = string(options[0].OptionId)
	}
	if id == "" {
		return acp.RequestPermissionOutcome{Cancelled: &acp.RequestPermissionOutcomeCancelled{}}
	}
	return acp.RequestPermissionOutcome{Selected: &acp.RequestPermissionOutcomeSelected{OptionId: acp.PermissionOptionId(id)}}
}

func permissionOptionIDByKind(options []acp.PermissionOption, kind acp.PermissionOptionKind) string {
	for _, option := range options {
		if option.Kind == kind {
			return string(option.OptionId)
		}
	}
	return ""
}

func permissionDescription(tool acp.ToolCallUpdate) string {
	if payload, ok := tool.RawInput.(map[string]any); ok {
		if reason := strings.TrimSpace(permissionRawString(payload["reason"])); reason != "" {
			return reason
		}
	}
	if tool.Title != nil {
		title := strings.TrimSpace(*tool.Title)
		title = strings.TrimPrefix(title, "Run command: ")
		if title != "" {
			return "Command execution requires approval"
		}
	}
	return "Approval required"
}

func approvalOptionPresentation(option acp.PermissionOption) (label string, decision string, description string) {
	switch {
	case strings.HasPrefix(string(option.OptionId), approvalOptionJSONPrefix):
		return "同意", "approve", "允许执行当前命令"
	case option.Kind == acp.PermissionOptionKindAllowAlways:
		return "同意", "approve", "允许执行当前命令"
	case option.Kind == acp.PermissionOptionKindAllowOnce:
		return "同意", "approve", "允许执行当前命令"
	default:
		return option.Name, "reject", ""
	}
}

func permissionRawString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []string:
		return strings.Join(typed, " ")
	case []any:
		parts := make([]string, 0, len(typed))
		for _, item := range typed {
			part := strings.TrimSpace(fmt.Sprint(item))
			if part != "" {
				parts = append(parts, part)
			}
		}
		return strings.Join(parts, " ")
	default:
		return ""
	}
}

func questionPayload(raw any) []map[string]any {
	input, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	mode, _ := input["mode"].(string)
	if mode != "question" {
		return nil
	}
	switch questions := input["questions"].(type) {
	case []map[string]any:
		return cloneMapSlice(questions)
	case []any:
		out := make([]map[string]any, 0, len(questions))
		for _, item := range questions {
			question, ok := item.(map[string]any)
			if !ok {
				return nil
			}
			out = append(out, cloneAnyMap(question))
		}
		return out
	default:
		return nil
	}
}

func questionOutcomeFromSubmit(params []map[string]any, questions []map[string]any) acp.RequestPermissionOutcome {
	if len(params) == 0 {
		return acp.RequestPermissionOutcome{
			Cancelled: &acp.RequestPermissionOutcomeCancelled{},
			Selected:  nil,
		}
	}
	answers := normalizeQuestionAnswers(params, questions)
	return acp.RequestPermissionOutcome{Selected: &acp.RequestPermissionOutcomeSelected{
		OptionId: acp.PermissionOptionId("submitted"),
		Meta: map[string]any{
			"params":     cloneMapSlice(params),
			"answers":    answers,
			"answerText": answerText(answers),
		},
	}}
}

func planDecisionFromSubmit(params []map[string]any) planDecision {
	decision := planDecision{Decision: "reject", Params: cloneMapSlice(params)}
	if len(params) == 0 {
		return decision
	}
	rawDecision := strings.ToLower(strings.TrimSpace(fmt.Sprint(params[0]["decision"])))
	if rawDecision == "approve" {
		decision.Decision = "approve"
	} else {
		decision.Decision = "reject"
	}
	if reason, ok := params[0]["reason"]; ok {
		decision.Reason = strings.TrimSpace(fmt.Sprint(reason))
	}
	return decision
}

func normalizeQuestionAnswers(params []map[string]any, questions []map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(params))
	for idx, param := range params {
		answer := cloneAnyMap(param)
		if idx < len(questions) {
			if id, _ := questions[idx]["id"].(string); id != "" {
				answer["id"] = id
			}
			if question, _ := questions[idx]["question"].(string); question != "" {
				answer["question"] = question
			}
		}
		out = append(out, answer)
	}
	return out
}

func answerText(answers []map[string]any) string {
	var lines []string
	for _, item := range answers {
		question, _ := item["question"].(string)
		if question == "" {
			question, _ = item["id"].(string)
		}
		answer := stringifyAnswer(item)
		if strings.TrimSpace(answer) == "" {
			continue
		}
		if question != "" {
			lines = append(lines, fmt.Sprintf("- %s：%s", question, answer))
		} else {
			lines = append(lines, "- "+answer)
		}
	}
	return strings.Join(lines, "\n")
}

func stringifyAnswer(item map[string]any) string {
	if value, ok := item["answer"]; ok {
		return fmt.Sprint(value)
	}
	if values, ok := item["answers"]; ok {
		switch typed := values.(type) {
		case []string:
			return strings.Join(typed, ", ")
		case []any:
			parts := make([]string, 0, len(typed))
			for _, value := range typed {
				parts = append(parts, fmt.Sprint(value))
			}
			return strings.Join(parts, ", ")
		default:
			return fmt.Sprint(values)
		}
	}
	return ""
}

func outcomeMeta(outcome acp.RequestPermissionOutcome, key string) any {
	if outcome.Selected == nil || outcome.Selected.Meta == nil {
		return nil
	}
	return outcome.Selected.Meta[key]
}

func outcomePayload(outcome acp.RequestPermissionOutcome) []map[string]any {
	if outcome.Cancelled != nil {
		return []map[string]any{{"decision": "reject"}}
	}
	if outcome.Selected != nil {
		return []map[string]any{{"id": string(outcome.Selected.OptionId), "decision": "approve"}}
	}
	return []map[string]any{{"decision": "reject"}}
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
