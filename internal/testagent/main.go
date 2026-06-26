package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"

	"github.com/coder/acp-go-sdk"

	"proxy-acp-codex/internal/platform"
)

type agent struct {
	conn     *acp.AgentSideConnection
	mu       sync.Mutex
	sessions map[string]struct{}
}

func main() {
	a := &agent{sessions: map[string]struct{}{}}
	conn := acp.NewAgentSideConnection(a, os.Stdout, os.Stdin)
	conn.SetLogger(slog.Default())
	a.conn = conn
	<-conn.Done()
}

func (a *agent) Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResponse, error) {
	return acp.InitializeResponse{ProtocolVersion: acp.ProtocolVersionNumber}, nil
}

func (a *agent) NewSession(context.Context, acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	id := "sess_" + randomHex()
	a.mu.Lock()
	a.sessions[id] = struct{}{}
	a.mu.Unlock()
	return acp.NewSessionResponse{SessionId: acp.SessionId(id)}, nil
}

func (a *agent) Prompt(ctx context.Context, params acp.PromptRequest) (acp.PromptResponse, error) {
	if err := a.conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: params.SessionId,
		Update:    acp.UpdateAgentThoughtText("thinking"),
	}); err != nil {
		return acp.PromptResponse{}, err
	}
	if err := a.conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: params.SessionId,
		Update:    acp.UpdateAgentMessageText("hello "),
	}); err != nil {
		return acp.PromptResponse{}, err
	}
	if err := a.conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: params.SessionId,
		Update:    acp.UpdateAgentMessageText(promptText(params)),
	}); err != nil {
		return acp.PromptResponse{}, err
	}
	planningMode, hasPlanningMode := params.Meta[platform.PromptMetaPlanningMode].(bool)
	if planningMode {
		if err := a.conn.SessionUpdate(ctx, acp.SessionNotification{
			SessionId: params.SessionId,
			Update:    acp.UpdateAgentMessageText(" planning-meta-true"),
		}); err != nil {
			return acp.PromptResponse{}, err
		}
		if strings.Contains(promptText(params), "plan-hitl") {
			if err := a.emitPlanning(ctx, params.SessionId, "plan_test", "Test implementation plan."); err != nil {
				return acp.PromptResponse{}, err
			}
		}
	} else if hasPlanningMode {
		if err := a.conn.SessionUpdate(ctx, acp.SessionNotification{
			SessionId: params.SessionId,
			Update:    acp.UpdateAgentMessageText(" planning-meta-false"),
		}); err != nil {
			return acp.PromptResponse{}, err
		}
	}
	if err := a.conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: params.SessionId,
		Update: acp.StartToolCall(
			acp.ToolCallId("tool_1"),
			"Read file",
			acp.WithStartKind(acp.ToolKindRead),
			acp.WithStartStatus(acp.ToolCallStatusPending),
			acp.WithStartRawInput(map[string]any{"path": "/tmp/demo.txt"}),
		),
	}); err != nil {
		return acp.PromptResponse{}, err
	}
	if err := a.conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: params.SessionId,
		Update: acp.UpdateToolCall(
			acp.ToolCallId("tool_1"),
			acp.WithUpdateStatus(acp.ToolCallStatusCompleted),
			acp.WithUpdateRawOutput(map[string]any{"ok": true}),
		),
	}); err != nil {
		return acp.PromptResponse{}, err
	}
	if err := a.emitUsageSnapshot(ctx, params.SessionId); err != nil {
		return acp.PromptResponse{}, err
	}
	if strings.Contains(promptText(params), "permission") {
		resp, err := a.conn.RequestPermission(ctx, acp.RequestPermissionRequest{
			SessionId: params.SessionId,
			ToolCall: acp.ToolCallUpdate{
				ToolCallId: acp.ToolCallId("tool_perm"),
				Title:      acp.Ptr("Dangerous command"),
				RawInput:   map[string]any{"command": "echo ok"},
			},
			Options: []acp.PermissionOption{
				{Kind: acp.PermissionOptionKindAllowOnce, Name: "Allow", OptionId: acp.PermissionOptionId("allow")},
				{Kind: acp.PermissionOptionKindRejectOnce, Name: "Reject", OptionId: acp.PermissionOptionId("reject")},
			},
		})
		if err != nil {
			return acp.PromptResponse{}, err
		}
		if resp.Outcome.Selected != nil {
			if err := a.conn.SessionUpdate(ctx, acp.SessionNotification{
				SessionId: params.SessionId,
				Update:    acp.UpdateAgentMessageText(" approved"),
			}); err != nil {
				return acp.PromptResponse{}, err
			}
		}
	}
	if strings.Contains(promptText(params), "question") {
		resp, err := a.conn.RequestPermission(ctx, acp.RequestPermissionRequest{
			SessionId: params.SessionId,
			ToolCall: acp.ToolCallUpdate{
				ToolCallId: acp.ToolCallId("tool_question"),
				Title:      acp.Ptr("AskUserQuestion"),
				RawInput: map[string]any{
					"mode": "question",
					"questions": []map[string]any{{
						"id":       "work_focus",
						"type":     "select",
						"question": "你目前最希望在工作中优先改进哪个方面？",
						"header":   "工作重点",
						"options": []map[string]any{
							{"label": "提升代码质量", "description": "通过重构、测试覆盖、code review 等方式让代码更健壮、可维护"},
							{"label": "加快交付速度", "description": "优化开发流程、CI/CD、自动化工具，让功能更快上线"},
							{"label": "深化技术能力", "description": "学习新技术栈、深入底层原理、扩展架构设计能力"},
						},
					}},
				},
			},
			Options: []acp.PermissionOption{
				{Kind: acp.PermissionOptionKindAllowOnce, Name: "Submit answer", OptionId: acp.PermissionOptionId("submitted")},
				{Kind: acp.PermissionOptionKindRejectOnce, Name: "Cancel", OptionId: acp.PermissionOptionId("cancelled")},
			},
		})
		if err != nil {
			return acp.PromptResponse{}, err
		}
		if resp.Outcome.Selected != nil {
			if err := a.conn.SessionUpdate(ctx, acp.SessionNotification{
				SessionId: params.SessionId,
				Update:    acp.UpdateAgentMessageText(" answered"),
			}); err != nil {
				return acp.PromptResponse{}, err
			}
		}
	}
	return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
}

func (a *agent) emitPlanning(ctx context.Context, sessionID acp.SessionId, planningID string, text string) error {
	for _, item := range []struct {
		eventType string
		text      string
	}{
		{eventType: "planning.start"},
		{eventType: "planning.delta", text: text},
		{eventType: "planning.end"},
	} {
		if err := a.conn.SessionUpdate(ctx, acp.SessionNotification{
			SessionId: sessionID,
			Update: acp.SessionUpdate{AgentThoughtChunk: &acp.SessionUpdateAgentThoughtChunk{
				Content:       acp.TextBlock(item.text),
				Meta:          map[string]any{platform.ACPMetaEventType: item.eventType, platform.ACPMetaPlanningID: planningID},
				SessionUpdate: "agent_thought_chunk",
			}},
		}); err != nil {
			return err
		}
	}
	return nil
}

func (a *agent) emitUsageSnapshot(ctx context.Context, sessionID acp.SessionId) error {
	return a.conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: sessionID,
		Update: acp.SessionUpdate{AgentThoughtChunk: &acp.SessionUpdateAgentThoughtChunk{
			Content: acp.TextBlock(""),
			Meta: map[string]any{
				platform.ACPMetaEventType: "usage.snapshot",
				platform.ACPMetaUsageSnapshot: map[string]any{
					"model": map[string]any{"key": "fake-model"},
					"contextWindow": map[string]any{
						"maxSize":         272000,
						"currentSize":     16513,
						"modelKey":        "fake-model",
						"reasoningEffort": "medium",
					},
					"usage": map[string]any{
						"current": map[string]any{
							"modelKey":               "fake-model",
							"reasoningEffort":        "medium",
							"promptTokens":           16513,
							"completionTokens":       52,
							"totalTokens":            16565,
							"llmChatCompletionCount": 1,
							"promptTokensDetails": map[string]any{
								"cacheHitTokens":  2432,
								"cacheMissTokens": 14081,
							},
							"completionTokensDetails": map[string]any{
								"reasoningTokens": 45,
							},
						},
					},
				},
			},
			SessionUpdate: "agent_thought_chunk",
		}},
	})
}

func (a *agent) Cancel(context.Context, acp.CancelNotification) error { return nil }
func (a *agent) Authenticate(context.Context, acp.AuthenticateRequest) (acp.AuthenticateResponse, error) {
	return acp.AuthenticateResponse{}, nil
}
func (a *agent) LoadSession(context.Context, acp.LoadSessionRequest) (acp.LoadSessionResponse, error) {
	return acp.LoadSessionResponse{}, acp.NewMethodNotFound(acp.AgentMethodSessionLoad)
}
func (a *agent) CloseSession(context.Context, acp.CloseSessionRequest) (acp.CloseSessionResponse, error) {
	return acp.CloseSessionResponse{}, nil
}
func (a *agent) ListSessions(context.Context, acp.ListSessionsRequest) (acp.ListSessionsResponse, error) {
	return acp.ListSessionsResponse{}, acp.NewMethodNotFound(acp.AgentMethodSessionList)
}
func (a *agent) ResumeSession(context.Context, acp.ResumeSessionRequest) (acp.ResumeSessionResponse, error) {
	return acp.ResumeSessionResponse{}, acp.NewMethodNotFound(acp.AgentMethodSessionResume)
}
func (a *agent) SetSessionConfigOption(context.Context, acp.SetSessionConfigOptionRequest) (acp.SetSessionConfigOptionResponse, error) {
	return acp.SetSessionConfigOptionResponse{}, nil
}
func (a *agent) SetSessionMode(context.Context, acp.SetSessionModeRequest) (acp.SetSessionModeResponse, error) {
	return acp.SetSessionModeResponse{}, nil
}
func (a *agent) UnstableForkSession(context.Context, acp.UnstableForkSessionRequest) (acp.UnstableForkSessionResponse, error) {
	return acp.UnstableForkSessionResponse{}, acp.NewMethodNotFound(acp.AgentMethodSessionFork)
}
func (a *agent) UnstableSetSessionModel(context.Context, acp.UnstableSetSessionModelRequest) (acp.UnstableSetSessionModelResponse, error) {
	return acp.UnstableSetSessionModelResponse{}, nil
}

func promptText(params acp.PromptRequest) string {
	for _, block := range params.Prompt {
		if block.Text != nil {
			return block.Text.Text
		}
	}
	return ""
}

func randomHex() string {
	var b [8]byte
	if _, err := io.ReadFull(rand.Reader, b[:]); err != nil {
		return fmt.Sprintf("%d", os.Getpid())
	}
	return hex.EncodeToString(b[:])
}
