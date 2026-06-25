package acpbridge

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"

	"proxy-acp-codex/internal/config"
	"proxy-acp-codex/internal/platform"
)

type EventSink interface {
	Publish(platform.EventData) error
}

type Manager struct {
	cfg config.Config

	mu       sync.Mutex
	sessions map[string]*backendSession
	awaiting map[string]*pendingPermission
	runs     map[string]*backendSession
	access   map[string]string
}

func NewManager(cfg config.Config) *Manager {
	return &Manager{
		cfg:      cfg,
		sessions: map[string]*backendSession{},
		awaiting: map[string]*pendingPermission{},
		runs:     map[string]*backendSession{},
		access:   map[string]string{},
	}
}

func (m *Manager) Close() {
	m.mu.Lock()
	sessions := make([]*backendSession, 0, len(m.sessions))
	for _, sess := range m.sessions {
		sessions = append(sessions, sess)
	}
	m.sessions = map[string]*backendSession{}
	m.runs = map[string]*backendSession{}
	m.awaiting = map[string]*pendingPermission{}
	m.access = map[string]string{}
	m.mu.Unlock()

	for _, sess := range sessions {
		sess.Close()
	}
}

func (m *Manager) Execute(ctx context.Context, req platform.QueryRequest, sink EventSink) error {
	backend, cwd, model, err := m.resolveBackend(req)
	if err != nil {
		return err
	}
	sess, err := m.session(ctx, backend, req.ChatID, cwd, model)
	if err != nil {
		return err
	}

	turn := newTurn(req, sink, m)
	m.registerRun(req.RunID, sess)
	if model.accessLevel != "" {
		m.setRunAccess(req.RunID, model.accessLevel)
	}
	defer func() {
		m.unregisterRun(req.RunID)
		m.clearRunAccess(req.RunID)
	}()

	return sess.prompt(ctx, req, turn)
}

func (m *Manager) Submit(req platform.SubmitRequest) (platform.SubmitResponse, bool) {
	m.mu.Lock()
	pending := m.awaiting[req.AwaitingID]
	m.mu.Unlock()
	if pending == nil || pending.runID != req.RunID {
		return platform.SubmitResponse{
			Accepted:   false,
			Status:     "unmatched",
			RunID:      req.RunID,
			AwaitingID: req.AwaitingID,
			Detail:     "No active ACP permission request matched this submit",
		}, false
	}

	if pending.mode == "plan" {
		decision := planDecisionFromSubmit(req.Params)
		select {
		case pending.planReply <- decision:
			return platform.SubmitResponse{
				Accepted:   true,
				Status:     "accepted",
				RunID:      req.RunID,
				AwaitingID: req.AwaitingID,
				Detail:     "ACP plan response forwarded",
			}, true
		case <-pending.done:
			return platform.SubmitResponse{
				Accepted:   false,
				Status:     "stale",
				RunID:      req.RunID,
				AwaitingID: req.AwaitingID,
				Detail:     "ACP plan request already completed",
			}, true
		case <-time.After(2 * time.Second):
			return platform.SubmitResponse{
				Accepted:   false,
				Status:     "timeout",
				RunID:      req.RunID,
				AwaitingID: req.AwaitingID,
				Detail:     "Timed out forwarding ACP plan response",
			}, true
		}
	}

	outcome := permissionOutcomeFromSubmit(req.Params, pending.options)
	if pending.mode == "question" {
		outcome = questionOutcomeFromSubmit(req.Params, pending.questions)
	}
	select {
	case pending.response <- outcome:
		return platform.SubmitResponse{
			Accepted:   true,
			Status:     "accepted",
			RunID:      req.RunID,
			AwaitingID: req.AwaitingID,
			Detail:     "ACP permission response forwarded",
		}, true
	case <-pending.done:
		return platform.SubmitResponse{
			Accepted:   false,
			Status:     "stale",
			RunID:      req.RunID,
			AwaitingID: req.AwaitingID,
			Detail:     "ACP permission request already completed",
		}, true
	case <-time.After(2 * time.Second):
		return platform.SubmitResponse{
			Accepted:   false,
			Status:     "timeout",
			RunID:      req.RunID,
			AwaitingID: req.AwaitingID,
			Detail:     "Timed out forwarding ACP permission response",
		}, true
	}
}

func (m *Manager) UpdateAccessLevel(req platform.AccessLevelRequest) (platform.AccessLevelResponse, bool) {
	accessLevel := normalizeAccessLevel(req.AccessLevel)
	if accessLevel == "" {
		return platform.AccessLevelResponse{
			Accepted:    false,
			Status:      "invalid",
			RunID:       req.RunID,
			AccessLevel: req.AccessLevel,
			Detail:      "accessLevel must be default, auto_approve, or full_access",
		}, false
	}

	m.mu.Lock()
	if m.runs[req.RunID] == nil {
		m.mu.Unlock()
		return platform.AccessLevelResponse{
			Accepted:    false,
			Status:      "unmatched",
			RunID:       req.RunID,
			AccessLevel: accessLevel,
			Detail:      "No active ACP run matched access level update",
		}, false
	}
	previous := m.access[req.RunID]
	if previous == "" {
		previous = "default"
	}
	m.access[req.RunID] = accessLevel
	var pending []*pendingPermission
	if accessLevelAllowsAutoApproval(accessLevel) {
		for _, item := range m.awaiting {
			if item.runID == req.RunID && item.mode == "approval" {
				pending = append(pending, item)
			}
		}
	}
	m.mu.Unlock()

	for _, item := range pending {
		outcome := permissionOutcomeFromAccessLevel(accessLevel, item.options)
		select {
		case item.response <- outcome:
		case <-item.done:
		default:
		}
	}

	return platform.AccessLevelResponse{
		Accepted:            true,
		Status:              "updated",
		RunID:               req.RunID,
		PreviousAccessLevel: previous,
		AccessLevel:         accessLevel,
		Detail:              "accessLevel updated",
	}, true
}

func (m *Manager) Steer(req platform.SteerRequest) platform.SteerResponse {
	req.SteerID = normalizeSteerID(req.SteerID)
	m.mu.Lock()
	sess := m.runs[req.RunID]
	m.mu.Unlock()
	if sess == nil {
		return platform.SteerResponse{Accepted: false, Status: "unmatched", RunID: req.RunID, SteerID: req.SteerID, Detail: "No active ACP run matched steer"}
	}
	t := sess.activeTurn()
	if t == nil {
		return platform.SteerResponse{Accepted: false, Status: "unmatched", RunID: req.RunID, SteerID: req.SteerID, Detail: "ACP run is not accepting steer"}
	}
	return t.enqueueSteer(req)
}

func (m *Manager) Interrupt(req platform.InterruptRequest) platform.InterruptResponse {
	m.mu.Lock()
	sess := m.runs[req.RunID]
	m.mu.Unlock()
	if sess == nil {
		return platform.InterruptResponse{Accepted: false, Status: "unmatched", RunID: req.RunID, Detail: "No active ACP run matched interrupt"}
	}
	if err := sess.cancel(context.Background()); err != nil {
		return platform.InterruptResponse{Accepted: false, Status: "error", RunID: req.RunID, Detail: err.Error()}
	}
	return platform.InterruptResponse{Accepted: true, Status: "accepted", RunID: req.RunID, Detail: "ACP session cancel forwarded"}
}

func (m *Manager) registerAwaiting(p *pendingPermission) {
	m.mu.Lock()
	m.awaiting[p.awaitingID] = p
	m.mu.Unlock()
}

func (m *Manager) unregisterAwaiting(awaitingID string, p *pendingPermission) {
	m.mu.Lock()
	if m.awaiting[awaitingID] == p {
		delete(m.awaiting, awaitingID)
	}
	m.mu.Unlock()
}

func (m *Manager) registerRun(runID string, sess *backendSession) {
	m.mu.Lock()
	m.runs[runID] = sess
	m.mu.Unlock()
}

func (m *Manager) unregisterRun(runID string) {
	m.mu.Lock()
	delete(m.runs, runID)
	m.mu.Unlock()
}

func (m *Manager) setRunAccess(runID string, accessLevel string) {
	runID = strings.TrimSpace(runID)
	accessLevel = normalizeAccessLevel(accessLevel)
	if runID == "" || accessLevel == "" {
		return
	}
	m.mu.Lock()
	m.access[runID] = accessLevel
	m.mu.Unlock()
}

func (m *Manager) clearRunAccess(runID string) {
	m.mu.Lock()
	delete(m.access, runID)
	m.mu.Unlock()
}

func (m *Manager) runAccessLevel(runID string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return normalizeAccessLevel(m.access[runID])
}

func normalizeSteerID(steerID string) string {
	if strings.TrimSpace(steerID) != "" {
		return strings.TrimSpace(steerID)
	}
	return time.Now().UTC().Format("20060102150405.000000000")
}

func (m *Manager) resolveBackend(req platform.QueryRequest) (config.BackendConfig, string, codexSessionOptions, error) {
	key := stringParam(req.Params, "backend")
	if key == "" {
		key = strings.TrimSpace(req.AgentKey)
	}
	backend, ok := m.cfg.Backend(key)
	if !ok {
		backend, ok = m.cfg.Backend("")
	}
	if !ok {
		return config.BackendConfig{}, "", codexSessionOptions{}, fmt.Errorf("backend %q not configured", key)
	}
	cwd := stringParam(req.Params, "cwd")
	if cwd == "" {
		return config.BackendConfig{}, "", codexSessionOptions{}, fmt.Errorf("params.cwd is required; agent-platform must provide the ACP session working directory")
	}
	abs, err := filepath.Abs(cwd)
	if err != nil {
		return config.BackendConfig{}, "", codexSessionOptions{}, err
	}
	return backend, abs, requestCodexSessionOptions(req), nil
}

type codexSessionOptions struct {
	model           string
	reasoningEffort string
	serviceTier     string
	accessLevel     string
	approvalPolicy  string
	sandboxMode     string
}

func requestCodexSessionOptions(req platform.QueryRequest) codexSessionOptions {
	opts := codexSessionOptions{
		model:           requestModel(req),
		reasoningEffort: requestReasoningEffort(req),
		serviceTier:     requestServiceTier(req),
	}
	opts.accessLevel = requestAccessLevel(req)
	opts.approvalPolicy, opts.sandboxMode = codexAccessPolicy(opts.accessLevel)
	if policy := strings.TrimSpace(stringParam(req.Params, "approvalPolicy")); policy != "" {
		opts.approvalPolicy = policy
	}
	if sandbox := strings.TrimSpace(firstNonBlank(stringParam(req.Params, "sandboxMode"), stringParam(req.Params, "sandbox"))); sandbox != "" {
		opts.sandboxMode = sandbox
	}
	return opts
}

func requestModel(req platform.QueryRequest) string {
	if req.Model == nil {
		return strings.TrimSpace(firstNonBlank(
			stringParam(req.Params, "modelId"),
			stringParam(req.Params, "modelKey"),
			stringParam(req.Params, "model"),
		))
	}
	if modelID := strings.TrimSpace(req.Model.ModelID); modelID != "" {
		return modelID
	}
	return strings.TrimSpace(req.Model.Key)
}

func requestReasoningEffort(req platform.QueryRequest) string {
	if req.Model == nil {
		return normalizeReasoningEffort(stringParam(req.Params, "reasoningEffort"))
	}
	return normalizeReasoningEffort(req.Model.ReasoningEffort)
}

func normalizeReasoningEffort(value string) string {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "LOW":
		return "low"
	case "MEDIUM":
		return "medium"
	case "HIGH":
		return "high"
	case "XHIGH", "EXTRA_HIGH":
		return "xhigh"
	case "NONE":
		return ""
	default:
		return ""
	}
}

func requestServiceTier(req platform.QueryRequest) string {
	if req.Model == nil {
		return normalizeServiceTier(stringParam(req.Params, "serviceTier"))
	}
	return normalizeServiceTier(req.Model.ServiceTier)
}

func normalizeServiceTier(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "standard", "default", "auto":
		return ""
	case "fast", "priority":
		return "fast"
	case "flex":
		return "flex"
	default:
		return ""
	}
}

func requestAccessLevel(req platform.QueryRequest) string {
	accessLevel := strings.TrimSpace(req.AccessLevel)
	if accessLevel == "" {
		accessLevel = stringParam(req.Params, "accessLevel")
	}
	return normalizeAccessLevel(accessLevel)
}

func normalizeAccessLevel(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return ""
	case "default":
		return "default"
	case "auto_approve", "auto-approve", "autoapprove", "full_auto", "full-auto":
		return "auto_approve"
	case "full_access", "full-access", "fullaccess", "danger_full_access", "danger-full-access":
		return "full_access"
	default:
		return ""
	}
}

func codexAccessPolicy(accessLevel string) (string, string) {
	switch normalizeAccessLevel(accessLevel) {
	case "default":
		return "on-request", "workspace-write"
	case "auto_approve":
		return "on-failure", "workspace-write"
	case "full_access":
		return "never", "danger-full-access"
	default:
		return "", ""
	}
}

func accessLevelAllowsAutoApproval(accessLevel string) bool {
	switch normalizeAccessLevel(accessLevel) {
	case "auto_approve", "full_access":
		return true
	default:
		return false
	}
}

func permissionOutcomeFromAccessLevel(accessLevel string, options []acp.PermissionOption) acp.RequestPermissionOutcome {
	switch normalizeAccessLevel(accessLevel) {
	case "":
		return acp.RequestPermissionOutcome{Cancelled: &acp.RequestPermissionOutcomeCancelled{}}
	case "full_access":
		if id := permissionOptionIDByPrefix(options, "decision_json:"); id != "" {
			return acp.RequestPermissionOutcome{Selected: &acp.RequestPermissionOutcomeSelected{OptionId: acp.PermissionOptionId(id)}}
		}
		return permissionOutcomeFromSubmit([]map[string]any{{"decision": "approve"}}, options)
	case "auto_approve":
		return permissionOutcomeFromSubmit([]map[string]any{{"decision": "approve_rule_run"}}, options)
	default:
		return acp.RequestPermissionOutcome{Cancelled: &acp.RequestPermissionOutcomeCancelled{}}
	}
}

func permissionOptionIDByPrefix(options []acp.PermissionOption, prefix string) string {
	for _, option := range options {
		if strings.HasPrefix(string(option.OptionId), prefix) {
			return string(option.OptionId)
		}
	}
	return ""
}

func backendArgsWithModel(base []string, model string) []string {
	return backendArgsWithSessionOptions(base, codexSessionOptions{model: model})
}

func backendArgsWithModelOptions(base []string, opts codexSessionOptions) []string {
	return backendArgsWithSessionOptions(base, opts)
}

func backendArgsWithSessionOptions(base []string, opts codexSessionOptions) []string {
	args := append([]string(nil), base...)
	model := strings.TrimSpace(opts.model)
	if model == "" {
		if strings.TrimSpace(opts.reasoningEffort) == "" && strings.TrimSpace(opts.serviceTier) == "" && strings.TrimSpace(opts.approvalPolicy) == "" && strings.TrimSpace(opts.sandboxMode) == "" {
			return args
		}
	} else {
		args = append(args, "-model", model)
	}
	reasoningEffort := strings.TrimSpace(opts.reasoningEffort)
	if reasoningEffort != "" {
		args = append(args, "-model-reasoning-effort", reasoningEffort)
	}
	if serviceTier := strings.TrimSpace(opts.serviceTier); serviceTier != "" {
		args = append(args, "-service-tier", serviceTier)
	}
	if approvalPolicy := strings.TrimSpace(opts.approvalPolicy); approvalPolicy != "" {
		args = append(args, "-approval-policy", approvalPolicy)
	}
	if sandboxMode := strings.TrimSpace(opts.sandboxMode); sandboxMode != "" {
		args = append(args, "-sandbox-mode", sandboxMode)
	}
	return args
}

func (m *Manager) session(ctx context.Context, backend config.BackendConfig, chatID string, cwd string, opts codexSessionOptions) (*backendSession, error) {
	if strings.TrimSpace(chatID) == "" {
		chatID = "default"
	}
	key := backend.Key + "\x00" + chatID + "\x00" + cwd + "\x00" + opts.model + "\x00" + opts.reasoningEffort + "\x00" + opts.serviceTier + "\x00" + opts.approvalPolicy + "\x00" + opts.sandboxMode

	m.mu.Lock()
	if existing := m.sessions[key]; existing != nil && existing.alive() {
		m.mu.Unlock()
		return existing, nil
	}
	m.mu.Unlock()

	sess, err := startBackendSession(ctx, backend, chatID, cwd, opts)
	if err != nil {
		return nil, err
	}
	sess.onExit = func() {
		m.mu.Lock()
		if m.sessions[key] == sess {
			delete(m.sessions, key)
		}
		m.mu.Unlock()
	}

	m.mu.Lock()
	if existing := m.sessions[key]; existing != nil && existing.alive() {
		m.mu.Unlock()
		sess.Close()
		return existing, nil
	}
	m.sessions[key] = sess
	m.mu.Unlock()
	return sess, nil
}

type backendSession struct {
	cfg       config.BackendConfig
	chatID    string
	cwd       string
	options   codexSessionOptions
	sessionID acp.SessionId
	cmd       *exec.Cmd
	conn      *acp.ClientSideConnection
	client    *bridgeClient
	onExit    func()

	promptMu sync.Mutex
	mu       sync.Mutex
	active   *turn
	closed   bool
}

func startBackendSession(ctx context.Context, cfg config.BackendConfig, chatID string, cwd string, opts codexSessionOptions) (*backendSession, error) {
	command, err := backendCommandPath(cfg.Command)
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(context.Background(), command, backendArgsWithSessionOptions(cfg.Args, opts)...)
	cmd.Dir = cwd
	cmd.Env = mergeEnv(os.Environ(), cfg.Env)
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
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	sess := &backendSession{cfg: cfg, chatID: chatID, cwd: cwd, options: opts, cmd: cmd}
	client := &bridgeClient{session: sess, terminals: map[string]*terminalProcess{}}
	conn := acp.NewClientSideConnection(client, stdin, stdout)
	conn.SetLogger(slog.Default())
	sess.client = client
	sess.conn = conn

	go func() {
		err := cmd.Wait()
		if err != nil && !sess.isClosed() {
			log.Printf("[proxy-acp-codex][backend:%s] exited: %v", cfg.Key, err)
		}
		if sess.onExit != nil {
			sess.onExit()
		}
	}()

	initCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if _, err := conn.Initialize(initCtx, acp.InitializeRequest{
		ProtocolVersion: acp.ProtocolVersionNumber,
		ClientInfo:      &acp.Implementation{Name: "proxy-acp-codex", Version: "0.1.0"},
		ClientCapabilities: acp.ClientCapabilities{
			Fs: acp.FileSystemCapabilities{
				ReadTextFile:  cfg.Capabilities.ReadTextFile(),
				WriteTextFile: cfg.Capabilities.WriteTextFile(),
			},
			Terminal: cfg.Capabilities.TerminalEnabled(),
		},
	}); err != nil {
		sess.Close()
		return nil, fmt.Errorf("initialize ACP backend %q: %w", cfg.Key, err)
	}
	newSession, err := conn.NewSession(initCtx, acp.NewSessionRequest{Cwd: cwd, McpServers: []acp.McpServer{}})
	if err != nil {
		sess.Close()
		return nil, fmt.Errorf("create ACP session for backend %q: %w", cfg.Key, err)
	}
	sess.sessionID = newSession.SessionId
	return sess, nil
}

func backendCommandPath(command string) (string, error) {
	if command == config.SelfBackendCommand {
		executable, err := os.Executable()
		if err != nil {
			return "", fmt.Errorf("resolve current executable: %w", err)
		}
		return executable, nil
	}
	if !strings.Contains(command, "/") && !strings.Contains(command, `\`) {
		return command, nil
	}
	absolute, err := filepath.Abs(command)
	if err != nil {
		return "", fmt.Errorf("resolve backend command %q: %w", command, err)
	}
	return absolute, nil
}

func (s *backendSession) prompt(ctx context.Context, req platform.QueryRequest, t *turn) error {
	s.promptMu.Lock()
	defer s.promptMu.Unlock()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return fmt.Errorf("backend session is closed")
	}
	s.active = t
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if s.active == t {
			s.active = nil
		}
		s.mu.Unlock()
	}()

	queryPayload := map[string]any{
		"requestId":   req.RequestID,
		"runId":       req.RunID,
		"chatId":      req.ChatID,
		"agentKey":    req.AgentKey,
		"role":        req.Role,
		"message":     req.Message,
		"references":  req.References,
		"params":      req.Params,
		"accessLevel": req.AccessLevel,
		"model":       req.Model,
		"scene":       req.Scene,
	}
	if req.PlanningMode != nil {
		queryPayload["planningMode"] = *req.PlanningMode
	}
	t.emit("request.query", queryPayload)
	t.emit("run.start", map[string]any{"runId": req.RunID, "chatId": req.ChatID, "agentKey": req.AgentKey})

	prompt, err := promptBlocks(req)
	if err != nil {
		t.emitRunError(err)
		return err
	}

	currentReq := req
	resp, err := s.conn.Prompt(ctx, acp.PromptRequest{SessionId: s.sessionID, Prompt: prompt, Meta: promptRequestMeta(currentReq)})
	for {
		t.closeOpenStreams()
		if err != nil {
			t.emitRunError(err)
			return err
		}
		if resp.StopReason == acp.StopReasonCancelled {
			break
		}
		if planningModeEnabled(currentReq) {
			if planningID, planText, ok := t.pendingPlanApproval(); ok {
				decision, accepted := t.requestPlanApproval(ctx, planningID, planText)
				if !accepted {
					break
				}
				steerReq := planContinuationRequest(currentReq, decision, planText)
				t.emit("request.steer", map[string]any{
					"requestId": steerReq.RequestID,
					"chatId":    steerReq.ChatID,
					"runId":     t.req.RunID,
					"steerId":   stableID(t.req.RunID, "plan_"+planningID+"_"+decision.Decision),
					"message":   steerReq.Message,
				})
				prompt, err = promptBlocks(steerReq)
				if err != nil {
					t.emitRunError(err)
					return err
				}
				currentReq = steerReq
				resp, err = s.conn.Prompt(ctx, acp.PromptRequest{SessionId: s.sessionID, Prompt: prompt, Meta: promptRequestMeta(currentReq)})
				continue
			}
		}
		steer, ok := t.nextSteer()
		if !ok {
			break
		}
		steerReq := currentReq
		steerReq.RequestID = steer.RequestID
		steerReq.ChatID = firstNonBlank(steer.ChatID, currentReq.ChatID, req.ChatID)
		steerReq.AgentKey = firstNonBlank(steer.AgentKey, currentReq.AgentKey, req.AgentKey)
		steerReq.TeamID = firstNonBlank(steer.TeamID, currentReq.TeamID, req.TeamID)
		steerReq.Message = steer.Message
		steerReq.References = nil
		prompt, err = promptBlocks(steerReq)
		if err != nil {
			t.emitRunError(err)
			return err
		}
		currentReq = steerReq
		resp, err = s.conn.Prompt(ctx, acp.PromptRequest{SessionId: s.sessionID, Prompt: prompt, Meta: promptRequestMeta(currentReq)})
	}

	switch resp.StopReason {
	case acp.StopReasonCancelled:
		t.emit("run.cancel", map[string]any{"runId": req.RunID, "usage": usagePayload(resp.Usage)})
	default:
		t.emit("run.complete", map[string]any{"runId": req.RunID, "finishReason": string(resp.StopReason), "usage": usagePayload(resp.Usage)})
	}
	return nil
}

func promptRequestMeta(req platform.QueryRequest) map[string]any {
	if req.PlanningMode == nil {
		return nil
	}
	return map[string]any{platform.PromptMetaPlanningMode: *req.PlanningMode}
}

func planningModeEnabled(req platform.QueryRequest) bool {
	return req.PlanningMode != nil && *req.PlanningMode
}

func planContinuationRequest(base platform.QueryRequest, decision planDecision, planText string) platform.QueryRequest {
	next := base
	next.RequestID = stableID(base.RunID, "plan_"+decision.Decision+"_"+time.Now().UTC().Format("20060102150405.000000000"))
	next.References = nil
	if decision.Decision == "approve" {
		disabled := false
		next.PlanningMode = &disabled
		next.Message = fmt.Sprintf("用户已经批准计划。请基于已确认计划继续执行，不要再次请求确认。\n\n已确认计划：\n%s", strings.TrimSpace(planText))
		return next
	}
	enabled := true
	next.PlanningMode = &enabled
	reason := strings.TrimSpace(decision.Reason)
	if reason == "" {
		reason = "未提供具体修改意见。"
	}
	next.Message = fmt.Sprintf("用户已经拒绝计划。请根据反馈修订方案或给出下一步，不要执行被拒绝的计划。\n\n反馈：%s\n\n被拒绝的计划：\n%s", reason, strings.TrimSpace(planText))
	return next
}

func (s *backendSession) cancel(ctx context.Context) error {
	s.mu.Lock()
	sessionID := s.sessionID
	active := s.active
	s.mu.Unlock()
	if sessionID == "" {
		return fmt.Errorf("ACP session is not initialized")
	}
	if active != nil {
		active.interrupt()
	}
	return s.conn.Cancel(ctx, acp.CancelNotification{SessionId: sessionID})
}

func (s *backendSession) activeTurn() *turn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active
}

func (s *backendSession) alive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.cmd == nil || s.cmd.Process == nil {
		return false
	}
	select {
	case <-s.conn.Done():
		return false
	default:
		return true
	}
}

func (s *backendSession) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	cmd := s.cmd
	s.mu.Unlock()
	if s.client != nil {
		s.client.closeTerminals()
	}
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}

func (s *backendSession) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

type bridgeClient struct {
	session *backendSession

	terminalMu sync.Mutex
	terminals  map[string]*terminalProcess
}

func (c *bridgeClient) SessionUpdate(_ context.Context, params acp.SessionNotification) error {
	t := c.session.activeTurn()
	if t == nil {
		return nil
	}
	return t.handleUpdate(params.Update)
}

func (c *bridgeClient) RequestPermission(ctx context.Context, params acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	t := c.session.activeTurn()
	if t == nil {
		return acp.RequestPermissionResponse{}, fmt.Errorf("permission request without active turn")
	}
	return t.requestPermission(ctx, params)
}

func (c *bridgeClient) ReadTextFile(_ context.Context, params acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	if !c.session.cfg.Capabilities.ReadTextFile() {
		return acp.ReadTextFileResponse{}, fmt.Errorf("backend %q is not allowed to read files", c.session.cfg.Key)
	}
	if !filepath.IsAbs(params.Path) {
		return acp.ReadTextFileResponse{}, fmt.Errorf("path must be absolute: %s", params.Path)
	}
	data, err := os.ReadFile(params.Path)
	if err != nil {
		return acp.ReadTextFileResponse{}, err
	}
	text := string(data)
	if params.Line != nil || params.Limit != nil {
		text = sliceTextLines(text, params.Line, params.Limit)
	}
	return acp.ReadTextFileResponse{Content: text}, nil
}

func (c *bridgeClient) WriteTextFile(_ context.Context, params acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	if !c.session.cfg.Capabilities.WriteTextFile() {
		return acp.WriteTextFileResponse{}, fmt.Errorf("backend %q is not allowed to write files", c.session.cfg.Key)
	}
	if !filepath.IsAbs(params.Path) {
		return acp.WriteTextFileResponse{}, fmt.Errorf("path must be absolute: %s", params.Path)
	}
	if err := os.WriteFile(params.Path, []byte(params.Content), 0o644); err != nil {
		return acp.WriteTextFileResponse{}, err
	}
	return acp.WriteTextFileResponse{}, nil
}

func (c *bridgeClient) CreateTerminal(ctx context.Context, params acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error) {
	if !c.session.cfg.Capabilities.TerminalEnabled() {
		return acp.CreateTerminalResponse{}, fmt.Errorf("backend %q is not allowed to create terminals", c.session.cfg.Key)
	}
	if err := c.requestTerminalApproval(ctx, params); err != nil {
		return acp.CreateTerminalResponse{}, err
	}
	term, err := startTerminalProcess(c.session, params)
	if err != nil {
		return acp.CreateTerminalResponse{}, err
	}
	c.terminalMu.Lock()
	if c.terminals == nil {
		c.terminals = map[string]*terminalProcess{}
	}
	c.terminals[term.id] = term
	c.terminalMu.Unlock()
	return acp.CreateTerminalResponse{TerminalId: term.id}, nil
}

func (c *bridgeClient) requestTerminalApproval(ctx context.Context, params acp.CreateTerminalRequest) error {
	t := c.session.activeTurn()
	if t == nil {
		return fmt.Errorf("terminal request without active turn")
	}
	kind := acp.ToolKindExecute
	status := acp.ToolCallStatusPending
	command := terminalCommandText(params)
	title := "Run command: " + command
	resp, err := t.requestPermission(ctx, acp.RequestPermissionRequest{
		SessionId: c.session.sessionID,
		ToolCall: acp.ToolCallUpdate{
			ToolCallId: nextTerminalApprovalID(),
			Title:      &title,
			Kind:       &kind,
			Status:     &status,
			RawInput:   terminalRawInput(c.session, params),
		},
		Options: []acp.PermissionOption{
			{OptionId: "allow", Name: "Allow", Kind: acp.PermissionOptionKindAllowOnce},
			{OptionId: "reject", Name: "Reject", Kind: acp.PermissionOptionKindRejectOnce},
		},
	})
	if err != nil {
		return err
	}
	if resp.Outcome.Selected != nil && resp.Outcome.Selected.OptionId == "allow" {
		return nil
	}
	return fmt.Errorf("terminal command rejected by user")
}

func (c *bridgeClient) KillTerminal(_ context.Context, params acp.KillTerminalRequest) (acp.KillTerminalResponse, error) {
	term, err := c.terminal(params.TerminalId)
	if err != nil {
		return acp.KillTerminalResponse{}, err
	}
	term.kill()
	return acp.KillTerminalResponse{}, nil
}

func (c *bridgeClient) TerminalOutput(_ context.Context, params acp.TerminalOutputRequest) (acp.TerminalOutputResponse, error) {
	term, err := c.terminal(params.TerminalId)
	if err != nil {
		return acp.TerminalOutputResponse{}, err
	}
	return term.output(), nil
}

func (c *bridgeClient) ReleaseTerminal(_ context.Context, params acp.ReleaseTerminalRequest) (acp.ReleaseTerminalResponse, error) {
	term, err := c.removeTerminal(params.TerminalId)
	if err != nil {
		return acp.ReleaseTerminalResponse{}, err
	}
	term.kill()
	return acp.ReleaseTerminalResponse{}, nil
}

func (c *bridgeClient) WaitForTerminalExit(ctx context.Context, params acp.WaitForTerminalExitRequest) (acp.WaitForTerminalExitResponse, error) {
	term, err := c.terminal(params.TerminalId)
	if err != nil {
		return acp.WaitForTerminalExitResponse{}, err
	}
	status, err := term.wait(ctx)
	if err != nil {
		return acp.WaitForTerminalExitResponse{}, err
	}
	return acp.WaitForTerminalExitResponse{ExitCode: status.ExitCode, Signal: status.Signal}, nil
}

func (c *bridgeClient) terminal(id string) (*terminalProcess, error) {
	c.terminalMu.Lock()
	defer c.terminalMu.Unlock()
	term := c.terminals[id]
	if term == nil {
		return nil, fmt.Errorf("terminal %q not found", id)
	}
	return term, nil
}

func (c *bridgeClient) removeTerminal(id string) (*terminalProcess, error) {
	c.terminalMu.Lock()
	defer c.terminalMu.Unlock()
	term := c.terminals[id]
	if term == nil {
		return nil, fmt.Errorf("terminal %q not found", id)
	}
	delete(c.terminals, id)
	return term, nil
}

func (c *bridgeClient) closeTerminals() {
	c.terminalMu.Lock()
	terms := make([]*terminalProcess, 0, len(c.terminals))
	for _, term := range c.terminals {
		terms = append(terms, term)
	}
	c.terminals = map[string]*terminalProcess{}
	c.terminalMu.Unlock()
	for _, term := range terms {
		term.kill()
	}
}

func promptBlocks(req platform.QueryRequest) ([]acp.ContentBlock, error) {
	blocks := []acp.ContentBlock{acp.TextBlock(req.Message)}
	for _, ref := range req.References {
		name := strings.TrimSpace(ref.Name)
		if name == "" {
			name = strings.TrimSpace(ref.ID)
		}
		if name == "" {
			name = "reference"
		}
		uri := strings.TrimSpace(ref.URL)
		if uri == "" && ref.SandboxPath != "" {
			abs, err := filepath.Abs(ref.SandboxPath)
			if err != nil {
				return nil, err
			}
			uri = (&url.URL{Scheme: "file", Path: abs}).String()
		}
		if uri == "" {
			continue
		}
		block := acp.ResourceLinkBlock(name, uri)
		if ref.MimeType != "" {
			block.ResourceLink.MimeType = acp.Ptr(ref.MimeType)
		}
		if ref.SizeBytes != nil {
			size := int(*ref.SizeBytes)
			block.ResourceLink.Size = &size
		}
		blocks = append(blocks, block)
	}
	return blocks, nil
}

func mergeEnv(base []string, overrides map[string]string) []string {
	out := append([]string(nil), base...)
	for key, value := range overrides {
		prefix := key + "="
		replaced := false
		for idx := range out {
			if strings.HasPrefix(out[idx], prefix) {
				out[idx] = prefix + value
				replaced = true
				break
			}
		}
		if !replaced {
			out = append(out, prefix+value)
		}
	}
	return out
}

func stringParam(params map[string]any, key string) string {
	if params == nil {
		return ""
	}
	value, ok := params[key]
	if !ok {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	default:
		return strings.TrimSpace(fmt.Sprint(typed))
	}
}

func usagePayload(usage *acp.Usage) map[string]any {
	if usage == nil {
		return nil
	}
	out := map[string]any{
		"promptTokens":     usage.InputTokens,
		"completionTokens": usage.OutputTokens,
		"totalTokens":      usage.TotalTokens,
	}
	if usage.ThoughtTokens != nil {
		out["reasoningTokens"] = *usage.ThoughtTokens
	}
	if usage.CachedReadTokens != nil {
		out["cachedReadTokens"] = *usage.CachedReadTokens
	}
	if usage.CachedWriteTokens != nil {
		out["cachedWriteTokens"] = *usage.CachedWriteTokens
	}
	return out
}

func sliceTextLines(content string, line *int, limit *int) string {
	lines := strings.Split(content, "\n")
	start := 0
	if line != nil && *line > 0 {
		start = *line - 1
	}
	if start > len(lines) {
		start = len(lines)
	}
	end := len(lines)
	if limit != nil && *limit >= 0 && start+*limit < end {
		end = start + *limit
	}
	return strings.Join(lines[start:end], "\n")
}

func marshalAny(value any) string {
	if value == nil {
		return ""
	}
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	return string(data)
}
