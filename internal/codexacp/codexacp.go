package codexacp

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/coder/acp-go-sdk"
)

var errCodexCLI = errors.New("codex cli returned an error")

const (
	backendAppServer = "app-server"
	backendExecJSON  = "exec-json"
)

type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, " ") }
func (m *multiFlag) Set(value string) error {
	*m = append(*m, value)
	return nil
}

type agent struct {
	conn         *acp.AgentSideConnection
	backend      string
	codexCommand string
	execArgs     []string
	appArgs      []string

	mu        sync.Mutex
	sessions  map[acp.SessionId]*sessionState
	active    map[acp.SessionId]*exec.Cmd
	cancelled map[acp.SessionId]bool
}

type sessionState struct {
	cwd      string
	threadID string
	app      *appServerSession
}

type codexEvent struct {
	Type     string          `json:"type"`
	ThreadID string          `json:"thread_id"`
	Message  string          `json:"message"`
	Delta    string          `json:"delta"`
	Text     string          `json:"text"`
	Error    json.RawMessage `json:"error"`
	Item     *codexItem      `json:"item"`
}

type codexItem struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type parseResult struct {
	chunks   []string
	threadID string
	err      error
}

type codexStreamParser struct {
	seenText bool
}

func Run(args []string) error {
	var execExtra multiFlag
	var appExtra multiFlag
	flags := flag.NewFlagSet("codex-cli-acp", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	backend := flags.String("backend", backendAppServer, "Codex backend: app-server or exec-json")
	codexCommand := flags.String("codex", "codex", "Codex CLI command")
	flags.Var(&execExtra, "arg", "Extra argument passed to codex exec, repeatable")
	flags.Var(&appExtra, "app-server-arg", "Extra argument passed to codex app-server, repeatable")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *backend != backendAppServer && *backend != backendExecJSON {
		return fmt.Errorf("unsupported codex backend %q", *backend)
	}

	a := &agent{
		backend:      *backend,
		codexCommand: *codexCommand,
		execArgs:     execExtra,
		appArgs:      appExtra,
		sessions:     map[acp.SessionId]*sessionState{},
		active:       map[acp.SessionId]*exec.Cmd{},
		cancelled:    map[acp.SessionId]bool{},
	}
	conn := acp.NewAgentSideConnection(a, os.Stdout, os.Stdin)
	conn.SetLogger(slog.Default())
	a.conn = conn
	<-conn.Done()
	return nil
}

func (a *agent) Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResponse, error) {
	return acp.InitializeResponse{ProtocolVersion: acp.ProtocolVersionNumber}, nil
}

func (a *agent) NewSession(ctx context.Context, params acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	id := acp.SessionId("sess_" + randomHex())
	state := &sessionState{cwd: params.Cwd}
	if a.backend == backendAppServer {
		app, err := startAppServerSession(ctx, a.codexCommand, a.appArgs, params.Cwd, id, a.conn)
		if err != nil {
			return acp.NewSessionResponse{}, err
		}
		state.app = app
		state.threadID = app.threadID
	}
	a.mu.Lock()
	a.sessions[id] = state
	a.mu.Unlock()
	return acp.NewSessionResponse{SessionId: id}, nil
}

func (a *agent) Prompt(ctx context.Context, params acp.PromptRequest) (acp.PromptResponse, error) {
	state, ok := a.state(params.SessionId)
	if !ok {
		return acp.PromptResponse{}, fmt.Errorf("unknown session %q", params.SessionId)
	}
	text := promptText(params)
	if strings.TrimSpace(text) == "" {
		return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
	}

	if a.backend == backendAppServer {
		if state.app == nil {
			return acp.PromptResponse{}, fmt.Errorf("app-server backend is not initialized for session %q", params.SessionId)
		}
		stopReason, err := state.app.prompt(ctx, text)
		if err != nil {
			return acp.PromptResponse{}, err
		}
		return acp.PromptResponse{StopReason: stopReason}, nil
	}

	result, err := a.runCodex(ctx, params.SessionId, state.cwd, codexArgs(a.execArgs, state.cwd, state.threadID, text))
	if result.threadID != "" {
		a.setThreadID(params.SessionId, result.threadID)
	}
	if err != nil {
		if a.wasCancelled(params.SessionId) || errors.Is(ctx.Err(), context.Canceled) {
			return acp.PromptResponse{StopReason: acp.StopReasonCancelled}, nil
		}
		return acp.PromptResponse{}, err
	}
	return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
}

func (a *agent) runCodex(ctx context.Context, sessionID acp.SessionId, cwd string, args []string) (parseResult, error) {
	cmd := exec.CommandContext(ctx, a.codexCommand, args...)
	cmd.Dir = cwd
	cmd.Env = os.Environ()
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return parseResult{}, err
	}
	if err := cmd.Start(); err != nil {
		return parseResult{}, err
	}

	a.setActive(sessionID, cmd)
	defer a.clearActive(sessionID, cmd)

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	parser := &codexStreamParser{}
	var result parseResult
	for scanner.Scan() {
		parsed, err := a.handleLine(ctx, sessionID, parser, scanner.Bytes())
		if parsed.threadID != "" {
			result.threadID = parsed.threadID
		}
		if err != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			return result, err
		}
	}
	if err := scanner.Err(); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return result, err
	}
	if err := cmd.Wait(); err != nil {
		if a.wasCancelled(sessionID) || errors.Is(ctx.Err(), context.Canceled) {
			return result, nil
		}
		return result, err
	}
	return result, nil
}

func (a *agent) handleLine(ctx context.Context, sessionID acp.SessionId, parser *codexStreamParser, line []byte) (parseResult, error) {
	parsed, err := parser.parseLine(line)
	for _, chunk := range parsed.chunks {
		if err := a.conn.SessionUpdate(ctx, acp.SessionNotification{
			SessionId: sessionID,
			Update:    acp.UpdateAgentMessageText(chunk),
		}); err != nil {
			return parsed, err
		}
	}
	return parsed, err
}

func (p *codexStreamParser) parseLine(line []byte) (parseResult, error) {
	var event codexEvent
	if err := json.Unmarshal(line, &event); err != nil {
		return parseResult{}, nil
	}
	result := parseResult{}
	if event.Type == "thread.started" && event.ThreadID != "" {
		result.threadID = event.ThreadID
		return result, nil
	}
	if event.Type == "error" || len(event.Error) > 0 {
		msg := strings.TrimSpace(firstNonBlank(event.Message, event.Text, string(event.Error)))
		if isTransientCodexStreamError(msg) {
			return result, nil
		}
		if msg != "" {
			result.chunks = []string{msg}
		}
		return result, errCodexCLI
	}
	if chunk := codexTextChunk(event); chunk != "" {
		p.seenText = true
		result.chunks = []string{chunk}
	}
	return result, nil
}

func isTransientCodexStreamError(msg string) bool {
	msg = strings.ToLower(strings.TrimSpace(msg))
	return strings.HasPrefix(msg, "reconnecting...")
}

func codexTextChunk(event codexEvent) string {
	switch event.Type {
	case "agent_message.delta", "message.delta", "response.output_text.delta":
		return firstNonBlank(event.Delta, event.Text, event.Message)
	case "agent_message", "message", "response.output_text.done":
		return firstNonBlank(event.Message, event.Text)
	case "item.completed":
		if event.Item != nil && event.Item.Type == "agent_message" {
			return event.Item.Text
		}
		return ""
	default:
		return ""
	}
}

func (a *agent) Cancel(_ context.Context, params acp.CancelNotification) error {
	if a.backend == backendAppServer {
		state, ok := a.state(params.SessionId)
		if ok && state.app != nil {
			return state.app.cancel(context.Background())
		}
		return nil
	}

	a.mu.Lock()
	cmd := a.active[params.SessionId]
	a.cancelled[params.SessionId] = true
	a.mu.Unlock()
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
	return nil
}

func (a *agent) Authenticate(context.Context, acp.AuthenticateRequest) (acp.AuthenticateResponse, error) {
	return acp.AuthenticateResponse{}, nil
}

func (a *agent) LoadSession(context.Context, acp.LoadSessionRequest) (acp.LoadSessionResponse, error) {
	return acp.LoadSessionResponse{}, acp.NewMethodNotFound(acp.AgentMethodSessionLoad)
}

func (a *agent) CloseSession(_ context.Context, params acp.CloseSessionRequest) (acp.CloseSessionResponse, error) {
	a.mu.Lock()
	state := a.sessions[params.SessionId]
	delete(a.sessions, params.SessionId)
	if cmd := a.active[params.SessionId]; cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
	delete(a.active, params.SessionId)
	delete(a.cancelled, params.SessionId)
	a.mu.Unlock()
	if state != nil && state.app != nil {
		state.app.close()
	}
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

func (a *agent) state(sessionID acp.SessionId) (sessionState, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	state, ok := a.sessions[sessionID]
	if !ok || state == nil {
		return sessionState{}, false
	}
	return *state, true
}

func (a *agent) setThreadID(sessionID acp.SessionId, threadID string) {
	a.mu.Lock()
	if state := a.sessions[sessionID]; state != nil {
		state.threadID = threadID
	}
	a.mu.Unlock()
}

func (a *agent) setActive(sessionID acp.SessionId, cmd *exec.Cmd) {
	a.mu.Lock()
	a.active[sessionID] = cmd
	delete(a.cancelled, sessionID)
	a.mu.Unlock()
}

func (a *agent) clearActive(sessionID acp.SessionId, cmd *exec.Cmd) {
	a.mu.Lock()
	if a.active[sessionID] == cmd {
		delete(a.active, sessionID)
	}
	a.mu.Unlock()
}

func (a *agent) wasCancelled(sessionID acp.SessionId) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cancelled[sessionID]
}

func promptText(params acp.PromptRequest) string {
	var parts []string
	for _, block := range params.Prompt {
		if block.Text != nil && block.Text.Text != "" {
			parts = append(parts, block.Text.Text)
		}
	}
	return strings.Join(parts, "\n\n")
}

func codexArgs(base []string, cwd string, threadID string, text string) []string {
	if threadID == "" {
		args := []string{"exec"}
		args = append(args, base...)
		args = append(args, "--json", "--skip-git-repo-check", "-C", cwd)
		return append(args, text)
	}
	args := []string{"exec", "resume"}
	args = append(args, base...)
	args = append(args, "--json", "--skip-git-repo-check", threadID)
	return append(args, text)
}

func firstNonBlank(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
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
