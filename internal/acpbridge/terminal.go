package acpbridge

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"unicode/utf8"

	"github.com/coder/acp-go-sdk"
)

const defaultTerminalOutputByteLimit = 1024 * 1024

var terminalCounter uint64

type terminalProcess struct {
	id string

	cmd *exec.Cmd

	mu        sync.Mutex
	buf       bytes.Buffer
	limit     int
	truncated bool
	status    *terminalStatus
	waitErr   error
	done      chan struct{}
}

type terminalStatus struct {
	ExitCode *int
	Signal   *string
}

func startTerminalProcess(session *backendSession, params acp.CreateTerminalRequest) (*terminalProcess, error) {
	if params.Command == "" {
		return nil, fmt.Errorf("terminal command is required")
	}
	cwd := session.cwd
	if params.Cwd != nil && *params.Cwd != "" {
		cwd = *params.Cwd
	}
	if !filepath.IsAbs(cwd) {
		abs, err := filepath.Abs(cwd)
		if err != nil {
			return nil, err
		}
		cwd = abs
	}

	env := map[string]string{}
	for _, item := range params.Env {
		env[item.Name] = item.Value
	}
	limit := defaultTerminalOutputByteLimit
	if params.OutputByteLimit != nil {
		limit = *params.OutputByteLimit
	}
	if limit < 0 {
		limit = 0
	}

	cmd := exec.CommandContext(context.Background(), params.Command, params.Args...)
	cmd.Dir = cwd
	cmd.Env = mergeEnv(os.Environ(), env)

	term := &terminalProcess{
		id:    fmt.Sprintf("term_%d", atomic.AddUint64(&terminalCounter, 1)),
		cmd:   cmd,
		limit: limit,
		done:  make(chan struct{}),
	}
	cmd.Stdout = term
	cmd.Stderr = term
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	go term.waitProcess()
	return term, nil
}

func (t *terminalProcess) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := len(p)
	if t.limit == 0 {
		if n > 0 {
			t.truncated = true
		}
		return n, nil
	}
	_, _ = t.buf.Write(p)
	if t.buf.Len() > t.limit {
		trimmed := trimUTF8Start(t.buf.Bytes()[t.buf.Len()-t.limit:])
		t.buf.Reset()
		_, _ = t.buf.Write(trimmed)
		t.truncated = true
	}
	return n, nil
}

func (t *terminalProcess) output() acp.TerminalOutputResponse {
	t.mu.Lock()
	defer t.mu.Unlock()
	resp := acp.TerminalOutputResponse{
		Output:    t.buf.String(),
		Truncated: t.truncated,
	}
	if t.status != nil {
		resp.ExitStatus = &acp.TerminalExitStatus{ExitCode: t.status.ExitCode, Signal: t.status.Signal}
	}
	return resp
}

func (t *terminalProcess) wait(ctx context.Context) (terminalStatus, error) {
	select {
	case <-t.done:
	case <-ctx.Done():
		return terminalStatus{}, ctx.Err()
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.status == nil {
		return terminalStatus{}, t.waitErr
	}
	return *t.status, nil
}

func (t *terminalProcess) kill() {
	if t.cmd != nil && t.cmd.Process != nil {
		_ = t.cmd.Process.Kill()
	}
}

func (t *terminalProcess) waitProcess() {
	err := t.cmd.Wait()
	status := terminalStatusFromProcessState(t.cmd.ProcessState)
	t.mu.Lock()
	t.status = &status
	t.waitErr = err
	t.mu.Unlock()
	close(t.done)
}

func terminalStatusFromProcessState(state *os.ProcessState) terminalStatus {
	if state == nil {
		return terminalStatus{}
	}
	code := state.ExitCode()
	status := terminalStatus{}
	if code >= 0 {
		status.ExitCode = &code
	}
	if waitStatus, ok := state.Sys().(syscall.WaitStatus); ok && waitStatus.Signaled() {
		signal := waitStatus.Signal().String()
		status.Signal = &signal
	}
	return status
}

func trimUTF8Start(data []byte) []byte {
	for len(data) > 0 && !utf8.Valid(data) {
		_, size := utf8.DecodeRune(data)
		if size <= 0 {
			size = 1
		}
		data = data[size:]
	}
	return data
}
