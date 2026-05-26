package codexacp

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

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

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}
