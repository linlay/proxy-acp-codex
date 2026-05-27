package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadDefaultsToLocalCodexBackend(t *testing.T) {
	withCleanEnv(t)
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.ListenAddr != "127.0.0.1:17071" {
		t.Fatalf("listen addr = %q", cfg.ListenAddr)
	}
	if cfg.AuthToken != "" {
		t.Fatalf("auth token = %q, want empty", cfg.AuthToken)
	}
	backend := cfg.Backends[0]
	if backend.Key != DefaultCodexBackendKey {
		t.Fatalf("backend key = %q", backend.Key)
	}
	if backend.Command != SelfBackendCommand {
		t.Fatalf("backend command = %q", backend.Command)
	}
	if got, want := backend.Args, []string{CodexBackendModeArg, "-backend", "app-server", "-codex", "codex"}; strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("args = %#v, want %#v", got, want)
	}
	if !backend.Capabilities.ReadTextFile() || backend.Capabilities.WriteTextFile() || backend.Capabilities.TerminalEnabled() {
		t.Fatalf("codex capabilities should default to read-only: %#v", backend.Capabilities)
	}
}

func TestLoadAppliesEnvOverrides(t *testing.T) {
	withCleanEnv(t)
	t.Setenv("PROXY_ACP_PORT", "4567")
	t.Setenv("PROXY_ACP_ADDR", "0.0.0.0")
	t.Setenv("PROXY_ACP_AUTH_TOKEN", "secret")
	t.Setenv("CODEX_CLI", "/opt/bin/codex")
	t.Setenv("CODEX_APP_SERVER_ARGS", `--enable network_proxy -c "model=gpt-5"`)
	t.Setenv("PROXY_ACP_IDLE_TIMEOUT_MS", "42")
	t.Setenv("http_proxy", "http://127.0.0.1:8001")
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:8001")
	t.Setenv("https_proxy", "http://127.0.0.1:8001")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:8001")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.ListenAddr != "0.0.0.0:4567" {
		t.Fatalf("listen addr = %q", cfg.ListenAddr)
	}
	if cfg.AuthToken != "secret" {
		t.Fatalf("auth token = %q", cfg.AuthToken)
	}
	backend := cfg.Backends[0]
	if backend.Command != SelfBackendCommand {
		t.Fatalf("backend command = %q", backend.Command)
	}
	if got, want := backend.Args, []string{
		CodexBackendModeArg,
		"-backend", "app-server",
		"-codex", "/opt/bin/codex",
		"-app-server-arg", "--enable",
		"-app-server-arg", "network_proxy",
		"-app-server-arg", "-c",
		"-app-server-arg", "model=gpt-5",
	}; strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("args = %#v, want %#v", got, want)
	}
	if backend.IdleTimeoutMs != 42 {
		t.Fatalf("idle timeout = %d", backend.IdleTimeoutMs)
	}
	for _, key := range []string{"http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY"} {
		if got := backend.Env[key]; got != "http://127.0.0.1:8001" {
			t.Fatalf("backend env %s = %q", key, got)
		}
	}
}

func TestLoadDotenvAndProcessEnvPrecedence(t *testing.T) {
	withCleanEnv(t)
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	if err := os.WriteFile(path, []byte(strings.Join([]string{
		"PROXY_ACP_PORT=5555",
		"PROXY_ACP_ADDR=0.0.0.0",
		"PROXY_ACP_AUTH_TOKEN=from_file",
		"CODEX_CLI=/file/codex",
		"CODEX_APP_SERVER_ARGS=\"--enable network_proxy\"",
		"PROXY_ACP_IDLE_TIMEOUT_MS=99",
		"HTTP_PROXY=http://127.0.0.1:8001",
		"",
	}, "\n")), 0o600); err != nil {
		t.Fatalf("write dotenv: %v", err)
	}
	t.Setenv("PROXY_ACP_AUTH_TOKEN", "from_env")
	t.Setenv("CODEX_CLI", "/env/codex")
	t.Setenv("CODEX_APP_SERVER_ARGS", "--enable network_proxy -c model=gpt-5")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.ListenAddr != "0.0.0.0:5555" {
		t.Fatalf("listen addr = %q", cfg.ListenAddr)
	}
	if cfg.AuthToken != "from_env" {
		t.Fatalf("auth token = %q", cfg.AuthToken)
	}
	if got := cfg.Backends[0].Command; got != SelfBackendCommand {
		t.Fatalf("codex cli acp = %q", got)
	}
	if got := cfg.Backends[0].Args[4]; got != "/env/codex" {
		t.Fatalf("codex cli = %q", got)
	}
	if got, want := cfg.Backends[0].Args[5:], []string{"-app-server-arg", "--enable", "-app-server-arg", "network_proxy", "-app-server-arg", "-c", "-app-server-arg", "model=gpt-5"}; strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("extra args = %#v, want %#v", got, want)
	}
	if cfg.Backends[0].IdleTimeoutMs != 99 {
		t.Fatalf("idle timeout = %d", cfg.Backends[0].IdleTimeoutMs)
	}
	if got := cfg.Backends[0].Env["HTTP_PROXY"]; got != "http://127.0.0.1:8001" {
		t.Fatalf("backend HTTP_PROXY = %q", got)
	}
}

func TestLoadRejectsInvalidAppServerArgs(t *testing.T) {
	withCleanEnv(t)
	t.Setenv("CODEX_APP_SERVER_ARGS", `"unterminated`)
	_, err := Load("")
	if err == nil || !strings.Contains(err.Error(), "CODEX_APP_SERVER_ARGS") {
		t.Fatalf("error = %v, want CODEX_APP_SERVER_ARGS error", err)
	}
}

func TestLoadExecJSONBackendUsesCodexArgs(t *testing.T) {
	withCleanEnv(t)
	t.Setenv("CODEX_BACKEND", "exec-json")
	t.Setenv("CODEX_ARGS", `--permission-mode dontAsk --label "hello world"`)

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got, want := cfg.Backends[0].Args, []string{
		CodexBackendModeArg,
		"-backend", "exec-json",
		"-codex", "codex",
		"-arg", "--permission-mode",
		"-arg", "dontAsk",
		"-arg", "--label",
		"-arg", "hello world",
	}; strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("args = %#v, want %#v", got, want)
	}
}

func TestLoadRejectsInvalidPort(t *testing.T) {
	withCleanEnv(t)
	t.Setenv("PROXY_ACP_PORT", "70000")
	_, err := Load("")
	if err == nil || !strings.Contains(err.Error(), "PROXY_ACP_PORT") {
		t.Fatalf("error = %v, want port error", err)
	}
}

func TestLoadRejectsInvalidIdleTimeout(t *testing.T) {
	withCleanEnv(t)
	t.Setenv("PROXY_ACP_IDLE_TIMEOUT_MS", "nope")
	_, err := Load("")
	if err == nil || !strings.Contains(err.Error(), "PROXY_ACP_IDLE_TIMEOUT_MS") {
		t.Fatalf("error = %v, want idle timeout error", err)
	}
}

func TestNormalizeNonCodexDefaultsAreRestricted(t *testing.T) {
	cfg := Config{
		DefaultBackend: "fake",
		Backends: []BackendConfig{{
			Key:     "fake",
			Command: "fake-acp",
		}},
	}
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	caps := cfg.Backends[0].Capabilities
	if !caps.ReadTextFile() {
		t.Fatalf("read capability should default to enabled")
	}
	if caps.WriteTextFile() || caps.TerminalEnabled() {
		t.Fatalf("non-codex backend should not default to write/terminal: %#v", caps)
	}
}

func withCleanEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"PROXY_ACP_ENV",
		"PROXY_ACP_PORT",
		"PROXY_ACP_ADDR",
		"PROXY_ACP_AUTH_TOKEN",
		"CODEX_CLI",
		"CODEX_BACKEND",
		"CODEX_ARGS",
		"CODEX_APP_SERVER_ARGS",
		"PROXY_ACP_IDLE_TIMEOUT_MS",
		"http_proxy",
		"HTTP_PROXY",
		"https_proxy",
		"HTTPS_PROXY",
	} {
		value, ok := os.LookupEnv(key)
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset %s: %v", key, err)
		}
		t.Cleanup(func() {
			if ok {
				_ = os.Setenv(key, value)
			} else {
				_ = os.Unsetenv(key)
			}
		})
	}
	t.Chdir(t.TempDir())
}
