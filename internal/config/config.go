package config

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultAddr          = "127.0.0.1"
	defaultPort          = "3210"
	defaultCodexCLI      = "codex"
	defaultCodexBackend  = "app-server"
	defaultIdleTimeoutMs = int64(30 * time.Minute / time.Millisecond)

	DefaultCodexBackendKey = "codex"
	SelfBackendCommand     = "__proxy-acp-codex-self__"
	CodexBackendModeArg    = "__codex-cli-acp"
)

type Config struct {
	ListenAddr     string
	AuthToken      string
	DefaultBackend string
	Backends       []BackendConfig
}

type BackendConfig struct {
	Key           string
	Command       string
	Args          []string
	Env           map[string]string
	IdleTimeoutMs int64
	Capabilities  BackendCapabilities
}

type BackendCapabilities struct {
	Fs       FileSystemCapabilities
	Terminal *bool
}

type FileSystemCapabilities struct {
	ReadTextFile  *bool
	WriteTextFile *bool
}

func Load(path string) (Config, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		path = strings.TrimSpace(os.Getenv("PROXY_ACP_ENV"))
	}
	if path == "" {
		if _, err := os.Stat(".env"); err == nil {
			path = ".env"
		}
	}

	values := map[string]string{}
	if path != "" {
		fileValues, err := readDotenv(path)
		if err != nil {
			return Config{}, err
		}
		for key, value := range fileValues {
			values[key] = value
		}
	}
	for _, key := range []string{
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
		if value, ok := os.LookupEnv(key); ok {
			values[key] = value
		}
	}

	cfg, err := fromEnv(values)
	if err != nil {
		return Config{}, err
	}
	if err := cfg.Normalize(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func fromEnv(values map[string]string) (Config, error) {
	addr := strings.TrimSpace(envOrDefault(values, "PROXY_ACP_ADDR", defaultAddr))
	if addr == "" {
		addr = defaultAddr
	}
	port := strings.TrimSpace(envOrDefault(values, "PROXY_ACP_PORT", defaultPort))
	if err := validatePort(port); err != nil {
		return Config{}, err
	}
	codexCLI := strings.TrimSpace(envOrDefault(values, "CODEX_CLI", defaultCodexCLI))
	if codexCLI == "" {
		codexCLI = defaultCodexCLI
	}
	codexBackend := strings.TrimSpace(envOrDefault(values, "CODEX_BACKEND", defaultCodexBackend))
	if codexBackend == "" {
		codexBackend = defaultCodexBackend
	}
	if codexBackend != "app-server" && codexBackend != "exec-json" {
		return Config{}, fmt.Errorf("CODEX_BACKEND must be app-server or exec-json")
	}
	backendArgs := []string{CodexBackendModeArg, "-backend", codexBackend, "-codex", codexCLI}
	switch codexBackend {
	case "app-server":
		appServerArgs, err := splitShellArgs(strings.TrimSpace(values["CODEX_APP_SERVER_ARGS"]))
		if err != nil {
			return Config{}, fmt.Errorf("CODEX_APP_SERVER_ARGS: %w", err)
		}
		for _, arg := range appServerArgs {
			backendArgs = append(backendArgs, "-app-server-arg", arg)
		}
	case "exec-json":
		codexArgs, err := splitShellArgs(strings.TrimSpace(values["CODEX_ARGS"]))
		if err != nil {
			return Config{}, fmt.Errorf("CODEX_ARGS: %w", err)
		}
		for _, arg := range codexArgs {
			backendArgs = append(backendArgs, "-arg", arg)
		}
	}
	idleTimeoutMs := defaultIdleTimeoutMs
	if raw := strings.TrimSpace(values["PROXY_ACP_IDLE_TIMEOUT_MS"]); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed <= 0 {
			return Config{}, fmt.Errorf("PROXY_ACP_IDLE_TIMEOUT_MS must be a positive integer")
		}
		idleTimeoutMs = parsed
	}
	backendEnv := proxyEnv(values)

	cfg := Config{
		ListenAddr:     net.JoinHostPort(addr, port),
		AuthToken:      strings.TrimSpace(values["PROXY_ACP_AUTH_TOKEN"]),
		DefaultBackend: DefaultCodexBackendKey,
		Backends: []BackendConfig{{
			Key:           DefaultCodexBackendKey,
			Command:       SelfBackendCommand,
			Args:          backendArgs,
			Env:           backendEnv,
			IdleTimeoutMs: idleTimeoutMs,
		}},
	}
	return cfg, nil
}

func proxyEnv(values map[string]string) map[string]string {
	env := map[string]string{}
	for _, key := range []string{"http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY"} {
		if value := strings.TrimSpace(values[key]); value != "" {
			env[key] = value
		}
	}
	return env
}

func (c *Config) Normalize() error {
	c.ListenAddr = strings.TrimSpace(c.ListenAddr)
	if c.ListenAddr == "" {
		c.ListenAddr = net.JoinHostPort(defaultAddr, defaultPort)
	}
	if len(c.Backends) == 0 {
		return fmt.Errorf("at least one backend is required")
	}

	seen := map[string]struct{}{}
	for idx := range c.Backends {
		backend := &c.Backends[idx]
		backend.Key = strings.TrimSpace(backend.Key)
		backend.Command = strings.TrimSpace(backend.Command)
		if backend.Key == "" {
			return fmt.Errorf("backends[%d].key is required", idx)
		}
		if backend.Command == "" {
			return fmt.Errorf("backends[%d].command is required", idx)
		}
		if _, ok := seen[backend.Key]; ok {
			return fmt.Errorf("duplicate backend key %q", backend.Key)
		}
		seen[backend.Key] = struct{}{}
		if backend.IdleTimeoutMs <= 0 {
			backend.IdleTimeoutMs = defaultIdleTimeoutMs
		}
		if backend.Env == nil {
			backend.Env = map[string]string{}
		}
		backend.Capabilities.normalize()
	}

	c.DefaultBackend = strings.TrimSpace(c.DefaultBackend)
	if c.DefaultBackend == "" && len(c.Backends) > 0 {
		c.DefaultBackend = c.Backends[0].Key
	}
	if _, ok := seen[c.DefaultBackend]; !ok {
		return fmt.Errorf("defaultBackend %q does not match any backend", c.DefaultBackend)
	}
	return nil
}

func (c Config) Backend(key string) (BackendConfig, bool) {
	key = strings.TrimSpace(key)
	if key == "" {
		key = c.DefaultBackend
	}
	for _, backend := range c.Backends {
		if backend.Key == key {
			return backend, true
		}
	}
	return BackendConfig{}, false
}

func (c *BackendCapabilities) normalize() {
	if c.Fs.ReadTextFile == nil {
		c.Fs.ReadTextFile = boolPtr(true)
	}
	if c.Fs.WriteTextFile == nil {
		c.Fs.WriteTextFile = boolPtr(false)
	}
	if c.Terminal == nil {
		c.Terminal = boolPtr(false)
	}
}

func (c BackendCapabilities) ReadTextFile() bool {
	return c.Fs.ReadTextFile != nil && *c.Fs.ReadTextFile
}

func (c BackendCapabilities) WriteTextFile() bool {
	return c.Fs.WriteTextFile != nil && *c.Fs.WriteTextFile
}

func (c BackendCapabilities) TerminalEnabled() bool {
	return c.Terminal != nil && *c.Terminal
}

func readDotenv(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	values := map[string]string{}
	scanner := bufio.NewScanner(file)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("%s:%d: expected KEY=VALUE", path, lineNo)
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("%s:%d: key is required", path, lineNo)
		}
		values[key] = parseDotenvValue(strings.TrimSpace(value))
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return values, nil
}

func parseDotenvValue(value string) string {
	if len(value) >= 2 {
		quote := value[0]
		if quote == value[len(value)-1] && (quote == '"' || quote == '\'') {
			value = value[1 : len(value)-1]
			if quote == '"' {
				value = strings.NewReplacer(`\n`, "\n", `\r`, "\r", `\t`, "\t", `\"`, `"`, `\\`, `\`).Replace(value)
			}
		}
	}
	return value
}

func envOrDefault(values map[string]string, key string, fallback string) string {
	if value, ok := values[key]; ok {
		return value
	}
	return fallback
}

func validatePort(port string) error {
	parsed, err := strconv.Atoi(port)
	if err != nil || parsed < 1 || parsed > 65535 {
		return fmt.Errorf("PROXY_ACP_PORT must be an integer from 1 to 65535")
	}
	return nil
}

func splitShellArgs(input string) ([]string, error) {
	var args []string
	var current strings.Builder
	var quote rune
	escaped := false
	inArg := false

	for _, r := range input {
		if escaped {
			current.WriteRune(r)
			escaped = false
			inArg = true
			continue
		}
		if r == '\\' {
			escaped = true
			inArg = true
			continue
		}
		if quote != 0 {
			if r == quote {
				quote = 0
			} else {
				current.WriteRune(r)
			}
			inArg = true
			continue
		}
		if r == '\'' || r == '"' {
			quote = r
			inArg = true
			continue
		}
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			if inArg {
				args = append(args, current.String())
				current.Reset()
				inArg = false
			}
			continue
		}
		current.WriteRune(r)
		inArg = true
	}
	if escaped {
		return nil, fmt.Errorf("unfinished escape")
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated quote")
	}
	if inArg {
		args = append(args, current.String())
	}
	return args, nil
}

func boolPtr(v bool) *bool {
	return &v
}
