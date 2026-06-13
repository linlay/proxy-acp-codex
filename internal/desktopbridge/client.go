package desktopbridge

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	envPluginID      = "DESKTOP_PLUGIN_ID"
	envBridgeVersion = "DESKTOP_PLUGIN_BRIDGE_VERSION"
	envBridgePath    = "DESKTOP_PLUGIN_BRIDGE_PATH"
	envBridgeToken   = "DESKTOP_PLUGIN_BRIDGE_TOKEN"
	bridgeVersion    = "1"
)

type Logger interface {
	Printf(format string, args ...any)
}

type Config struct {
	PluginID string
	Version  string
	Path     string
	Token    string
}

type Client struct {
	config     Config
	proxyID    string
	baseURL    string
	timeoutMS  int
	logger     Logger
	beforeQuit chan struct{}
	closeOnce  sync.Once
	writeMu    sync.Mutex
	requestSeq atomic.Uint64
}

func LoadConfigFromEnv() (Config, bool) {
	cfg := Config{
		PluginID: strings.TrimSpace(os.Getenv(envPluginID)),
		Version:  strings.TrimSpace(os.Getenv(envBridgeVersion)),
		Path:     strings.TrimSpace(os.Getenv(envBridgePath)),
		Token:    strings.TrimSpace(os.Getenv(envBridgeToken)),
	}
	if cfg.PluginID == "" || cfg.Version == "" || cfg.Path == "" || cfg.Token == "" {
		return Config{}, false
	}
	return cfg, true
}

func New(config Config, proxyID string, baseURL string, timeoutMS int, logger Logger) *Client {
	if logger == nil {
		logger = log.Default()
	}
	if timeoutMS <= 0 {
		timeoutMS = 300000
	}
	return &Client{
		config:     config,
		proxyID:    strings.TrimSpace(proxyID),
		baseURL:    strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		timeoutMS:  timeoutMS,
		logger:     logger,
		beforeQuit: make(chan struct{}),
	}
}

func NewFromEnv(proxyID string, baseURL string, timeoutMS int, logger Logger) (*Client, bool) {
	cfg, ok := LoadConfigFromEnv()
	if !ok {
		return nil, false
	}
	return New(cfg, proxyID, baseURL, timeoutMS, logger), true
}

func BaseURLFromListenAddr(listenAddr string) string {
	host, port, err := net.SplitHostPort(strings.TrimSpace(listenAddr))
	if err != nil {
		return "http://" + strings.TrimSpace(listenAddr)
	}
	host = strings.Trim(host, "[]")
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port)
}

func (c *Client) BeforeQuit() <-chan struct{} {
	return c.beforeQuit
}

func (c *Client) Run(ctx context.Context) {
	backoff := 250 * time.Millisecond
	for {
		if err := c.runConnection(ctx); err != nil && ctx.Err() == nil {
			c.logger.Printf("[desktop-bridge] disconnected: %v", err)
		}
		if ctx.Err() != nil {
			return
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

func (c *Client) runConnection(ctx context.Context) error {
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "unix", c.config.Path)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := c.writeEnvelope(conn, map[string]any{
		"type":            "hello",
		"pluginId":        c.config.PluginID,
		"token":           c.config.Token,
		"protocolVersion": 1,
	}); err != nil {
		return err
	}

	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 0, 4096), 1024*1024)
	for scanner.Scan() {
		var envelope map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &envelope); err != nil {
			continue
		}
		c.handleEnvelope(conn, envelope)
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return fmt.Errorf("bridge connection closed")
}

func (c *Client) handleEnvelope(conn net.Conn, envelope map[string]any) {
	if envelope["type"] != "event" {
		return
	}
	name, _ := envelope["name"].(string)
	switch name {
	case "desktop.ready", "agentPlatform.ready", "service.statusChanged:agent-platform":
		if err := c.requestUpsert(conn); err != nil {
			c.logger.Printf("[desktop-bridge] failed to upsert ACP proxy: %v", err)
		}
	case "desktop.beforeQuit":
		c.closeOnce.Do(func() {
			close(c.beforeQuit)
		})
	}
}

func (c *Client) requestUpsert(conn net.Conn) error {
	if c.proxyID == "" || c.baseURL == "" {
		return nil
	}
	id := fmt.Sprintf("proxy_acp_codex_%d", c.requestSeq.Add(1))
	return c.writeEnvelope(conn, map[string]any{
		"type":   "request",
		"id":     id,
		"method": "agentPlatform.upsertAcpProxy",
		"params": map[string]any{
			"proxyId":   c.proxyID,
			"baseUrl":   c.baseURL,
			"timeoutMs": c.timeoutMS,
		},
	})
}

func (c *Client) writeEnvelope(conn net.Conn, envelope map[string]any) error {
	payload, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	payload = append(payload, '\n')
	_, err = conn.Write(payload)
	return err
}
