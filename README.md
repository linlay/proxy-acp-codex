# proxy-acp-codex

## 1. Project Overview

`proxy-acp-codex` bridges Zenmind `agent-platform` PROXY agents to the local Codex CLI through ACP. It exposes the HTTP/SSE and WebSocket surface expected by the platform, then translates requests into ACP stdio sessions backed by Codex app-server true streaming.

```text
webclient -> agent-platform(PROXY) -> proxy-acp-codex(HTTP/SSE + WS + internal ACP backend) -> Codex CLI app-server
```

This project currently supports Codex CLI only. The proxy server, platform protocol handling, and ACP conversion layer are bundled into this repository and built into the single `proxy-acp-codex` binary; users do not need to install the sibling `proxy-acp` project or a separate ACP adapter such as `codex-acp`.

## 2. Quick Start

### Prerequisites

- Go 1.26 or newer
- Codex CLI available on `PATH` or configured by absolute path, for example `@openai/codex` CLI `0.133.0`
- Codex authentication available to the service environment, usually through the same user account that runs `codex`

### Local Run

```bash
cp .env.example .env
# Edit CODEX_CLI and CODEX_APP_SERVER_ARGS in .env when needed.
make build
make run
```

By default the service listens on `http://127.0.0.1:17071` and auth is disabled.

Point an `agent-platform` PROXY / ACP-PROXY agent at:

```yaml
mode: PROXY
proxyConfig:
  baseUrl: http://127.0.0.1:17071
```

The platform must send `params.cwd` with each query. `proxy-acp-codex` does not choose a default working directory.

### Test

```bash
make test
```

## 3. Configuration

`proxy-acp-codex` reads dotenv configuration from the `-env` flag, then `PROXY_ACP_ENV`, then `.env` in the working directory when present.

- `.env.example` is the committed example configuration.
- `.env` is local-only and must not be committed.
- `PROXY_ACP_PORT` defaults to `17071`.
- `PROXY_ACP_ADDR` defaults to `127.0.0.1` when empty. To allow remote access, set it explicitly, for example `PROXY_ACP_ADDR=0.0.0.0`.
- `PROXY_ACP_AUTH_TOKEN` defaults to empty, which leaves API routes unauthenticated. When set, clients must send `Authorization: Bearer <token>` or `?token=<token>`. For `agent-platform` PROXY agents, configure `proxyConfig.token` or `proxyConfig.tokenEnv` only when this token is set upstream.
- `CODEX_CLI` defaults to `codex` and may be an absolute path.
- `CODEX_BACKEND` defaults to `app-server`, which runs `codex app-server --listen stdio://` and forwards real Codex deltas. Set `CODEX_BACKEND=exec-json` only for the legacy `codex exec --json` adapter.
- `CODEX_APP_SERVER_ARGS` defaults to empty and accepts shell-style argument splitting. Values are passed to `codex app-server` after `--listen stdio://`, for example `CODEX_APP_SERVER_ARGS="--enable network_proxy"`.
- `CODEX_ARGS` is used only when `CODEX_BACKEND=exec-json`. Values are passed to `codex exec` / `codex exec resume` before the prompt, for example `CODEX_ARGS="--model gpt-5"`.
- `PROXY_ACP_IDLE_TIMEOUT_MS` defaults to `1800000`.
- `agent-platform` PROXY request timeout defaults to `300000ms`; `proxyConfig.timeoutMs` is only needed when overriding that default.

Configuration priority:

```text
built-in defaults < dotenv file < process environment
```

Backend processes inherit the `proxy-acp-codex` process environment, so values such as `CODEX_HOME`, `PATH`, and `HOME` come from the Go service process. Commands are launched directly with `exec.Command`, not through a shell, so shell aliases and functions are not inherited.

Runtime dependency model:

```text
required on user machine: Codex CLI
bundled by this project: HTTP/SSE/WS proxy, platform DTOs, ACP bridge, Codex app-server adapter
```

## 4. Deployment

Build the local binary:

```bash
make build
```

This writes the single executable artifact to `bin/proxy-acp-codex`.

Build the Windows amd64 binary:

```bash
make build-windows-amd64
```

This writes `dist/windows-amd64/proxy-acp-codex.exe`.

Run with an explicit dotenv path:

```bash
./bin/proxy-acp-codex -env /path/to/proxy-acp-codex.env
```

Deployments should inject sensitive values through platform secrets or environment-specific dotenv files outside source control. This repository does not currently include a Dockerfile or container orchestration definition.

## 5. Operations

### Health Check

```bash
curl http://127.0.0.1:17071/healthz
```

### Logs

The service writes process logs to stdout/stderr. Capture those streams through the process manager, container runtime, or hosting platform used for deployment.

### Common Checks

- Confirm the configured port and address are reachable.
- Confirm the configured Codex CLI command exists on `PATH` or is configured by absolute path.
- Confirm the installed Codex CLI supports `codex app-server --listen stdio://`.
- Confirm `agent-platform` sends `params.cwd` for each query.
- Confirm the client sends the configured bearer token when `PROXY_ACP_AUTH_TOKEN` is set.
- Confirm long-running sessions are not being closed by `PROXY_ACP_IDLE_TIMEOUT_MS`.

The default Codex backend advertises file read only through ACP. Codex still owns its normal approval and sandbox behavior when it executes work through app-server, so use this proxy only for trusted local/platform access.

## Compatibility Surface

- `POST /api/query` returns platform-compatible SSE with `event: message` and terminal `data: [DONE]`.
- `POST /api/submit` forwards platform HITL approval responses to ACP `session/request_permission` when Codex app-server requests command, file-change, or permission approval.
- `POST /api/steer` queues a follow-up user prompt on the active ACP run.
- `POST /api/interrupt` sends ACP `session/cancel`.
- `GET /ws` accepts platform PROXY frames for query, submit, and interrupt.
# proxy-acp-codex
