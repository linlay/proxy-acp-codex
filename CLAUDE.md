# CLAUDE.md

## 1. Project Overview

`proxy-acp-codex` is a Go backend service that adapts Zenmind `agent-platform` PROXY traffic to Codex CLI through ACP. Its boundary is protocol translation and session management; it does not implement model inference, persistence, billing, or user management.

The intended runtime contract is intentionally small: the user installs Codex CLI locally, and this repository provides everything else needed for the proxy path. Do not require the sibling `proxy-acp` project or a separate ACP conversion process at runtime.

Core capabilities:

- Expose platform-compatible HTTP/SSE endpoints.
- Expose a WebSocket endpoint for platform PROXY frames.
- Run an internal ACP backend mode for the host Codex CLI app-server from the same `proxy-acp-codex` binary.
- Start and reuse ACP stdio backend sessions by backend, chat, and working directory.
- Forward cancellation requests to ACP sessions; generic platform HITL forwarding remains available for backends that request ACP permissions.

This repository defaults to Codex CLI app-server through `codex app-server --listen stdio://` for true streaming. The legacy `codex exec --json` adapter remains available with `CODEX_BACKEND=exec-json`.

## 2. Tech Stack

- Language: Go 1.26
- HTTP server: Go standard library `net/http`
- WebSocket: `github.com/gorilla/websocket`
- ACP client SDK: `github.com/coder/acp-go-sdk`
- Config format: dotenv loaded by `internal/config`
- Build tool: `go build` through `Makefile`
- Deployment style: process/binary deployment; container packaging is not defined in this repository.

## 3. Architecture Design

The service is split into three main layers:

- `cmd/proxy-acp-codex`: process entrypoint, flag parsing, env config loading, HTTP server lifecycle, graceful shutdown.
- `internal/codexacp`: internal ACP stdio backend mode for running Codex CLI.
- `internal/server`: HTTP and WebSocket handlers, auth checks, request validation, platform response shaping.
- `internal/acpbridge`: backend selection, ACP process lifecycle, session reuse, turn execution, permission forwarding, cancellation.
- `internal/platform`: platform DTOs and HTTP/SSE serialization helpers.
- `internal/config`: dotenv loading, defaults, normalization, and backend lookup.

Request flow:

```text
platform client -> internal/server -> internal/acpbridge -> proxy-acp-codex hidden ACP mode -> Codex CLI app-server
```

Backend sessions are keyed by backend key, chat id, and working directory. Active runs are tracked separately so interrupt requests can find the session currently handling a run.

## 4. Directory Structure

```text
cmd/proxy-acp-codex/          Service entrypoint.
internal/acpbridge/     ACP backend session and turn management.
internal/codexacp/     Internal Codex ACP backend.
internal/config/        Dotenv config loading and normalization.
internal/platform/      Platform request/response/event types.
internal/server/        HTTP, SSE, WebSocket routes and auth.
internal/testagent/     Local test helper entrypoint.
.env.example            Example dotenv configuration.
README.md               User-facing usage, deployment, and operations guide.
Makefile                Build, run, test, format, and cleanup commands.
```

## 5. Data Structures

Core configuration types live in `internal/config`:

- `Config`: listen address, optional auth token, default backend, backend list.
- `BackendConfig`: backend key, command, args, environment, and idle timeout.
- `BackendCapabilities`: ACP client file-system and terminal capabilities advertised to Codex.

Platform DTOs live in `internal/platform`:

- `QueryRequest`: incoming query payload and routing params.
- `SubmitRequest`: HITL permission response payload.
- `InterruptRequest`: cancellation payload.
- `EventData`: stream event envelope with ordered JSON serialization.

ACP session state lives in `internal/acpbridge`:

- `Manager`: configured backend, session registry, awaiting permission registry, active run registry.
- `backendSession`: running ACP process, ACP connection, active turn state, and lifecycle hooks.

## 6. API Definition

Authentication:

- If `PROXY_ACP_AUTH_TOKEN` is empty, API routes are unauthenticated.
- If `PROXY_ACP_AUTH_TOKEN` is set, clients must pass `Authorization: Bearer <token>` or `?token=<token>`.

Endpoints:

- `GET /healthz`: returns a JSON success response with `{ "ok": true }`.
- `POST /api/query`: accepts `platform.QueryRequest`, requires `params.cwd`, streams platform SSE events, and ends with `[DONE]`.
- `POST /api/submit`: accepts `platform.SubmitRequest`, forwards permission responses, and returns `SubmitResponse`.
- `POST /api/steer`: accepts `platform.SteerRequest`, queues a follow-up prompt on the active run, and returns `SteerResponse`.
- `POST /api/interrupt`: accepts `platform.InterruptRequest`, cancels an active ACP session run, and returns `InterruptResponse`.
- `GET /ws`: upgrades to WebSocket and accepts `request.query`, `request.submit`, `request.steer`, and `request.interrupt` frames.

Error responses use the platform JSON envelope with non-zero `code`, a `msg`, and an empty data object.

## 7. Development Notes

- Keep protocol-specific DTOs in `internal/platform`.
- Keep ACP process and session behavior in `internal/acpbridge`.
- Keep HTTP routing, method checks, auth, and response writing in `internal/server`.
- Config defaults belong in code. Example values belong in `.env.example`. Local values belong in ignored `.env` files.
- The platform owns the session working directory and must send `params.cwd`; do not add a service-side default cwd.
- Tests should cover protocol serialization, config loading, server handlers, and ACP bridge session behavior.
- Avoid committing real tokens, production paths, or host-specific backend credentials.

## 8. Development Flow

Typical local flow:

```bash
cp .env.example .env
make fmt
make test
make run
```

Build the local binary:

```bash
make build
```

Build the Windows amd64 binary:

```bash
make build-windows-amd64
```

Clean local build output:

```bash
make clean
```

## 9. Known Constraints And Notes

- The repository does not currently define Docker or Kubernetes deployment assets.
- Codex CLI must be installed separately and available to the service process.
- `CODEX_CLI` configures the host Codex CLI command or absolute path.
- `CODEX_BACKEND` defaults to `app-server`; `exec-json` is legacy fallback.
- `CODEX_APP_SERVER_ARGS` configures extra `codex app-server` arguments using shell-style splitting.
- `CODEX_ARGS` configures extra `codex exec` arguments only when `CODEX_BACKEND=exec-json`.
- `PROXY_ACP_ADDR` defaults to local-only `127.0.0.1`; remote access requires an explicit value such as `0.0.0.0`.
- Backend processes inherit the service environment.
- Codex defaults to ACP file read only. Codex owns its normal approval and sandbox behavior inside app-server.
- WebSocket origin checks currently allow all origins.
