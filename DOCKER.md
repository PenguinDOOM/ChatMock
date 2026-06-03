# Docker Deployment

## Quick Start (Rust, validated primary runtime)
1) Setup env:
   cp .env.example .env

2) Login:
   docker compose --profile rust-login run --rm --service-ports chatmock-rust-login login

   - The command prints an auth URL, copy paste it into your browser.
   - If your browser cannot reach the container's localhost callback, copy the full redirect URL from the browser address bar and paste it back into the terminal when prompted.
   - Server should stop automatically once it receives the tokens and they are saved.

3) Start the server:
   docker compose --profile rust up -d chatmock-rust

4) Free to use it in whichever chat app you like!

Rust is now the primary validated Docker runtime path. Packaged releases still follow `python-release-path-unchanged`, Python fallback remains `explicit-legacy-python-cli`, GUI remains `deferred-python-pyinstaller`, and rollback stays explicit through the existing Python services or by disabling stateful websocket-upstream mode.

## Python rollback / fallback path
Python services remain available as the explicit rollback/fallback path.

- Serve service: `chatmock`
- Login service: `chatmock-login`

## Rust service details
Rust Docker services are available and validated.

- Serve service: `chatmock-rust` under profile `rust`
- Login service: `chatmock-rust-login` under profile `rust-login`
- The Rust image builds from `chatmock-rs/Dockerfile`
- The serve container listens on port `8000`
- The serve container responds on `/health` and `/v1/models`
- The Rust container respects auth state at mounted `/data/auth.json`
- The login container starts the login flow on port `1455` and prints the auth URL

Start the Rust server:

```bash
docker compose --profile rust up -d chatmock-rust
```

Run Rust login:

```bash
docker compose --profile rust-login run --rm --service-ports chatmock-rust-login login
```

## Configuration
Set options in `.env` or pass environment variables:
- `PORT`: Container listening port (default 8000)
- `CHATMOCK_IMAGE`: image tag to run (default `storagetime/chatmock:latest`)
- `VERBOSE`: `true|false` to enable request/stream logs
- `CHATGPT_LOCAL_REASONING_EFFORT`: minimal|low|medium|high|xhigh
- `CHATGPT_LOCAL_REASONING_SUMMARY`: auto|concise|detailed|none
- `CHATGPT_LOCAL_REASONING_COMPAT`: legacy|o3|think-tags|current
- `CHATGPT_LOCAL_FAST_MODE`: `true|false` to enable fast mode by default for supported models
- `CHATGPT_LOCAL_CLIENT_ID`: OAuth client id override (rarely needed)
- `CHATGPT_LOCAL_EXPOSE_REASONING_MODELS`: `true|false` to add reasoning model variants to `/v1/models`
- `CHATGPT_LOCAL_ENABLE_WEB_SEARCH`: `true|false` to enable default web search tool
- `CHATGPT_LOCAL_RESPONSES_WEBSOCKET_UPSTREAM`: `true|false` to opt into websocket upstream transport for HTTP `/v1/responses` (default `false`)
- `CHATGPT_LOCAL_RESPONSES_WEBSOCKET_UPSTREAM_STATEFUL`: `true|false` to retain HTTP `/v1/responses` follow-up state across requests; requires `CHATGPT_LOCAL_RESPONSES_WEBSOCKET_UPSTREAM=true` (default `false`)

## `/v1/responses` websocket upstream
Set `CHATGPT_LOCAL_RESPONSES_WEBSOCKET_UPSTREAM=true` to opt into websocket upstream transport for HTTP `/v1/responses` only.

- The client-facing request still uses HTTP.
- With only `CHATGPT_LOCAL_RESPONSES_WEBSOCKET_UPSTREAM=true`, HTTP follow-up requests stay one-shot even if the client resends `previous_response_id`. ChatMock does not reuse an upstream websocket across separate HTTP requests and does not automatically reuse that marker.
- Set `CHATGPT_LOCAL_RESPONSES_WEBSOCKET_UPSTREAM_STATEFUL=true` as well to enable the stateful bridge mode. This mode requires websocket-upstream mode; it is not valid on its own.
- In stateful mode, the first HTTP turn starts a new conversation. It does not require `previous_response_id`, and blank explicit `X-Session-Id` or `session_id` headers do not block that first-turn request.
- Stateful follow-up turns continue by sending `previous_response_id`. ChatMock retains websocket ownership by response marker rather than by explicit session header.
- Repeating the same follow-up marker is a deterministic conflict that returns HTTP `409`.
- For non-streaming stateful HTTP requests, a stale marker or lost retained socket returns HTTP `400` with `error.code=previous_response_not_found`. Clients such as VS Code can retry without `previous_response_id`.
- In stateful mode, streaming HTTP `/v1/responses` now stay incremental: ChatMock passes through SSE events as the upstream websocket produces them while still finalizing retained websocket ownership when the stream completes, fails, or is aborted. For an invalid marker, the current behavior is HTTP `200` SSE with the first event `type=error` and `status_code=400`, not a pre-commit HTTP `400`.
- Retained websocket ownership is process-local only. There is no shared registry across workers, containers, or processes, so follow-up requests must keep hitting the same ChatMock process.
- If the websocket upstream path fails, the request fails clearly instead of silently falling back to the legacy HTTP POST upstream transport.
- Rollback is immediate: set `CHATGPT_LOCAL_RESPONSES_WEBSOCKET_UPSTREAM_STATEFUL=false` or unset it, and ChatMock returns to the existing one-shot bridge behavior.

## Logs
Set `VERBOSE=true` to include extra logging for troubleshooting upstream or chat app requests. Please include and use these logs when submitting bug reports.

## Manual verification
Default-off regression check (`CHATGPT_LOCAL_RESPONSES_WEBSOCKET_UPSTREAM=true` and `CHATGPT_LOCAL_RESPONSES_WEBSOCKET_UPSTREAM_STATEFUL=false` or unset):

```bash
curl -s http://localhost:8000/v1/responses \
   -H 'Content-Type: application/json' \
   -H 'X-Session-Id: docs-regression' \
   -d '{"model":"gpt-5.4","input":"Remember the token ALPHA-42."}' | jq .

curl -s http://localhost:8000/v1/responses \
   -H 'Content-Type: application/json' \
   -H 'X-Session-Id: docs-regression' \
   -d '{"model":"gpt-5.4","input":"What token did I ask you to remember?"}' | jq .
```

Confirm the second response behaves like a fresh one-shot request rather than continuing the first turn. If you resend the first response marker as `previous_response_id` in this transport-only mode, confirm it still behaves as a one-shot request.

Stateful mode (`CHATGPT_LOCAL_RESPONSES_WEBSOCKET_UPSTREAM=true` and `CHATGPT_LOCAL_RESPONSES_WEBSOCKET_UPSTREAM_STATEFUL=true`):

1. Restart the active serve container after changing the environment variable. For the Rust primary path, that is `chatmock-rust`. Then send a first HTTP `/v1/responses` request without `previous_response_id`. Optionally send a blank `X-Session-Id` or `session_id` header and confirm it does not block the request.
2. Send a second request with `previous_response_id` from the first turn and confirm the conversation continues and can answer with `ALPHA-42`.
3. Repeat the same follow-up request for that marker and confirm the conflict is deterministic and returns HTTP `409`.
4. For a non-streaming follow-up, force a stale marker or lost retained socket and confirm HTTP `400` with `error.code=previous_response_not_found`, so the client can retry without `previous_response_id`.
5. For a streaming follow-up with an invalid marker, confirm the current limitation: HTTP `200` SSE with the first event `type=error` and `status_code=400`, not a pre-commit HTTP `400`.
6. Keep the follow-up requests on the same ChatMock process; cross-worker or shared-registry follow-up reuse is not supported.
7. If you intentionally break websocket upstream connectivity, confirm the request fails clearly instead of silently succeeding through the legacy HTTP POST upstream path.

## Test

The README-compatible smoke test below was revalidated against `gpt-5.4` and returned `pong`.

```
curl -s http://localhost:8000/v1/chat/completions \
   -H 'Content-Type: application/json' \
   -d '{"model":"gpt-5.4","messages":[{"role":"user","content":"ping"}]}' | jq .
```
