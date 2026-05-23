# slack_search_agent

AX remote-agent example. Implements `proto.AgentService` over gRPC on
`:8494`. On every `Connect()` call it takes the latest user-role
message's text, queries Slack's [Real-time Search API][slack-rts]
(`assistant.search.context`), and streams a single human-readable
`AgentResponse` containing the top 10 matches.

[slack-rts]: https://docs.slack.dev/apis/web-api/real-time-search-api/

We use the modern Real-time Search API (GA Feb 2026) rather than the
classic `search.messages`/`search.all` family — the classic endpoints
still demand the legacy `search:read` scope, while the RTS endpoint is
paired with the granular `search:read.*` scopes.

## Where it fits

```
AX server (ax serve)
     │
     │ RemoteAgent (registry entry `remote_agents:`)
     ▼
slack_search_agent gRPC :8494
     │
     ▼
slack.com/api/assistant.search.context
```

Unlike `python_sandbox_agent`, this agent doesn't need a Sandbox /
gVisor pod — it's a pure outbound-HTTPS service. Run it anywhere AX's
ControllerService can dial it.

## Build

```bash
cd /path/to/ax
gcloud builds submit \
  --config=examples/slack_search_agent/cloudbuild.yaml \
  --substitutions=_TAG=$(git rev-parse --short HEAD) .
```

Produces:
- `us-east4-docker.pkg.dev/official-unofficial/docker/slack-search-agent:<sha>`
- `...:latest`

## Configure AX to use it

In `ax.yaml`:

```yaml
registry:
  remote_agents:
    - id: slack_search
      name: Slack Search
      description: Searches the team's Slack workspace for recent messages matching a query
      address: slack-search-agent.agent-platform.svc.cluster.local:8494
      protocol: axp
```

## Required env

| Variable           | Required | Default | Description |
|--------------------|----------|---------|-------------|
| `SLACK_USER_TOKEN` | yes      | —       | Slack user token (`xoxp-...`). Bot tokens (`xoxb-`) cannot search out-of-band. |
| `LISTEN_ADDR`      | no       | `:8494` | gRPC bind address. |

### Required Slack scopes

Granular search scopes only — no legacy `search:read`. Add these to
**User Token Scopes** on the Slack app and reinstall:

- `search:read.public` (minimum)
- `search:read.private` (optional)
- `search:read.im` (optional)
- `search:read.mpim` (optional)
- `search:read.files` (optional)

## How the prompt is read

The Slack query string can arrive in either of two ways, in priority
order:

1. **Structured field (preferred).** When the AX planner invokes us as
   a subagent, it sets `AgentStart.subagent_prompt` directly from
   Gemini's typed `prompt` function-call argument. We use it as-is —
   no envelope parsing, no `lastUserText` scan.

2. **Legacy `messages[]` envelope (back-compat).** When
   `subagent_prompt` is empty (direct callers via `grpcurl`,
   pre-structured-field planners), we fall back to scanning
   `start.messages` for the most recent user-text message and peeling
   off the planner's `"History Summary:\n…\nPrompt:\n…"` envelope if
   present.

See [DESIGN.md](./DESIGN.md) for the full migration story and
backward-compatibility guarantee.

## Smoke test (no K8s)

```bash
export SLACK_USER_TOKEN=xoxp-...
go run ./examples/slack_search_agent &
grpcurl -plaintext \
  -d '{"start":{"messages":[{"role":"user","content":{"text":{"text":"friday launch"}}}]}}' \
  localhost:8494 ax.AgentService/Connect
# => Found N messages for "friday launch":
#    1. #general by alice at 2026-04-12 14:33:00 UTC
#       https://...
#       > we agreed to ship friday
#    ...
```

## Tests

```bash
go test ./examples/slack_search_agent/...
```

6 tests covering: query extraction, formatting of multiple matches,
empty-results, Slack API error passthrough, missing `Start`, and
bot-mention stripping. A fake `httpClient` is injected via
`WithHTTPClient` so no real Slack calls happen in CI.
