# slack_search_agent

AX remote-agent example. Implements `proto.AgentService` over gRPC on
`:8494`. On every `Connect()` call it takes the latest user-role
message's text, queries Slack's [`search.messages`][slack-search] API,
and streams a single human-readable `AgentResponse` containing the top
10 matches.

[slack-search]: https://api.slack.com/methods/search.messages

## Where it fits

```
AX server (ax serve)
     │
     │ RemoteAgent (registry entry `remote_agents:`)
     ▼
slack_search_agent gRPC :8494
     │
     ▼
slack.com/api/search.messages
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
    - id: slack
      name: Slack Search Agent
      address: slack-search-agent.agent-platform.svc.cluster.local:8494
      protocol: axp
```

## Required env

| Variable           | Required | Default | Description |
|--------------------|----------|---------|-------------|
| `SLACK_USER_TOKEN` | yes      | —       | Slack user token (`xoxp-...`). Bot tokens cannot call `search.messages`. |
| `LISTEN_ADDR`      | no       | `:8494` | gRPC bind address. |

### Required Slack scopes

The `search.messages` method is user-token-only. Issue an `xoxp-` token
with whichever of these scopes match the surfaces you want to search:

- `search:read.public`
- `search:read.private`
- `search:read.im`
- `search:read.mpim`
- `search:read.files`

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
