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

## Query syntax (what the planner can ask for)

The agent passes the prompt straight to Slack's
`assistant.search.context` after two small rewrites — so the planner
can use Slack's native search filters in the prompt verbatim and they
just work.

Filters Slack honors that the planner can include in `prompt`:

| Filter | Example | Effect |
|---|---|---|
| `from:<name>` | `from:erica deploys` | Messages authored by the named user. The agent resolves `<name>` (real name, display name, first name, normalized variants) against a 1-hour-cached `users.list` snapshot and rewrites it to Slack's canonical `from:<@USERID>` form. Unresolvable or ambiguous names pass through verbatim. |
| `from <Name>` / `by <Name>` | `recent commits by Erica` | Same resolution as `from:`, but in natural-language shape. Requires a capitalized name token so common sentences ("hear from the team") aren't munged. |
| `did <Name> say/post/write/…` | `what did Erica say about deploys` | Same — picks up the "did NAME verb …" shape. |
| `before:YYYY-MM-DD` | `before:2026-05-01 incident` | Slack-native; passed through. |
| `after:YYYY-MM-DD` | `after:2026-05-01 incident` | Slack-native; passed through. |
| `in:#channel` | `in:#deploys friday` | Slack-native; passed through. |
| `has:link`, `has:reaction:tada`, etc. | `has:link onboarding` | Slack-native; passed through. |

### Recency-aware sort

When the prompt contains any of `latest`, `newest`, `most recent`,
`recent `, `today`, `yesterday`, or `last week` (case-insensitive), the
agent picks `sort=timestamp` so the LLM gets the 10 most-recent matches
rather than Slack's semantic ranking. Otherwise it uses the default
`sort=score`. The planner can therefore steer ranking with one English
keyword:

- `recent makechain decisions` — semantic ranking (relevance wins).
- `latest from:erica` — chronological (most recent first).

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
go test -race ./examples/slack_search_agent/...
```

19 tests covering the gRPC contract, slack-go integration, intent-based
sort selection (recency vs semantic), `from:`/`from <Name>`/`did <Name>
say` resolution against a cached `users.list` snapshot (with cache-hit,
TTL-eviction, and duplicate-name-ambiguity cases), and the two prompt
ingestion paths (structured `subagent_prompt` and legacy
`messages[]` envelope). An `httptest.Server` pretending to be slack.com
is wired up via `slack.OptionAPIURL`, so no real Slack calls happen in
CI.
