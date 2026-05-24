# websearch_agent

AX remote-agent example. Implements `proto.AgentService` over gRPC on
`:8494`. On every `Connect()` call it takes the search query, makes a
single Gemini call with `Tools=[GoogleSearch{}]` only, and streams one
`AgentResponse` containing the grounded text plus a `Sources:` footer of
the citations Gemini returned.

## Why it exists (and where it fits)

Vertex Enterprise rejects `Tools: [FunctionDeclarations + GoogleSearch]`
in the same Gemini call with:

```
400 INVALID_ARGUMENT: Multiple tools are supported only when they are
all search tools.
```

This kills the obvious "let the planner just turn on `google_search`"
shortcut — the planner ALSO has to pass our custom subagents (`slacksearch`,
`py`) as `FunctionDeclarations`, so mixing the two in a single call is
forbidden.

The fix is the **agent-as-tool** pattern: register a synthetic
`websearch` AX subagent that, when invoked by the planner, makes a
SECOND Gemini call internally — that second call uses ONLY
`Tools=[GoogleSearch{}]`, no `FunctionDeclarations`, so Vertex is happy.

```
AX server (ax serve)
     │
     │ planner picks websearch as a FunctionDeclaration
     ▼
websearch_agent gRPC :8494
     │
     │ second Gemini call with Tools=[GoogleSearch{}] only
     ▼
aiplatform.googleapis.com (Vertex / Gemini)
```

Like `slack_search_agent`, this is a pure outbound-HTTPS service — no
Sandbox / gVisor pod needed.

## Build

```bash
cd /path/to/ax
gcloud builds submit \
  --config=examples/websearch_agent/cloudbuild.yaml \
  --substitutions=_TAG=$(git rev-parse --short HEAD) .
```

Produces:
- `us-east4-docker.pkg.dev/official-unofficial/docker/websearch-agent:<sha>`
- `...:latest`

## Configure AX to use it

In `ax.yaml`:

```yaml
registry:
  remote_agents:
    - id: websearch
      name: Web Search
      description: |
        Search the live public web via Google. The `prompt` argument
        should be the search query in natural English. Use when the
        question requires current public information.
      address: websearch-agent.agent-platform.svc.cluster.local:8494
      protocol: axp
```

## Required env

| Variable                    | Required | Default                  | Description |
|-----------------------------|----------|--------------------------|-------------|
| `GOOGLE_CLOUD_PROJECT`      | yes      | —                        | GCP project that hosts Vertex/Gemini. |
| `GOOGLE_CLOUD_LOCATION`     | no       | `global`                 | Vertex region. |
| `GEMINI_MODEL`              | no       | `gemini-3-flash-preview` | Gemini model id. |
| `GOOGLE_GENAI_USE_VERTEXAI` | no       | (warn if unset)          | The genai SDK env-var hint; the Server passes Backend=BackendVertexAI explicitly so it's not strictly required, but operators usually set it for parity with other deployments. |
| `LISTEN_ADDR`               | no       | `:8494`                  | gRPC bind address. |

Auth is via Workload Identity Federation — the binary uses Application
Default Credentials. Bind the KSA that runs this pod to a GSA with
`roles/aiplatform.user` (or equivalent). The uno-infra deployment reuses
the `adk-agent-vertex` GSA that already has Vertex access.

## How the prompt is read

Identical to `slack_search_agent`:

1. **Structured field (preferred).** `AgentStart.subagent_prompt` is
   used as-is. The AX planner forwards Gemini's typed `prompt`
   function-call arg directly.
2. **Legacy `messages[]` envelope (back-compat).** When
   `subagent_prompt` is empty, we fall back to the most recent user-text
   message, strip the `History Summary:\n…\nPrompt:\n…` envelope if
   present, and strip any leading `<@U…>` bot mention.

## Smoke test (no K8s)

```bash
export GOOGLE_GENAI_USE_VERTEXAI=true
export GOOGLE_CLOUD_PROJECT=official-unofficial
export GOOGLE_CLOUD_LOCATION=global
gcloud auth application-default login    # if not already done
go run ./examples/websearch_agent &
grpcurl -plaintext \
  -d '{"start":{"subagent_prompt":"current weather in San Francisco"}}' \
  localhost:8494 ax.AgentService/Connect
# => "It is 64F and partly cloudy in San Francisco..."
#    "Sources:"
#    "1. National Weather Service — https://weather.gov/sf"
#    ...
```

## Tests

```bash
go test -race ./examples/websearch_agent/...
```

Covers the gRPC contract, the load-bearing Vertex tool-shape constraint
(GoogleSearch alone, no FunctionDeclarations), default model selection,
citation formatting, the empty-grounding pass-through, both prompt
ingestion paths (structured `subagent_prompt` and legacy envelope),
bot-mention stripping, and the Gemini-error fallthrough. A `fakeGenai`
implementing the small `genaiClient` interface stands in for
`*genai.Client.Models`, so no real Vertex calls happen in CI.
