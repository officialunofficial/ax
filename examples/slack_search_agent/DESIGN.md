# DESIGN: Structured subagent prompt contract

This note explains why `AgentStart` grew two new fields
(`subagent_prompt`, `subagent_history`), how the slack_search_agent
uses them, and how existing subagents keep working unmodified.

## Why the legacy envelope was suboptimal

When the Gemini planner dispatches to a subagent, it calls a Gemini
"function" whose declared parameters are `{history, prompt}`. Gemini
returns those args typed and structured:

```json
{
  "name": "slack_search",
  "args": {
    "history": "user: What is the latest thing Erica said?",
    "prompt": "latest from Erica"
  }
}
```

Until this change, `internal/gemini/gemini_planner.go` then flattened
both args into a single free-form string:

```
History Summary:
user: What is the latest thing Erica said?

Prompt:
latest from Erica
```

and shoved that string into `AgentStart.messages[0].content.text`.
The subagent received it as a user-role text message and had to
string-parse it back out to recover the `prompt`.

Concrete failure mode: slack_search_agent fed that whole blob to
Slack's `assistant.search.context` API. The query was the literal
multi-line envelope; Slack matched against `"History Summary"`,
`"user:"`, and other header text, returning semantic noise. The fix
required adding a private `stripAXHistoryEnvelope` helper that
LastIndex-searches for `"\nPrompt:\n"` — brittle, and silently breaks
the moment the planner's envelope wording changes.

## The new structured contract

Two new optional fields on `AgentStart`:

```proto
message AgentStart {
  string agent_id = 1;
  bytes  agent_config = 2;
  repeated Message messages = 3;
  string subagent_prompt = 4;   // NEW: planner's `prompt` arg verbatim
  string subagent_history = 5;  // NEW: planner's `history` arg verbatim
}
```

`gemini_planner.handleSubagentCall` populates BOTH:

1. The structured fields — typed access to Gemini's function-call args,
   no envelope shenanigans.
2. The legacy `"History Summary:\n…\nPrompt:\n…"` envelope as
   `messages[0]`, exactly as before.

Modern subagents opt in by reading `start.GetSubagentPrompt()` first
and falling back to the message-text path only when it's empty.

## Migration path

For each existing subagent author:

1. Read `req.GetStart().GetSubagentPrompt()`. If non-empty, use it as
   the canonical prompt; you're done.
2. Otherwise, fall back to whatever you already do (scan messages,
   strip envelope, etc.). This path handles direct callers (gRPC,
   `grpcurl`) and pre-structured-field planners.

The slack_search_agent in this example shows the pattern in
`internal/server/server.go::Connect`:

```go
raw := start.GetSubagentPrompt()
if raw == "" {
    var ok bool
    raw, ok = lastUserText(start.Messages)
    if !ok {
        return errors.New("no user message with text content found")
    }
    raw = stripAXHistoryEnvelope(raw)
}
query := stripBotMention(raw)
```

## Backward-compatibility guarantee

The change is **purely additive**:

- No existing proto field is removed, renumbered, or repurposed.
- `messages[0]` still carries the legacy envelope verbatim.
- `python_sandbox_agent`, which only reads `start.Messages`, is
  unchanged and untested-against in this PR (deliberately, as the
  proof that legacy subagents are untouched).
- Old wire bytes (subagent_prompt/history unset) decode into the same
  zero-value strings the new code already handles via the
  empty-string fallback branch.

## Tests

- `internal/gemini/gemini_planner_test.go::TestHandleSubagentCall_PopulatesStructuredAndLegacyFields`
  asserts the planner writes both the structured fields and the legacy
  envelope.
- `examples/slack_search_agent/internal/server/server_test.go::TestConnect_PrefersStructuredSubagentPrompt`
  asserts the subagent reads the structured field and ignores the
  envelope when present.
- `…::TestConnect_FallsBackToMessagesWhenSubagentPromptEmpty`
  asserts the back-compat path still works when the structured field
  is unset.
