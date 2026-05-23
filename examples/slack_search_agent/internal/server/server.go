// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package server implements the proto.AgentService for the
// slack_search_agent. Split from package main so it's importable in tests.
package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/slack-go/slack"
	"google.golang.org/grpc"

	"github.com/google/ax/proto"
)

// maxMatchTextLen caps each match body to keep responses scannable.
const maxMatchTextLen = 200

// userCacheTTL is how long the users.list snapshot is trusted before
// the next Lookup forces a refresh. Workspace membership doesn't churn
// often; an hour is a reasonable upper bound on staleness.
const userCacheTTL = time.Hour

// botMentionPrefix matches a leading Slack user mention like "<@U09R4QH2C4D>"
// (optionally followed by whitespace). We strip this so the search query
// doesn't include the mention of the bot itself.
var botMentionPrefix = regexp.MustCompile(`^<@[UW][A-Z0-9]+>\s*`)

// fromPattern captures `from:NAME` (Slack-style filter, lowercase word
// only, e.g. `from:erica`). Quoted multi-word values aren't supported —
// keep it conservative.
var fromPattern = regexp.MustCompile(`(?i)\bfrom:([a-z][a-z0-9._-]*)\b`)

// byNamePattern captures the natural-language equivalents we want to
// rewrite into `from:<@U…>`. Three shapes:
//
//	"from NAME"        (no colon; the colon shape is handled by fromPattern)
//	"by NAME"
//	"did NAME say/…"   (only when followed by a verb-shaped token — see below)
//
// We require a capitalized name for these shapes so we don't munge
// sentences like "what did we hear from the team about deploys" —
// only proper-noun-shaped tokens get resolved.
var byNamePattern = regexp.MustCompile(`(?i)\b(from|by)\s+([A-Z][a-z][A-Za-z0-9._-]*)\b`)

// didNamePattern picks up the "what did NAME say/think/post …" shape.
// We require a following verb-shaped token so "did NAME" alone (rare
// in real prompts) doesn't false-positive. The verb list is short and
// obvious — same rationale as recencyKeywords.
var didNamePattern = regexp.MustCompile(`(?i)\bdid\s+([A-Z][a-z][A-Za-z0-9._-]*)\s+(say|said|post|posted|write|wrote|mention|think|share|shared)\b`)

// recencyKeywords trigger `sort=timestamp` instead of Slack's default
// semantic ranking. Keep this list short and obvious — anything else
// belongs in a proper intent classifier, not here.
var recencyKeywords = []string{
	"latest",
	"newest",
	"most recent",
	"recent ",
	"today",
	"yesterday",
	"last week",
}

// Server is the AgentService implementation. Each Connect RPC takes the
// latest user-message text, queries Slack's assistant.search.context API
// (via slack-go), and streams one human-readable AgentResponse back.
type Server struct {
	proto.UnimplementedAgentServiceServer

	slack *slack.Client
	users *userResolver
}

// New builds a Server. slackToken must be a Slack user token (xoxp-...)
// with the granular search:read.* scopes (search:read.public is the
// minimum; .private/.im/.mpim/.files broaden the corpus). Bot tokens
// (xoxb-) cannot search out-of-band.
func New(slackToken string, opts ...Option) *Server {
	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}

	client := slack.New(slackToken, cfg.slackOptions...)
	s := &Server{
		slack: client,
		users: newUserResolver(client, userCacheTTL),
	}
	return s
}

// Option configures a Server at construction.
type Option func(*config)

type config struct {
	slackOptions []slack.Option
}

// WithSlackOptions passes raw slack-go client options through to the
// underlying *slack.Client. Used by tests to point slack-go at an
// httptest.Server via slack.OptionAPIURL.
func WithSlackOptions(opts ...slack.Option) Option {
	return func(c *config) { c.slackOptions = append(c.slackOptions, opts...) }
}

// Connect implements proto.AgentService. Single-turn: read the last user
// message, run a Slack search, stream one AgentResponse with the
// formatted top-10 matches.
func (s *Server) Connect(req *proto.AgentRequest, stream grpc.ServerStreamingServer[proto.AgentResponse]) error {
	start := req.GetStart()
	if start == nil {
		return errors.New("AgentRequest.Start is required")
	}
	// Prefer the structured AgentStart.subagent_prompt field (modern
	// contract added to proto/ax.proto): the AX planner forwards
	// Gemini's typed `prompt` function-call arg directly, so we get a
	// clean string with no envelope wrapping. Falls back to scanning
	// messages for the legacy "History Summary:\n…\nPrompt:\n…"
	// envelope when subagent_prompt is empty — covers direct (non-AX)
	// callers and pre-structured-field planners.
	raw := start.GetSubagentPrompt()
	if raw == "" {
		var ok bool
		raw, ok = lastUserText(start.Messages)
		if !ok {
			return errors.New("no user message with text content found")
		}
		// Order matters: peel the AX planner's
		// "History Summary:\n…\nPrompt:\n…" envelope FIRST (when
		// present), then strip any leading bot mention from the inner
		// prompt. Direct (non-AX) callers skip the first transform via
		// the helper's no-delimiter fast path.
		raw = stripAXHistoryEnvelope(raw)
	}
	query := stripBotMention(raw)

	var body string
	if query == "" {
		body = "Please provide a search query."
	} else {
		// Resolve any from:NAME / "from NAME" / "by NAME" tokens against
		// the workspace user directory. Unambiguous matches become
		// from:<@USERID>, the canonical Slack filter syntax. Anything
		// else (unresolvable, ambiguous) passes through verbatim.
		query = s.resolveFromTokens(stream.Context(), query)
		body = s.search(stream.Context(), query)
	}

	resp := &proto.AgentResponse{
		ConversationId: req.ConversationId,
		ExecId:         req.ExecId,
		Type: &proto.AgentResponse_Outputs{
			Outputs: &proto.AgentOutputs{
				Messages: []*proto.Message{{
					Role: "assistant",
					Content: &proto.Content{
						Type: &proto.Content_Text{
							Text: &proto.TextContent{Text: body},
						},
					},
				}},
			},
		},
	}
	if err := stream.Send(resp); err != nil {
		return fmt.Errorf("send response: %w", err)
	}
	return nil
}

// search runs the Slack API call via slack-go and returns the formatted
// body. Any transport / API errors are folded into the body itself so
// the caller gets a single human-readable response rather than a gRPC
// error.
func (s *Server) search(ctx context.Context, query string) string {
	params := slack.AssistantSearchContextParameters{
		Query:   query,
		Limit:   10,
		Sort:    sortForQuery(query),
		SortDir: "desc",
	}
	resp, err := s.slack.SearchAssistantContextContext(ctx, params)
	if err != nil {
		// slack-go folds API-level !ok errors into the same error
		// channel as transport errors. Surface the message as-is
		// (e.g. "invalid_auth", "ratelimited", "missing_scope").
		return fmt.Sprintf("Slack search failed: %v", err)
	}
	return formatMatches(query, resp.Results.Messages)
}

// sortForQuery picks `timestamp` for queries clearly asking for
// chronological/recent results; otherwise `score` (Slack's default,
// semantic ranking). The list is kept deliberately short — clever NLP
// here is a recipe for surprise. The planner can also pass an explicit
// `latest` keyword if it wants recency for its own reasons.
func sortForQuery(q string) string {
	lower := strings.ToLower(q)
	for _, k := range recencyKeywords {
		if strings.Contains(lower, k) {
			return "timestamp"
		}
	}
	return "score"
}

// resolveFromTokens rewrites `from:NAME`, `from NAME`, and `by NAME`
// tokens into Slack's canonical `from:<@USERID>` form when the user
// directory has an unambiguous match. Conservative by design — when
// resolution is ambiguous (two users named "Erica") or fails, the
// original text is preserved and a warning is logged.
func (s *Server) resolveFromTokens(ctx context.Context, query string) string {
	// Pass 1: explicit `from:NAME` (Slack-style filter without the @).
	query = fromPattern.ReplaceAllStringFunc(query, func(match string) string {
		sub := fromPattern.FindStringSubmatch(match)
		if len(sub) < 2 {
			return match
		}
		name := sub[1]
		id, status := s.users.Lookup(ctx, name)
		switch status {
		case lookupFound:
			return "from:<@" + id + ">"
		case lookupAmbiguous:
			log.Printf("slack_search_agent: ambiguous user name %q, leaving query unchanged", name)
		case lookupNotFound:
			// Silent — common case (typo, real human name not in workspace).
		case lookupError:
			log.Printf("slack_search_agent: users.list lookup for %q failed, leaving query unchanged", name)
		}
		return match
	})

	// Pass 2: natural-language `from NAME` / `by NAME` (proper-noun shape).
	query = byNamePattern.ReplaceAllStringFunc(query, func(match string) string {
		sub := byNamePattern.FindStringSubmatch(match)
		if len(sub) < 3 {
			return match
		}
		name := sub[2]
		id, status := s.users.Lookup(ctx, name)
		switch status {
		case lookupFound:
			return "from:<@" + id + ">"
		case lookupAmbiguous:
			log.Printf("slack_search_agent: ambiguous user name %q, leaving query unchanged", name)
		case lookupNotFound:
			// Silent.
		case lookupError:
			log.Printf("slack_search_agent: users.list lookup for %q failed, leaving query unchanged", name)
		}
		return match
	})

	// Pass 3: "did NAME say/post/think/…" → "from:<@ID>". The verb is
	// dropped along with "did" — the search query becomes just the
	// remaining content words plus the from: filter.
	query = didNamePattern.ReplaceAllStringFunc(query, func(match string) string {
		sub := didNamePattern.FindStringSubmatch(match)
		if len(sub) < 3 {
			return match
		}
		name := sub[1]
		id, status := s.users.Lookup(ctx, name)
		switch status {
		case lookupFound:
			return "from:<@" + id + ">"
		case lookupAmbiguous:
			log.Printf("slack_search_agent: ambiguous user name %q, leaving query unchanged", name)
		case lookupNotFound:
			// Silent.
		case lookupError:
			log.Printf("slack_search_agent: users.list lookup for %q failed, leaving query unchanged", name)
		}
		return match
	})

	return query
}

// formatMatches produces the human-readable body. Each match is its own
// block separated by a blank line; the LLM viewer can quote them naturally.
func formatMatches(query string, matches []slack.AssistantSearchContextMessage) string {
	if len(matches) == 0 {
		return fmt.Sprintf("No results for %q.", query)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Found %d messages for %q:\n\n", len(matches), query)
	for i, m := range matches {
		channelName := m.ChannelName
		if channelName == "" {
			channelName = m.ChannelID
		}
		who := m.AuthorName
		if who == "" {
			who = m.AuthorUserID
		}
		if who == "" {
			who = "unknown"
		}
		fmt.Fprintf(&b, "%d. #%s by %s at %s\n", i+1, channelName, who, humanTS(m.MessageTS))
		if m.Permalink != "" {
			fmt.Fprintf(&b, "   %s\n", m.Permalink)
		}
		fmt.Fprintf(&b, "   > %s\n", truncate(m.Content, maxMatchTextLen))
		if i < len(matches)-1 {
			b.WriteString("\n")
		}
	}
	return b.String()
}

// humanTS converts a Slack "1700000000.000100" timestamp into a
// "2023-11-14 22:13:20 UTC"-style string. Falls back to the raw value
// if parsing fails.
func humanTS(ts string) string {
	dot := strings.IndexByte(ts, '.')
	secStr := ts
	if dot >= 0 {
		secStr = ts[:dot]
	}
	sec, err := strconv.ParseInt(secStr, 10, 64)
	if err != nil {
		return ts
	}
	return time.Unix(sec, 0).UTC().Format("2006-01-02 15:04:05 UTC")
}

// truncate clips s to max runes with a trailing ellipsis.
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	if max <= 1 {
		return s[:max]
	}
	return s[:max-1] + "…"
}

// stripBotMention removes a leading "<@Uxxxx>" prefix and trims whitespace.
func stripBotMention(s string) string {
	s = strings.TrimSpace(s)
	s = botMentionPrefix.ReplaceAllString(s, "")
	return strings.TrimSpace(s)
}

// stripAXHistoryEnvelope unwraps the planner-synthesized envelope AX
// passes to subagents:
//
//	History Summary:
//	<...stringified history...>
//
//	Prompt:
//	<the planner's actual subagent prompt>
//
// (see gemini_planner.go ~line 362). The "History Summary:" header is
// useful context for code-execution agents but is just noise for a
// search-style agent — Slack's search API would treat the whole envelope
// as the query string and match against the literal multi-line header.
//
// If the trailing "Prompt:\n…" delimiter is present, return everything
// after it (whitespace-trimmed). Otherwise return s unchanged so direct
// (non-AX) callers still work.
func stripAXHistoryEnvelope(s string) string {
	const delim = "\nPrompt:\n"
	if i := strings.LastIndex(s, delim); i >= 0 {
		return strings.TrimSpace(s[i+len(delim):])
	}
	return s
}

// lastUserText returns the text content of the most recent user-role
// message.
func lastUserText(msgs []*proto.Message) (string, bool) {
	for i := len(msgs) - 1; i >= 0; i-- {
		m := msgs[i]
		if m == nil {
			continue
		}
		t := m.GetContent().GetText()
		if t == nil {
			continue
		}
		return t.Text, true
	}
	return "", false
}

// lookupStatus is the disposition of a name → user-ID lookup. We use it
// instead of a bare (string, bool) so callers can distinguish "no such
// name" from "two users share that name" (the latter must NOT silently
// pick one).
type lookupStatus int

const (
	lookupFound lookupStatus = iota
	lookupNotFound
	lookupAmbiguous
	lookupError
)

// userResolver caches a snapshot of the workspace user directory and
// answers name → ID lookups against it. The cache is refreshed lazily
// on first lookup and again any time the snapshot is older than ttl.
//
// We normalize every reasonable name variant (real_name, display_name,
// real_name_normalized, display_name_normalized, name, the first token
// of real_name) to lowercase keys, so `from:erica` resolves a user
// whose Slack profile says "Erica Gregor".
type userResolver struct {
	client *slack.Client
	ttl    time.Duration

	mu sync.Mutex
	// names maps normalized-name → set of user IDs. A set rather than a
	// single ID so we can detect ambiguity (two users share a first
	// name) and refuse to rewrite.
	names   map[string]map[string]struct{}
	fetched time.Time
	loadErr error
	// Tests inject a stub here to avoid hitting *slack.Client. Production
	// leaves it nil and the resolver calls client.GetUsersContext.
	fetchFn func(ctx context.Context) ([]slack.User, error)
}

func newUserResolver(client *slack.Client, ttl time.Duration) *userResolver {
	return &userResolver{client: client, ttl: ttl}
}

// Lookup resolves a (lowercase or mixed-case) name to a single user ID.
// Returns lookupFound + ID for an unambiguous match, lookupAmbiguous if
// >1 user matches, lookupNotFound if no user matches, lookupError if the
// users.list fetch itself failed.
func (r *userResolver) Lookup(ctx context.Context, name string) (string, lookupStatus) {
	if name == "" {
		return "", lookupNotFound
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.names == nil || time.Since(r.fetched) > r.ttl {
		if err := r.refreshLocked(ctx); err != nil {
			return "", lookupError
		}
	}

	ids, ok := r.names[strings.ToLower(strings.TrimSpace(name))]
	if !ok || len(ids) == 0 {
		return "", lookupNotFound
	}
	if len(ids) > 1 {
		return "", lookupAmbiguous
	}
	for id := range ids {
		return id, lookupFound
	}
	return "", lookupNotFound // unreachable
}

// refreshLocked rebuilds the names map. Caller must hold r.mu.
func (r *userResolver) refreshLocked(ctx context.Context) error {
	users, err := r.fetchUsers(ctx)
	if err != nil {
		r.loadErr = err
		return err
	}
	m := make(map[string]map[string]struct{}, len(users)*3)
	add := func(key, id string) {
		key = strings.ToLower(strings.TrimSpace(key))
		if key == "" {
			return
		}
		set, ok := m[key]
		if !ok {
			set = make(map[string]struct{}, 1)
			m[key] = set
		}
		set[id] = struct{}{}
	}
	for _, u := range users {
		if u.Deleted || u.IsBot {
			// Skip bots and tombstones — a user asking "from:erica" is
			// asking about a human. Including bots would also collide
			// with the many "slackbot" / app-name display names.
			continue
		}
		add(u.Name, u.ID)
		add(u.RealName, u.ID)
		add(u.Profile.DisplayName, u.ID)
		add(u.Profile.RealName, u.ID)
		add(u.Profile.DisplayNameNormalized, u.ID)
		add(u.Profile.RealNameNormalized, u.ID)
		// First token of the real name lets "from:erica" resolve a
		// user whose full Slack name is "Erica Gregor". Same for
		// display_name in case it's "Erica G.".
		add(firstWord(u.RealName), u.ID)
		add(firstWord(u.Profile.DisplayName), u.ID)
	}
	r.names = m
	r.fetched = time.Now()
	r.loadErr = nil
	return nil
}

func (r *userResolver) fetchUsers(ctx context.Context) ([]slack.User, error) {
	if r.fetchFn != nil {
		return r.fetchFn(ctx)
	}
	return r.client.GetUsersContext(ctx)
}

// firstWord returns the leading whitespace-delimited token of s, or ""
// if s is empty.
func firstWord(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if i := strings.IndexAny(s, " \t"); i > 0 {
		return s[:i]
	}
	return s
}
