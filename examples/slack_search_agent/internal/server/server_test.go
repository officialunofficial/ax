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

package server

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/slack-go/slack"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/google/ax/proto"
)

// fakeSlack is an httptest.Server that pretends to be slack.com. It
// understands the two endpoints this agent hits — `users.list` and
// `assistant.search.context` — and records the params of every call so
// tests can assert on the actual wire-level request body. slack-go's
// OptionAPIURL points the *slack.Client at this server.
type fakeSlack struct {
	srv *httptest.Server

	mu             sync.Mutex
	usersListCalls int
	searchCalls    int
	lastSearch     map[string]string // form values from the most recent search call

	// Configurable response bodies. Defaults are sane (one user, zero
	// matches) so tests only set what they care about.
	usersListBody string
	searchBody    string
}

func newFakeSlack() *fakeSlack {
	f := &fakeSlack{
		usersListBody: defaultUsersListBody,
		searchBody:    zeroMatchBody,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/users.list", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.usersListCalls++
		body := f.usersListBody
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	})
	mux.HandleFunc("/assistant.search.context", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		f.mu.Lock()
		f.searchCalls++
		f.lastSearch = make(map[string]string, len(r.PostForm))
		for k, v := range r.PostForm {
			if len(v) > 0 {
				f.lastSearch[k] = v[0]
			}
		}
		body := f.searchBody
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	})
	f.srv = httptest.NewServer(mux)
	return f
}

func (f *fakeSlack) close() { f.srv.Close() }

// apiURL returns the OptionAPIURL value to pass to slack.New. slack-go
// builds endpoints as `endpoint + path`, so the trailing slash is required.
func (f *fakeSlack) apiURL() string { return f.srv.URL + "/" }

func (f *fakeSlack) UsersListCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.usersListCalls
}

func (f *fakeSlack) SearchCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.searchCalls
}

func (f *fakeSlack) LastSearch() map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]string, len(f.lastSearch))
	for k, v := range f.lastSearch {
		out[k] = v
	}
	return out
}

// newTestServer builds a Server pointed at f. slack-go routes both its
// search calls and its users.list calls to f via OptionAPIURL.
func newTestServer(f *fakeSlack) *Server {
	return New("xoxp-test-token", WithSlackOptions(slack.OptionAPIURL(f.apiURL())))
}

// newTestClient spins up a Server on bufconn and returns an AgentService
// client wired to it.
func newTestClient(t *testing.T, srv *Server) proto.AgentServiceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	g := grpc.NewServer()
	proto.RegisterAgentServiceServer(g, srv)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return proto.NewAgentServiceClient(conn)
}

// userMessage builds a Message with text content under the "user" role.
func userMessage(text string) *proto.Message {
	return &proto.Message{
		Role: "user",
		Content: &proto.Content{
			Type: &proto.Content_Text{Text: &proto.TextContent{Text: text}},
		},
	}
}

// readAssistantText consumes one stream response and returns the
// assistant text.
func readAssistantText(t *testing.T, stream proto.AgentService_ConnectClient) string {
	t.Helper()
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	msgs := resp.GetOutputs().GetMessages()
	if len(msgs) == 0 {
		t.Fatalf("no messages in response")
	}
	return msgs[0].GetContent().GetText().Text
}

// Modern assistant.search.context response shape:
//
//	{ "ok": true, "results": { "messages": [...], "files": [], ... } }
const twoMatchBody = `{
  "ok": true,
  "results": {
    "messages": [
      {
        "author_name": "alice",
        "author_user_id": "U111",
        "channel_id": "C111",
        "channel_name": "general",
        "message_ts": "1700000000.000100",
        "content": "we agreed to ship friday",
        "permalink": "https://example.slack.com/archives/C111/p1700000000000100",
        "is_author_bot": false
      },
      {
        "author_name": "bob",
        "author_user_id": "U222",
        "channel_id": "C222",
        "channel_name": "random",
        "message_ts": "1700000100.000200",
        "content": "lunch at noon",
        "permalink": "https://example.slack.com/archives/C222/p1700000100000200",
        "is_author_bot": false
      }
    ],
    "files": [],
    "channels": [],
    "users": []
  }
}`

const zeroMatchBody = `{"ok":true,"results":{"messages":[],"files":[],"channels":[],"users":[]}}`

const slackErrorBody = `{"ok":false,"error":"invalid_auth"}`

// defaultUsersListBody is a minimal users.list response that any test
// not specifically exercising user resolution can fall back on. One
// non-bot user named "Erica Gregor".
const defaultUsersListBody = `{
  "ok": true,
  "members": [
    {
      "id": "U06L1HUGDCJ",
      "name": "erica",
      "real_name": "Erica Gregor",
      "deleted": false,
      "is_bot": false,
      "profile": {
        "real_name": "Erica Gregor",
        "real_name_normalized": "Erica Gregor",
        "display_name": "erica",
        "display_name_normalized": "erica"
      }
    }
  ],
  "response_metadata": {"next_cursor": ""}
}`

func TestConnect_QueriesSlackWithLatestUserText(t *testing.T) {
	f := newFakeSlack()
	defer f.close()
	client := newTestClient(t, newTestServer(f))

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{
			Messages: []*proto.Message{
				userMessage("ignored"),
				userMessage("project alpha launch"),
			},
		},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_ = readAssistantText(t, stream)

	if got := f.SearchCalls(); got != 1 {
		t.Fatalf("expected 1 search call, got %d", got)
	}
	form := f.LastSearch()
	if form["query"] != "project alpha launch" {
		t.Errorf("query = %q, want %q", form["query"], "project alpha launch")
	}
	// slack-go always includes the bearer token in the form body for
	// assistant.search.context — verify our token survived the slack-go
	// indirection.
	if form["token"] != "xoxp-test-token" {
		t.Errorf("token = %q, want xoxp-test-token", form["token"])
	}
}

func TestConnect_FormatsMatches(t *testing.T) {
	f := newFakeSlack()
	defer f.close()
	f.searchBody = twoMatchBody
	client := newTestClient(t, newTestServer(f))

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{Messages: []*proto.Message{userMessage("friday")}},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	text := readAssistantText(t, stream)

	wants := []string{
		`Found 2 messages for "friday"`,
		"#general",
		"alice",
		"we agreed to ship friday",
		"https://example.slack.com/archives/C111/p1700000000000100",
		"#random",
		"bob",
		"lunch at noon",
		"https://example.slack.com/archives/C222/p1700000100000200",
	}
	for _, w := range wants {
		if !strings.Contains(text, w) {
			t.Errorf("response missing %q\nfull body:\n%s", w, text)
		}
	}
}

func TestConnect_HandlesEmptyResults(t *testing.T) {
	f := newFakeSlack()
	defer f.close()
	client := newTestClient(t, newTestServer(f))

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{Messages: []*proto.Message{userMessage("zzzz")}},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	text := readAssistantText(t, stream)

	if !strings.Contains(text, "No results for") {
		t.Errorf("expected 'No results for' message, got %q", text)
	}
	if !strings.Contains(text, "zzzz") {
		t.Errorf("expected query echoed in response, got %q", text)
	}
}

func TestConnect_HandlesSlackError(t *testing.T) {
	f := newFakeSlack()
	defer f.close()
	f.searchBody = slackErrorBody
	client := newTestClient(t, newTestServer(f))

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{Messages: []*proto.Message{userMessage("anything")}},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	text := readAssistantText(t, stream)

	if !strings.Contains(text, "invalid_auth") {
		t.Errorf("expected error code in body, got %q", text)
	}
}

func TestConnect_RejectsMissingStart(t *testing.T) {
	f := newFakeSlack()
	defer f.close()
	client := newTestClient(t, newTestServer(f))

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if _, err := stream.Recv(); err == nil {
		t.Fatal("expected error for missing Start")
	}
}

// TestConnect_RecencyQueryUsesTimestampSort locks in the intent-based
// sort selection. Queries containing recency keywords ("latest",
// "recent", etc.) get sort=timestamp; everything else gets the default
// semantic sort=score.
func TestConnect_RecencyQueryUsesTimestampSort(t *testing.T) {
	f := newFakeSlack()
	defer f.close()
	client := newTestClient(t, newTestServer(f))

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{Messages: []*proto.Message{userMessage("latest deploy status")}},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_ = readAssistantText(t, stream)

	form := f.LastSearch()
	if form["sort"] != "timestamp" {
		t.Errorf("sort = %q, want timestamp", form["sort"])
	}
	if form["sort_dir"] != "desc" {
		t.Errorf("sort_dir = %q, want desc", form["sort_dir"])
	}
}

func TestConnect_SemanticQueryUsesScoreSort(t *testing.T) {
	f := newFakeSlack()
	defer f.close()
	client := newTestClient(t, newTestServer(f))

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{Messages: []*proto.Message{userMessage("decisions about makechain rollout")}},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_ = readAssistantText(t, stream)

	form := f.LastSearch()
	if form["sort"] != "score" {
		t.Errorf("sort = %q, want score", form["sort"])
	}
}

// TestFinalizeQuery_StripsFillerNotMeaningfulContent locks in the
// refined behavior: filler nouns ("message", "updates", "thing", …)
// get stripped because they're paraphrase artifacts from the planner,
// but real content scope ("kubernetes upgrade", "deploy") stays —
// dropping it discards legitimate user intent.
//
// History: the original fix dropped ALL non-filter tokens when
// sort=timestamp + any filter was present. That over-fired and
// erased queries like "latest from:erica kubernetes upgrade" down
// to just "from:erica", losing the kubernetes scope. The refined
// design strips a short filler list only.
func TestFinalizeQuery_StripsFillerNotMeaningfulContent(t *testing.T) {
	cases := []struct {
		in       string
		wantSort string
		wantQ    string
	}{
		// The original bug case: "message" is filler, so it goes,
		// and the from: filter remains.
		{"latest message from:<@U06L1HUGDCJ>", "timestamp", "from:<@U06L1HUGDCJ>"},
		// "kubernetes upgrade" is legitimate content scope — keep it.
		{"latest from:<@U06L1HUGDCJ> kubernetes upgrade", "timestamp", "from:<@U06L1HUGDCJ> kubernetes upgrade"},
		// "updates" is filler.
		{"recent updates in:<#C123>", "timestamp", "in:<#C123>"},
		// No recency: still strip filler so semantic search sees the
		// real signal (the from: filter), not paraphrase noise.
		{"message from:<@U06L1HUGDCJ>", "score", "from:<@U06L1HUGDCJ>"},
		// No filter, no filler: leave the only signal alone.
		{"newest deploy", "timestamp", "deploy"},
	}
	for _, tc := range cases {
		if gotSort := sortForQuery(tc.in); gotSort != tc.wantSort {
			t.Errorf("sortForQuery(%q) = %q, want %q", tc.in, gotSort, tc.wantSort)
		}
		if gotQ := finalizeQuery(tc.in); gotQ != tc.wantQ {
			t.Errorf("finalizeQuery(%q) = %q, want %q", tc.in, gotQ, tc.wantQ)
		}
	}
}

// TestSearch_StripsRecencyKeywordsFromQuery locks in the lesson from a
// real smoke: "latest" should drive sort=timestamp AND be removed from
// the query string. Without the strip, Slack matches messages
// containing the literal word "latest" (e.g. about Docker :latest
// tags), missing the user's actual intent.
func TestSearch_StripsRecencyKeywordsFromQuery(t *testing.T) {
	cases := []struct {
		in       string
		wantSort string
		wantQ    string
	}{
		{"latest from Erica", "timestamp", "from Erica"},
		{"most recent makechain decisions", "timestamp", "makechain decisions"},
		{"newest deploy", "timestamp", "deploy"},
		{"makechain decisions", "score", "makechain decisions"},
	}
	for _, tc := range cases {
		if gotSort := sortForQuery(tc.in); gotSort != tc.wantSort {
			t.Errorf("sortForQuery(%q) = %q, want %q", tc.in, gotSort, tc.wantSort)
		}
		if gotQ := stripRecencyKeywords(tc.in); gotQ != tc.wantQ {
			t.Errorf("stripRecencyKeywords(%q) = %q, want %q", tc.in, gotQ, tc.wantQ)
		}
	}
}

// TestSortForQuery_RecencyTriggersTimestamp covers the keyword
// detection in isolation — easier to extend than going through the
// whole gRPC stack for each phrase.
func TestSortForQuery_RecencyTriggersTimestamp(t *testing.T) {
	cases := []string{
		"latest from erica",
		"newest deploys",
		"most recent incident",
		"recent makechain decisions",
		"what happened today",
		"yesterday's standup notes",
		"last week deploys",
		"LATEST from erica (case-insensitive)",
	}
	for _, q := range cases {
		if got := sortForQuery(q); got != "timestamp" {
			t.Errorf("sortForQuery(%q) = %q, want timestamp", q, got)
		}
	}
}

func TestSortForQuery_SemanticDefault(t *testing.T) {
	cases := []string{
		"what did we decide about deploys",
		"makechain rollout discussion",
		"erica thoughts on validator sync",
		"",
	}
	for _, q := range cases {
		if got := sortForQuery(q); got != "score" {
			t.Errorf("sortForQuery(%q) = %q, want score", q, got)
		}
	}
}

// TestConnect_StripsAXHistoryEnvelope locks in that when AX's planner
// invokes this subagent with the synthesized History Summary / Prompt
// envelope (legacy contract), we send ONLY the trailing "Prompt:" body
// to Slack — not the whole envelope. Otherwise the search query becomes
// the literal multi-line envelope string and Slack returns garbage.
func TestConnect_StripsAXHistoryEnvelope(t *testing.T) {
	f := newFakeSlack()
	defer f.close()
	client := newTestClient(t, newTestServer(f))

	envelope := "History Summary:\nuser: What is the latest thing Erica said?\n\n\nPrompt:\nrecent decisions"
	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{Messages: []*proto.Message{userMessage(envelope)}},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_ = readAssistantText(t, stream)

	form := f.LastSearch()
	if form["query"] != "decisions" {
		t.Errorf("query = %q, want %q (recency keyword stripped)", form["query"], "decisions")
	}
	if strings.Contains(form["query"], "History Summary") {
		t.Errorf("query still contains 'History Summary': %q", form["query"])
	}
}

// TestConnect_PrefersStructuredSubagentPrompt locks in that when the AX
// planner populates the new structured AgentStart.subagent_prompt field
// (added to proto/ax.proto), the subagent uses it directly without
// touching messages[0] / the legacy envelope.
//
// This is the modern contract: the planner forwards Gemini's typed
// {history, prompt} function-call args as structured proto fields. The
// legacy "History Summary:\n…\nPrompt:\n…" envelope is still synthesized
// into messages[0] for backward compatibility, but modern subagents
// should ignore it when subagent_prompt is set.
func TestConnect_PrefersStructuredSubagentPrompt(t *testing.T) {
	f := newFakeSlack()
	defer f.close()
	client := newTestClient(t, newTestServer(f))

	// Both fields populated (matches what gemini_planner.go writes after
	// the structured-prompt change). The structured prompt should win —
	// the envelope in messages[0] is intentionally noisy / different to
	// prove it is NOT consulted.
	envelope := "History Summary:\nuser: ignore me\n\nPrompt:\nignore me too"
	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{
			Messages:        []*proto.Message{userMessage(envelope)},
			SubagentPrompt:  "recent decisions",
			SubagentHistory: "user: What is the latest thing Erica said?",
		},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_ = readAssistantText(t, stream)

	form := f.LastSearch()
	if form["query"] != "decisions" {
		t.Errorf("query = %q, want %q (structured subagent_prompt should win)", form["query"], "recent decisions")
	}
}

// TestConnect_FallsBackToMessagesWhenSubagentPromptEmpty locks in the
// backward-compat path: when the structured subagent_prompt is empty
// (legacy planner, direct caller, etc.), we still pull the query from
// messages[0] and strip the legacy envelope.
func TestConnect_FallsBackToMessagesWhenSubagentPromptEmpty(t *testing.T) {
	f := newFakeSlack()
	defer f.close()
	client := newTestClient(t, newTestServer(f))

	envelope := "History Summary:\nuser: What is the latest thing Erica said?\n\nPrompt:\nrecent decisions"
	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{
			Messages: []*proto.Message{userMessage(envelope)},
			// SubagentPrompt deliberately unset.
		},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_ = readAssistantText(t, stream)

	form := f.LastSearch()
	if form["query"] != "decisions" {
		t.Errorf("query = %q, want %q (legacy envelope fallback)", form["query"], "recent decisions")
	}
}

func TestConnect_StripsBotMention(t *testing.T) {
	f := newFakeSlack()
	defer f.close()
	client := newTestClient(t, newTestServer(f))

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{
			Messages: []*proto.Message{userMessage("<@U09R4QH2C4D> what did we say about X")},
		},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_ = readAssistantText(t, stream)

	form := f.LastSearch()
	if form["query"] != "what did we say about X" {
		t.Errorf("query = %q, want %q", form["query"], "what did we say about X")
	}
}

// TestConnect_ResolvesFromName covers the cache-backed user resolver:
// `from:erica` in the user's prompt becomes `from:<@U06L1HUGDCJ>` in
// the Slack call (the canonical filter syntax). The default users.list
// fixture has exactly one Erica, so the rewrite is unambiguous.
func TestConnect_ResolvesFromName(t *testing.T) {
	f := newFakeSlack()
	defer f.close()
	client := newTestClient(t, newTestServer(f))

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{Messages: []*proto.Message{userMessage("from:erica latest")}},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_ = readAssistantText(t, stream)

	form := f.LastSearch()
	if !strings.Contains(form["query"], "from:<@U06L1HUGDCJ>") {
		t.Errorf("query = %q, want it to contain from:<@U06L1HUGDCJ>", form["query"])
	}
	// "latest" is consumed for sort detection then STRIPPED from the
	// query so Slack doesn't match it as a literal keyword (real bug:
	// "latest" in the query matched messages about Docker :latest
	// tags). The sort still flips to timestamp.
	if strings.Contains(form["query"], "latest") {
		t.Errorf("query = %q; should NOT contain 'latest' after recency-strip", form["query"])
	}
	if form["sort"] != "timestamp" {
		t.Errorf("sort = %q, want timestamp (recency keyword present in original)", form["sort"])
	}
}

// TestConnect_ResolvesNaturalLanguageFromName covers the second
// resolution pass: "from Erica" / "by Erica" (no colon) get rewritten
// when the name is proper-noun-shaped.
func TestConnect_ResolvesNaturalLanguageFromName(t *testing.T) {
	f := newFakeSlack()
	defer f.close()
	client := newTestClient(t, newTestServer(f))

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{Messages: []*proto.Message{userMessage("what did Erica say about deploys")}},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_ = readAssistantText(t, stream)

	form := f.LastSearch()
	if !strings.Contains(form["query"], "from:<@U06L1HUGDCJ>") {
		t.Errorf("query = %q, want it to contain from:<@U06L1HUGDCJ>", form["query"])
	}
}

// TestConnect_LeavesUnresolvableNameUnchanged: an unknown name passes
// through verbatim. We do NOT make up a user ID, and we do NOT drop
// the from:filter (the planner can still use it as a plain-text search
// term).
func TestConnect_LeavesUnresolvableNameUnchanged(t *testing.T) {
	f := newFakeSlack()
	defer f.close()
	client := newTestClient(t, newTestServer(f))

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{Messages: []*proto.Message{userMessage("from:nonexistent foo")}},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_ = readAssistantText(t, stream)

	form := f.LastSearch()
	if form["query"] != "from:nonexistent foo" {
		t.Errorf("query = %q, want %q", form["query"], "from:nonexistent foo")
	}
}

// TestConnect_CachesUserList: two Connect calls in a row with the same
// resolver should fetch users.list exactly once. The second lookup
// must hit the in-memory cache.
func TestConnect_CachesUserList(t *testing.T) {
	f := newFakeSlack()
	defer f.close()
	srv := newTestServer(f)
	client := newTestClient(t, srv)

	for i := 0; i < 2; i++ {
		stream, err := client.Connect(context.Background(), &proto.AgentRequest{
			Start: &proto.AgentStart{Messages: []*proto.Message{userMessage("from:erica anything")}},
		})
		if err != nil {
			t.Fatalf("Connect[%d]: %v", i, err)
		}
		_ = readAssistantText(t, stream)
	}

	if got := f.UsersListCalls(); got != 1 {
		t.Errorf("users.list calls = %d, want 1 (second Connect should hit cache)", got)
	}
	if got := f.SearchCalls(); got != 2 {
		t.Errorf("search calls = %d, want 2", got)
	}
}

// TestConnect_HandlesDuplicateNames: when two users share a first name,
// resolution is ambiguous → the original token survives, no silent
// pick. This is the safety property — better to return slightly less
// relevant results than wrongly attribute messages to the wrong human.
func TestConnect_HandlesDuplicateNames(t *testing.T) {
	f := newFakeSlack()
	defer f.close()
	f.usersListBody = `{
      "ok": true,
      "members": [
        {
          "id": "U001",
          "name": "ericag",
          "real_name": "Erica Gregor",
          "deleted": false,
          "is_bot": false,
          "profile": {"real_name": "Erica Gregor", "display_name": "ericag"}
        },
        {
          "id": "U002",
          "name": "ericab",
          "real_name": "Erica Beck",
          "deleted": false,
          "is_bot": false,
          "profile": {"real_name": "Erica Beck", "display_name": "ericab"}
        }
      ],
      "response_metadata": {"next_cursor": ""}
    }`
	client := newTestClient(t, newTestServer(f))

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{Messages: []*proto.Message{userMessage("from:erica deploys")}},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_ = readAssistantText(t, stream)

	form := f.LastSearch()
	if form["query"] != "from:erica deploys" {
		t.Errorf("query = %q, want unchanged %q (ambiguous name must NOT be silently resolved)", form["query"], "from:erica deploys")
	}
}

// TestUserResolver_CacheRefreshAfterTTL: once the TTL elapses, the next
// Lookup must re-fetch. This is the eviction half of the cache
// contract — covered separately from the gRPC tests so we don't have
// to inflate the wall-clock TTL.
func TestUserResolver_CacheRefreshAfterTTL(t *testing.T) {
	var calls int32
	r := newUserResolver(nil, 10*time.Millisecond)
	r.fetchFn = func(ctx context.Context) ([]slack.User, error) {
		atomic.AddInt32(&calls, 1)
		return []slack.User{{
			ID:       "U999",
			Name:     "erica",
			RealName: "Erica Gregor",
		}}, nil
	}

	if _, st := r.Lookup(context.Background(), "erica"); st != lookupFound {
		t.Fatalf("first Lookup status = %v, want lookupFound", st)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("after first Lookup: fetch calls = %d, want 1", got)
	}

	// Within TTL → still 1 fetch.
	if _, st := r.Lookup(context.Background(), "erica"); st != lookupFound {
		t.Fatalf("cached Lookup status = %v, want lookupFound", st)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("within TTL: fetch calls = %d, want 1", got)
	}

	// Past TTL → second fetch.
	time.Sleep(15 * time.Millisecond)
	if _, st := r.Lookup(context.Background(), "erica"); st != lookupFound {
		t.Fatalf("post-TTL Lookup status = %v, want lookupFound", st)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("post-TTL: fetch calls = %d, want 2", got)
	}
}

// TestByNamePattern_RequiresProperNoun locks in that the
// from/by NAME regex only fires on a capitalized name, not on
// articles or filler nouns. The earlier `(?i)` flag made the
// character class `[A-Z]` match lowercase too, so "from the team"
// matched (capturing "the") and the proper-noun guard in the
// comment was a lie. Go RE2 applies (?i) to character classes
// as well — confirmed by https://pkg.go.dev/regexp/syntax.
func TestByNamePattern_RequiresProperNoun(t *testing.T) {
	mustNotMatch := []string{
		"what did we hear from the team about deploys",
		"any news by the team",
		"from the engineers",
		"by my manager",
	}
	for _, in := range mustNotMatch {
		if byNamePattern.MatchString(in) {
			t.Errorf("byNamePattern matched %q, want no match (lowercase noun)", in)
		}
	}
	mustMatch := []string{
		"from Erica",
		"by Erica",
		"FROM Erica",
		"By Erica",
		"updates from Erica yesterday",
	}
	for _, in := range mustMatch {
		if !byNamePattern.MatchString(in) {
			t.Errorf("byNamePattern did not match %q, want match (proper noun)", in)
		}
	}
}

// TestDidNamePattern_RequiresProperNoun is the same guard for the
// "did NAME say/post/…" shape. Most lowercase-after-did phrases are
// already deflected by the trailing verb constraint, but the (?i)
// flag still lets "did the post say" through (captures "the", verb
// "post"). The proper-noun-only fix kills that path.
func TestDidNamePattern_RequiresProperNoun(t *testing.T) {
	mustNotMatch := []string{
		"what did the boss say about deploys",
		"did the team post the update",
		"did our manager mention the release",
		// Triggers the (?i) bug: captured name = "the", verb = "post".
		// Without the fix this matches because [A-Z] is case-insensitive
		// under (?i) and matches lowercase "the".
		"did the post say it",
	}
	for _, in := range mustNotMatch {
		if didNamePattern.MatchString(in) {
			t.Errorf("didNamePattern matched %q, want no match (lowercase noun)", in)
		}
	}
	mustMatch := []string{
		"what did Erica say",
		"did Erica post the recap",
		"Did Erica mention the deploy",
		"DID Erica share the link",
	}
	for _, in := range mustMatch {
		if !didNamePattern.MatchString(in) {
			t.Errorf("didNamePattern did not match %q, want match (proper noun)", in)
		}
	}
}

// TestTruncate_HandlesMultibyteUTF8 locks in that truncate() clips by
// runes, not bytes. Before the fix, truncate("…foo bar — baz qux", 18)
// could land inside the em-dash's UTF-8 byte sequence and emit invalid
// UTF-8 (which then breaks downstream JSON encoders and viewers).
func TestTruncate_HandlesMultibyteUTF8(t *testing.T) {
	cases := []struct {
		in  string
		max int
	}{
		// Em-dash near the boundary.
		{"foo bar — baz qux quux", 10},
		{"foo bar — baz qux quux", 9},
		{"foo bar — baz qux quux", 8},
		// Emoji near the boundary (4-byte UTF-8).
		{"deploy 🚀 finally green", 9},
		{"deploy 🚀 finally green", 8},
		// Accented characters.
		{"café société naïve", 6},
		// Short input — no truncation needed, must still be valid.
		{"é", 5},
		// Edge: max=1.
		{"hello", 1},
	}
	for _, tc := range cases {
		out := truncate(tc.in, tc.max)
		if !utf8.ValidString(out) {
			t.Errorf("truncate(%q, %d) = %q (invalid UTF-8)", tc.in, tc.max, out)
		}
		if got := utf8.RuneCountInString(out); got > tc.max {
			t.Errorf("truncate(%q, %d) = %q has %d runes, want <= %d", tc.in, tc.max, out, got, tc.max)
		}
	}
}
