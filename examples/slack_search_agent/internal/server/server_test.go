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
	"net/url"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/google/ax/proto"
)

// fakeHTTPClient records requests and returns canned JSON responses.
type fakeHTTPClient struct {
	requests []*http.Request
	body     string
	status   int
	err      error
}

func (f *fakeHTTPClient) Do(req *http.Request) (*http.Response, error) {
	f.requests = append(f.requests, req)
	if f.err != nil {
		return nil, f.err
	}
	status := f.status
	if status == 0 {
		status = 200
	}
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(f.body)),
		Header:     make(http.Header),
	}, nil
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

// readAll consumes the stream and returns the assistant text from the
// first response message.
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
        "is_author_bot": false,
        "reply_count": 0
      },
      {
        "author_name": "bob",
        "author_user_id": "U222",
        "channel_id": "C222",
        "channel_name": "random",
        "message_ts": "1700000100.000200",
        "content": "lunch at noon",
        "permalink": "https://example.slack.com/archives/C222/p1700000100000200",
        "is_author_bot": false,
        "reply_count": 2
      }
    ],
    "files": [],
    "channels": [],
    "users": []
  }
}`

const zeroMatchBody = `{"ok":true,"results":{"messages":[],"files":[],"channels":[],"users":[]}}`

const slackErrorBody = `{"ok":false,"error":"invalid_auth"}`

func TestConnect_QueriesSlackWithLatestUserText(t *testing.T) {
	fake := &fakeHTTPClient{body: zeroMatchBody}
	srv := New("xoxp-test-token", WithHTTPClient(fake))
	client := newTestClient(t, srv)

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

	if len(fake.requests) != 1 {
		t.Fatalf("expected 1 HTTP call, got %d", len(fake.requests))
	}
	req := fake.requests[0]
	if req.Method != http.MethodPost {
		t.Errorf("method = %q, want POST", req.Method)
	}
	if req.URL.Host != "slack.com" {
		t.Errorf("host = %q, want slack.com", req.URL.Host)
	}
	if req.URL.Path != "/api/assistant.search.context" {
		t.Errorf("path = %q, want /api/assistant.search.context", req.URL.Path)
	}
	if got := req.Header.Get("Content-Type"); got != "application/x-www-form-urlencoded" {
		t.Errorf("Content-Type = %q, want application/x-www-form-urlencoded", got)
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		t.Fatalf("parse form: %v", err)
	}
	if got := form.Get("query"); got != "project alpha launch" {
		t.Errorf("form query = %q, want %q", got, "project alpha launch")
	}
	if got := req.Header.Get("Authorization"); got != "Bearer xoxp-test-token" {
		t.Errorf("Authorization = %q, want Bearer xoxp-test-token", got)
	}
}

func TestConnect_FormatsMatches(t *testing.T) {
	fake := &fakeHTTPClient{body: twoMatchBody}
	srv := New("xoxp-test", WithHTTPClient(fake))
	client := newTestClient(t, srv)

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
	fake := &fakeHTTPClient{body: zeroMatchBody}
	srv := New("xoxp-test", WithHTTPClient(fake))
	client := newTestClient(t, srv)

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
	fake := &fakeHTTPClient{body: slackErrorBody}
	srv := New("xoxp-bad", WithHTTPClient(fake))
	client := newTestClient(t, srv)

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
	srv := New("xoxp-test", WithHTTPClient(&fakeHTTPClient{body: zeroMatchBody}))
	client := newTestClient(t, srv)

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if _, err := stream.Recv(); err == nil {
		t.Fatal("expected error for missing Start")
	}
}

// TestConnect_SortsByTimestampDescending locks in that the Slack call
// requests recency-sorted results (sort=timestamp&sort_dir=desc).
// Default semantic ranking misses recent short messages — e.g. a query
// for "latest thing Erica said" returned Erica's substantive May 9-20
// messages but never surfaced her May 22 "bonjour @Uno" reply because
// the short greeting ranked low semantically.
//
// Recency-first is the right default for an assistant: the LLM consumer
// gets the 10 most recent matches and can decide which are relevant.
func TestConnect_SortsByTimestampDescending(t *testing.T) {
	fake := &fakeHTTPClient{body: zeroMatchBody}
	srv := New("xoxp-test", WithHTTPClient(fake))
	client := newTestClient(t, srv)

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{Messages: []*proto.Message{userMessage("anything")}},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_ = readAssistantText(t, stream)

	if len(fake.requests) != 1 {
		t.Fatalf("expected 1 HTTP call, got %d", len(fake.requests))
	}
	body, err := io.ReadAll(fake.requests[0].Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		t.Fatalf("parse form: %v", err)
	}
	if got := form.Get("sort"); got != "timestamp" {
		t.Errorf("sort = %q, want %q", got, "timestamp")
	}
	if got := form.Get("sort_dir"); got != "desc" {
		t.Errorf("sort_dir = %q, want %q", got, "desc")
	}
}

// TestConnect_StripsAXHistoryEnvelope locks in that when AX's planner
// invokes this subagent with the synthesized
//
//	History Summary:
//	user: <original user prompt>
//
//	Prompt:
//	<subagent prompt arg>
//
// envelope (gemini_planner.go ~line 362), we send ONLY the trailing
// "Prompt:" body to Slack's search.context — not the whole envelope.
// Otherwise the search query becomes the literal multi-line envelope
// string and Slack returns garbage / semantic noise.
//
// Empirically discovered: a "What is the latest thing Erica said?"
// turn produced a Slack search query of
// "History Summary:\nuser: What is the latest thing Erica said?\n\n\nPrompt:\nlatest thing Erica said"
// which returned older results than what Erica had actually posted
// most recently.
func TestConnect_StripsAXHistoryEnvelope(t *testing.T) {
	fake := &fakeHTTPClient{body: zeroMatchBody}
	srv := New("xoxp-test", WithHTTPClient(fake))
	client := newTestClient(t, srv)

	envelope := "History Summary:\nuser: What is the latest thing Erica said?\n\n\nPrompt:\nlatest from Erica"
	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{
			Messages: []*proto.Message{userMessage(envelope)},
		},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_ = readAssistantText(t, stream)

	if len(fake.requests) != 1 {
		t.Fatalf("expected 1 HTTP call, got %d", len(fake.requests))
	}
	body, err := io.ReadAll(fake.requests[0].Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		t.Fatalf("parse form: %v", err)
	}
	got := form.Get("query")
	if got != "latest from Erica" {
		t.Errorf("Slack query = %q, want %q (envelope should be stripped, only the trailing Prompt: body sent)", got, "latest from Erica")
	}
	if strings.Contains(got, "History Summary") {
		t.Errorf("Slack query still contains 'History Summary' header: %q", got)
	}
}

func TestConnect_StripsBotMention(t *testing.T) {
	fake := &fakeHTTPClient{body: zeroMatchBody}
	srv := New("xoxp-test", WithHTTPClient(fake))
	client := newTestClient(t, srv)

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{
			Messages: []*proto.Message{
				userMessage("<@U09R4QH2C4D> what did we say about X"),
			},
		},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_ = readAssistantText(t, stream)

	if len(fake.requests) != 1 {
		t.Fatalf("expected 1 HTTP call, got %d", len(fake.requests))
	}
	body, err := io.ReadAll(fake.requests[0].Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		t.Fatalf("parse form: %v", err)
	}
	if got := form.Get("query"); got != "what did we say about X" {
		t.Errorf("query = %q, want %q", got, "what did we say about X")
	}
}
