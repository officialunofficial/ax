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
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/genai"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/google/ax/proto"
)

// fakeGenai is an in-process stub for the genaiClient interface. It
// captures the args of the most recent GenerateContent call and returns
// a configurable response so tests can assert on what was actually sent
// to Vertex without making a real network call.
type fakeGenai struct {
	mu sync.Mutex

	// Capture.
	lastModel    string
	lastContents []*genai.Content
	lastConfig   *genai.GenerateContentConfig
	calls        int

	// Configurable response. If respErr is non-nil it's returned as-is.
	resp    *genai.GenerateContentResponse
	respErr error
}

func (f *fakeGenai) GenerateContent(_ context.Context, model string, contents []*genai.Content, cfg *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastModel = model
	f.lastContents = contents
	f.lastConfig = cfg
	if f.respErr != nil {
		return nil, f.respErr
	}
	return f.resp, nil
}

func (f *fakeGenai) snapshot() (model string, contents []*genai.Content, cfg *genai.GenerateContentConfig, calls int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastModel, f.lastContents, f.lastConfig, f.calls
}

// textOnlyResponse builds a GenerateContentResponse with a single text
// candidate and no grounding metadata. The text is used as-is.
func textOnlyResponse(text string) *genai.GenerateContentResponse {
	return &genai.GenerateContentResponse{
		Candidates: []*genai.Candidate{{
			Content: &genai.Content{
				Parts: []*genai.Part{{Text: text}},
			},
			FinishReason: genai.FinishReasonStop,
		}},
	}
}

// groundedResponse builds a response with text + N grounding web chunks.
func groundedResponse(text string, chunks []*genai.GroundingChunkWeb) *genai.GenerateContentResponse {
	out := make([]*genai.GroundingChunk, len(chunks))
	for i, c := range chunks {
		out[i] = &genai.GroundingChunk{Web: c}
	}
	return &genai.GenerateContentResponse{
		Candidates: []*genai.Candidate{{
			Content: &genai.Content{
				Parts: []*genai.Part{{Text: text}},
			},
			FinishReason: genai.FinishReasonStop,
			GroundingMetadata: &genai.GroundingMetadata{
				GroundingChunks: out,
			},
		}},
	}
}

// newTestServer builds a Server with the given fake genai client.
// model is the model name the Server should pass to GenerateContent.
func newTestServer(client genaiClient, model string) *Server {
	return NewWithClient(client, model)
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

func userMessage(text string) *proto.Message {
	return &proto.Message{
		Role: "user",
		Content: &proto.Content{
			Type: &proto.Content_Text{Text: &proto.TextContent{Text: text}},
		},
	}
}

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

// TestConnect_SendsGoogleSearchToolOnly is the load-bearing assertion:
// the websearch agent MUST call Gemini with exactly one Tool entry, that
// TestConnect_GroundsAnswerInCurrentDate locks in the fix for a real
// smoke: Gemini's GoogleSearch returned old web content (October 2024
// Anthropic releases) and the bot wrote "Anthropic has had a very busy
// October 2024" — stale content presented as current. Separately for a
// weather query the bot invented a date "today is May 24, 2026" when
// the actual day was May 23.
//
// Fix: pass today's date in the SystemInstruction so Gemini grounds the
// answer in the present and doesn't quote stale content as recent.
func TestConnect_GroundsAnswerInCurrentDate(t *testing.T) {
	fg := &fakeGenai{resp: textOnlyResponse("ok")}
	client := newTestClient(t, newTestServer(fg, "gemini-3-flash-preview"))
	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{SubagentPrompt: "anything"},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_ = readAssistantText(t, stream)

	_, _, cfg, _ := fg.snapshot()
	if cfg == nil || cfg.SystemInstruction == nil {
		t.Fatal("expected a SystemInstruction (needed so Gemini grounds in today's date)")
	}
	var combined string
	for _, p := range cfg.SystemInstruction.Parts {
		combined += p.Text
	}
	year := fmt.Sprintf("%d", time.Now().UTC().Year())
	if !strings.Contains(combined, year) {
		t.Errorf("SystemInstruction = %q; expected current year %q so Gemini grounds in the present", combined, year)
	}
	if !strings.Contains(strings.ToLower(combined), "today") {
		t.Errorf("SystemInstruction = %q; expected a 'today' hint so Gemini doesn't quote old content as recent", combined)
	}
}

// Tool's GoogleSearch MUST be set, and NO FunctionDeclarations may be
// attached. Mixing the two trips Vertex's "Multiple tools are supported
// only when they are all search tools" 400 (verified live).
func TestConnect_SendsGoogleSearchToolOnly(t *testing.T) {
	fg := &fakeGenai{resp: textOnlyResponse("ok")}
	client := newTestClient(t, newTestServer(fg, "gemini-3-flash-preview"))

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{SubagentPrompt: "weather in SF"},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_ = readAssistantText(t, stream)

	_, _, cfg, calls := fg.snapshot()
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
	if cfg == nil {
		t.Fatal("config nil")
	}
	if len(cfg.Tools) != 1 {
		t.Fatalf("len(Tools) = %d, want 1", len(cfg.Tools))
	}
	tool := cfg.Tools[0]
	if tool.GoogleSearch == nil {
		t.Error("Tools[0].GoogleSearch is nil; must be set")
	}
	if len(tool.FunctionDeclarations) != 0 {
		t.Errorf("Tools[0] has %d FunctionDeclarations; want 0 (Vertex rejects FD+search in same call)", len(tool.FunctionDeclarations))
	}
}

func TestConnect_UsesGemini3FlashPreviewByDefault(t *testing.T) {
	fg := &fakeGenai{resp: textOnlyResponse("ok")}
	client := newTestClient(t, newTestServer(fg, "gemini-3-flash-preview"))

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{SubagentPrompt: "anything"},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_ = readAssistantText(t, stream)

	model, _, _, _ := fg.snapshot()
	if model != "gemini-3-flash-preview" {
		t.Errorf("model = %q, want gemini-3-flash-preview", model)
	}
}

func TestConnect_FormatsGroundedTextAndCitations(t *testing.T) {
	fg := &fakeGenai{
		resp: groundedResponse(
			"It is 68F and sunny in San Francisco.",
			[]*genai.GroundingChunkWeb{
				{Title: "Weather.gov SF", URI: "https://weather.gov/sf"},
				{Title: "AccuWeather SF", URI: "https://accuweather.com/sf"},
			},
		),
	}
	client := newTestClient(t, newTestServer(fg, "gemini-3-flash-preview"))

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{SubagentPrompt: "weather in SF"},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	body := readAssistantText(t, stream)

	wants := []string{
		"It is 68F and sunny in San Francisco.",
		"Sources:",
		"Weather.gov SF",
		"https://weather.gov/sf",
		"AccuWeather SF",
		"https://accuweather.com/sf",
	}
	for _, w := range wants {
		if !strings.Contains(body, w) {
			t.Errorf("body missing %q\nfull body:\n%s", w, body)
		}
	}
}

func TestConnect_HandlesEmptyGroundingMetadata(t *testing.T) {
	fg := &fakeGenai{resp: textOnlyResponse("The capital of France is Paris.")}
	client := newTestClient(t, newTestServer(fg, "gemini-3-flash-preview"))

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{SubagentPrompt: "capital of France"},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	body := readAssistantText(t, stream)

	if !strings.Contains(body, "The capital of France is Paris.") {
		t.Errorf("body missing text: %q", body)
	}
	if strings.Contains(body, "Sources:") {
		t.Errorf("body has Sources: footer despite no grounding chunks:\n%s", body)
	}
}

func TestConnect_PrefersStructuredSubagentPrompt(t *testing.T) {
	fg := &fakeGenai{resp: textOnlyResponse("ok")}
	client := newTestClient(t, newTestServer(fg, "gemini-3-flash-preview"))

	// Both fields populated. The structured prompt should win — the
	// envelope in messages[0] is deliberately different to prove it is
	// NOT consulted.
	envelope := "History Summary:\nuser: ignore me\n\nPrompt:\nignore me too"
	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{
			Messages:       []*proto.Message{userMessage(envelope)},
			SubagentPrompt: "Anthropic funding round 2026",
		},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_ = readAssistantText(t, stream)

	_, contents, _, _ := fg.snapshot()
	gotPrompt := extractPromptText(t, contents)
	if gotPrompt != "Anthropic funding round 2026" {
		t.Errorf("prompt = %q, want %q", gotPrompt, "Anthropic funding round 2026")
	}
}

func TestConnect_FallsBackToMessages(t *testing.T) {
	fg := &fakeGenai{resp: textOnlyResponse("ok")}
	client := newTestClient(t, newTestServer(fg, "gemini-3-flash-preview"))

	envelope := "History Summary:\nuser: who won the world cup 2026?\n\nPrompt:\nWorld Cup 2026 winner"
	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{
			Messages: []*proto.Message{userMessage(envelope)},
			// SubagentPrompt deliberately empty.
		},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_ = readAssistantText(t, stream)

	_, contents, _, _ := fg.snapshot()
	gotPrompt := extractPromptText(t, contents)
	if gotPrompt != "World Cup 2026 winner" {
		t.Errorf("prompt = %q, want %q (envelope should be stripped)", gotPrompt, "World Cup 2026 winner")
	}
}

func TestConnect_RejectsMissingStart(t *testing.T) {
	fg := &fakeGenai{resp: textOnlyResponse("ok")}
	client := newTestClient(t, newTestServer(fg, "gemini-3-flash-preview"))

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if _, err := stream.Recv(); err == nil {
		t.Fatal("expected error for missing Start")
	}
}

func TestConnect_HandlesGeminiError(t *testing.T) {
	fg := &fakeGenai{respErr: errors.New("vertex 503: upstream unavailable")}
	client := newTestClient(t, newTestServer(fg, "gemini-3-flash-preview"))

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{SubagentPrompt: "anything"},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	body := readAssistantText(t, stream)

	if !strings.Contains(body, "vertex 503") {
		t.Errorf("body should carry the gemini error as text; got %q", body)
	}
}

func TestConnect_StripsBotMention(t *testing.T) {
	fg := &fakeGenai{resp: textOnlyResponse("ok")}
	client := newTestClient(t, newTestServer(fg, "gemini-3-flash-preview"))

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{
			Messages: []*proto.Message{userMessage("<@U09R4QH2C4D> weather in SF")},
		},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_ = readAssistantText(t, stream)

	_, contents, _, _ := fg.snapshot()
	gotPrompt := extractPromptText(t, contents)
	if gotPrompt != "weather in SF" {
		t.Errorf("prompt = %q, want %q (bot mention should be stripped)", gotPrompt, "weather in SF")
	}
}

// extractPromptText pulls the user-role Text out of the genai contents
// that the Server passed to GenerateContent. Test helper — we just want
// to know what text was sent to Gemini, regardless of the (single)
// Content/Part shape.
func extractPromptText(t *testing.T, contents []*genai.Content) string {
	t.Helper()
	if len(contents) == 0 {
		t.Fatal("contents empty")
	}
	var b strings.Builder
	for _, c := range contents {
		for _, p := range c.Parts {
			b.WriteString(p.Text)
		}
	}
	return b.String()
}
