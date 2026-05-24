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

// Package server implements proto.AgentService for the websearch_agent.
// Each Connect RPC makes a single Gemini call with Tools=[GoogleSearch{}]
// only — no FunctionDeclarations — and streams one AgentResponse with
// the grounded text plus a Sources: footer of web citations.
//
// This is the agent-as-tool workaround for Vertex Enterprise's
// "Multiple tools are supported only when they are all search tools"
// restriction: the planner CAN'T pass GoogleSearch and our subagent
// function declarations in the same Gemini call, so we register a
// `websearch` subagent that makes a SECOND Gemini call internally with
// just GoogleSearch.
package server

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"google.golang.org/genai"
	"google.golang.org/grpc"

	"github.com/google/ax/proto"
)

// botMentionPrefix matches a leading Slack user mention like "<@U09R4QH2C4D>"
// (optionally followed by whitespace). We strip this so the search query
// doesn't include the mention of the bot itself.
var botMentionPrefix = regexp.MustCompile(`^<@[UW][A-Z0-9]+>\s*`)

// genaiClient is the small slice of *genai.Client.Models the websearch
// agent uses. Defined as an interface so tests can inject a fake that
// returns canned *genai.GenerateContentResponse values without touching
// the network. The real implementation is a thin adapter over
// *genai.Client.Models — see realClient below.
type genaiClient interface {
	GenerateContent(ctx context.Context, model string, contents []*genai.Content, cfg *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error)
}

// realClient adapts *genai.Client.Models to the genaiClient interface.
type realClient struct {
	client *genai.Client
}

func (r *realClient) GenerateContent(ctx context.Context, model string, contents []*genai.Content, cfg *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
	return r.client.Models.GenerateContent(ctx, model, contents, cfg)
}

// Server is the AgentService implementation. The genai client is built
// once at New time and reused across Connect calls — *genai.Client is
// safe for concurrent use.
type Server struct {
	proto.UnimplementedAgentServiceServer

	client genaiClient
	model  string
	// now is the wall-clock source for "today" in the SystemInstruction.
	// Defaults to time.Now; tests can stub it.
	now func() time.Time
}

// New builds a Server backed by a real *genai.Client constructed via
// genai.NewClient with the BackendVertexAI backend. project and location
// are the Vertex project + region (e.g. "official-unofficial" / "global").
// model is the Gemini model id to call (e.g. "gemini-3-flash-preview").
func New(ctx context.Context, project, location, model string) (*Server, error) {
	if project == "" {
		return nil, errors.New("project is required")
	}
	if location == "" {
		return nil, errors.New("location is required")
	}
	if model == "" {
		return nil, errors.New("model is required")
	}
	client, err := genai.NewClient(ctx, &genai.ClientConfig{
		Backend:  genai.BackendVertexAI,
		Project:  project,
		Location: location,
	})
	if err != nil {
		return nil, fmt.Errorf("create genai client: %w", err)
	}
	return &Server{client: &realClient{client: client}, model: model, now: time.Now}, nil
}

// NewWithClient is the test constructor: it skips genai.NewClient and
// takes an already-built genaiClient. Used by server_test.go to inject
// a fakeGenai.
func NewWithClient(client genaiClient, model string) *Server {
	return &Server{client: client, model: model, now: time.Now}
}

// Connect implements proto.AgentService. Single-turn: read the query
// (preferring AgentStart.subagent_prompt, falling back to the last user
// message + envelope strip), call Gemini with GoogleSearch grounding,
// stream one AgentResponse with the formatted body.
func (s *Server) Connect(req *proto.AgentRequest, stream grpc.ServerStreamingServer[proto.AgentResponse]) error {
	start := req.GetStart()
	if start == nil {
		return errors.New("AgentRequest.Start is required")
	}

	// Prefer the modern structured AgentStart.subagent_prompt field
	// (gemini_planner forwards Gemini's typed `prompt` arg directly).
	// Fall back to the legacy "History Summary:\n…\nPrompt:\n…" envelope
	// in messages[] for direct gRPC callers / pre-structured-field
	// planners.
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

	body := s.search(stream.Context(), query)

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

// search runs the Gemini call with GoogleSearch grounding and formats
// the response. Errors from the Gemini call are folded into the body so
// the caller gets a single human-readable AgentResponse rather than a
// gRPC failure.
func (s *Server) search(ctx context.Context, query string) string {
	if query == "" {
		return "Please provide a search query."
	}

	// Ground Gemini in the present so it doesn't quote stale web
	// content as if it were current. Without this hint, Gemini's
	// GoogleSearch happily returns October 2024 press releases for an
	// "Anthropic news" query and the synthesis writes "Anthropic has
	// had a busy October 2024 …" — verified empirically.
	today := s.now().UTC().Format("Monday, January 2, 2006")
	cfg := &genai.GenerateContentConfig{
		// EXACTLY one Tool, with GoogleSearch set and NO
		// FunctionDeclarations. Vertex rejects mixed tool kinds in the
		// same call ("Multiple tools are supported only when they are
		// all search tools" — 400 INVALID_ARGUMENT). This is the whole
		// reason the websearch_agent exists as a separate subagent.
		Tools: []*genai.Tool{{
			GoogleSearch: &genai.GoogleSearch{},
		}},
		SystemInstruction: &genai.Content{
			Parts: []*genai.Part{{Text: fmt.Sprintf(
				"Today is %s. When answering from search results, ground "+
					"in this date. Prefer the most recent sources. Do NOT "+
					"refer to past events as 'recent' or 'today' if their "+
					"publication date is older than the current month.",
				today,
			)}},
		},
	}

	resp, err := s.client.GenerateContent(ctx, s.model, genai.Text(query), cfg)
	if err != nil {
		return fmt.Sprintf("Web search failed: %v", err)
	}
	return formatGroundedResponse(resp)
}

// formatGroundedResponse turns a Gemini response into the final body
// the planner sees. Shape:
//
//	<grounded text>
//
//	Sources:
//	1. <title> — <url>
//	2. ...
//
// The Sources: footer is omitted when no grounding chunks came back
// (e.g. the model answered from parametric knowledge, or grounding was
// silently dropped).
func formatGroundedResponse(resp *genai.GenerateContentResponse) string {
	if resp == nil || len(resp.Candidates) == 0 {
		return "No response from Gemini."
	}
	cand := resp.Candidates[0]

	// Concatenate all text parts. Gemini usually returns a single Part
	// for grounded answers but the API allows multiple.
	var body strings.Builder
	if cand.Content != nil {
		for _, p := range cand.Content.Parts {
			if p == nil {
				continue
			}
			body.WriteString(p.Text)
		}
	}
	text := strings.TrimSpace(body.String())
	if text == "" {
		text = "(Gemini returned no text content.)"
	}

	citations := webCitations(cand.GroundingMetadata)
	if len(citations) == 0 {
		return text
	}

	var out strings.Builder
	out.WriteString(text)
	out.WriteString("\n\nSources:")
	for i, c := range citations {
		fmt.Fprintf(&out, "\n%d. %s — %s", i+1, c.title, c.uri)
	}
	return out.String()
}

type webCitation struct {
	title string
	uri   string
}

// webCitations extracts (title, uri) pairs from the GroundingMetadata's
// web chunks. Non-web chunks (Maps, RetrievedContext, …) are ignored —
// this agent only does open-web search.
//
// When a chunk has no Title we fall back to the URI as the display
// label so the footer never has a dangling em-dash.
func webCitations(meta *genai.GroundingMetadata) []webCitation {
	if meta == nil {
		return nil
	}
	out := make([]webCitation, 0, len(meta.GroundingChunks))
	for _, c := range meta.GroundingChunks {
		if c == nil || c.Web == nil {
			continue
		}
		title := c.Web.Title
		if title == "" {
			title = c.Web.URI
		}
		out = append(out, webCitation{title: title, uri: c.Web.URI})
	}
	return out
}

// stripBotMention removes a leading "<@Uxxxx>" prefix and trims whitespace.
func stripBotMention(s string) string {
	s = strings.TrimSpace(s)
	s = botMentionPrefix.ReplaceAllString(s, "")
	return strings.TrimSpace(s)
}

// stripAXHistoryEnvelope unwraps the planner-synthesized envelope that
// AX passes to subagents:
//
//	History Summary:
//	<...stringified history...>
//
//	Prompt:
//	<the planner's actual subagent prompt>
//
// (see internal/gemini/gemini_planner.go ~line 362). If the trailing
// "Prompt:\n…" delimiter is present, return everything after it
// (whitespace-trimmed). Otherwise return s unchanged so direct (non-AX)
// callers still work.
func stripAXHistoryEnvelope(s string) string {
	const delim = "\nPrompt:\n"
	if i := strings.LastIndex(s, delim); i >= 0 {
		return strings.TrimSpace(s[i+len(delim):])
	}
	return s
}

// lastUserText returns the text content of the most recent user-role
// message, or ("", false) if no such message exists.
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
