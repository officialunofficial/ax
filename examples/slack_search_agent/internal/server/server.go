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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"

	"github.com/google/ax/proto"
)

// slackSearchURL is the endpoint hit by every Connect call. Overridable
// via WithSlackBaseURL for tests that want to point at a local fake.
const slackSearchURL = "https://slack.com/api/search.messages"

// maxMatchTextLen caps each match body to keep responses scannable.
const maxMatchTextLen = 200

// botMentionPrefix matches a leading Slack user mention like "<@U09R4QH2C4D>"
// (optionally followed by whitespace). We strip this so the search query
// doesn't include the mention of the bot itself.
var botMentionPrefix = regexp.MustCompile(`^<@[UW][A-Z0-9]+>\s*`)

// Server is the AgentService implementation. Each Connect RPC takes the
// latest user-message text, queries Slack's search.messages API, and
// streams one human-readable AgentResponse back.
type Server struct {
	proto.UnimplementedAgentServiceServer

	slackToken   string
	slackBaseURL string
	http         httpClient
}

// httpClient is the subset of *http.Client we need. Lets tests inject a
// fake without spinning up an httptest.Server.
type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// New builds a Server. slackToken must be a Slack user token (xoxp-...)
// with the search:read.* scopes. Bot tokens cannot call search.messages.
func New(slackToken string, opts ...Option) *Server {
	s := &Server{
		slackToken:   slackToken,
		slackBaseURL: slackSearchURL,
		http:         &http.Client{Timeout: 15 * time.Second},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Option configures a Server at construction.
type Option func(*Server)

// WithHTTPClient injects a custom httpClient (tests).
func WithHTTPClient(c httpClient) Option {
	return func(s *Server) { s.http = c }
}

// WithSlackBaseURL overrides the search endpoint (tests / staging).
func WithSlackBaseURL(u string) Option {
	return func(s *Server) { s.slackBaseURL = u }
}

// Connect implements proto.AgentService. Single-turn: read the last user
// message, run a Slack search, stream one AgentResponse with the
// formatted top-10 matches.
func (s *Server) Connect(req *proto.AgentRequest, stream grpc.ServerStreamingServer[proto.AgentResponse]) error {
	start := req.GetStart()
	if start == nil {
		return errors.New("AgentRequest.Start is required")
	}
	raw, ok := lastUserText(start.Messages)
	if !ok {
		return errors.New("no user message with text content found")
	}
	query := stripBotMention(raw)

	var body string
	if query == "" {
		body = "Please provide a search query."
	} else {
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

// search runs the Slack API call and returns the formatted body. Any
// transport / API errors are folded into the body itself so the caller
// gets a single human-readable response rather than a gRPC error.
func (s *Server) search(ctx context.Context, query string) string {
	u, err := url.Parse(s.slackBaseURL)
	if err != nil {
		return fmt.Sprintf("internal error: invalid slack URL: %v", err)
	}
	q := u.Query()
	q.Set("query", query)
	q.Set("count", "10")
	q.Set("sort", "timestamp")
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return fmt.Sprintf("internal error: build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+s.slackToken)
	req.Header.Set("Accept", "application/json")

	httpResp, err := s.http.Do(req)
	if err != nil {
		return fmt.Sprintf("slack request failed: %v", err)
	}
	defer httpResp.Body.Close()

	raw, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return fmt.Sprintf("slack read failed: %v", err)
	}

	var parsed searchResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return fmt.Sprintf("slack returned invalid JSON: %v\nbody: %s", err, truncate(string(raw), 500))
	}
	if !parsed.OK {
		errMsg := parsed.Error
		if errMsg == "" {
			errMsg = "unknown_error"
		}
		return fmt.Sprintf("Slack search failed: %s", errMsg)
	}
	return formatMatches(query, parsed.Messages.Matches)
}

// searchResponse models the bits of search.messages we use.
type searchResponse struct {
	OK       bool   `json:"ok"`
	Error    string `json:"error,omitempty"`
	Messages struct {
		Total   int          `json:"total"`
		Matches []slackMatch `json:"matches"`
	} `json:"messages"`
}

type slackMatch struct {
	Channel struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"channel"`
	Username  string `json:"username"`
	User      string `json:"user"`
	TS        string `json:"ts"`
	Text      string `json:"text"`
	Permalink string `json:"permalink"`
}

// formatMatches produces the human-readable body. Each match is its own
// block separated by a blank line; the LLM viewer can quote them naturally.
func formatMatches(query string, matches []slackMatch) string {
	if len(matches) == 0 {
		return fmt.Sprintf("No results for %q.", query)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Found %d messages for %q:\n\n", len(matches), query)
	for i, m := range matches {
		channelName := m.Channel.Name
		if channelName == "" {
			channelName = m.Channel.ID
		}
		who := m.Username
		if who == "" {
			who = m.User
		}
		if who == "" {
			who = "unknown"
		}
		fmt.Fprintf(&b, "%d. #%s by %s at %s\n", i+1, channelName, who, humanTS(m.TS))
		if m.Permalink != "" {
			fmt.Fprintf(&b, "   %s\n", m.Permalink)
		}
		fmt.Fprintf(&b, "   > %s\n", truncate(m.Text, maxMatchTextLen))
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
