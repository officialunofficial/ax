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

// Command slack_search_agent serves AX's AgentService gRPC on :8494. On
// every Connect call it takes the latest user message text as a search
// query, calls Slack's search.messages API, and streams a single
// human-readable AgentResponse containing the top results.
//
// This is the user-code half of an AX remote-agent example. AX's
// controller dials this server via RemoteAgent (configured under
// `remote_agents:` in ax.yaml).
//
// Image: us-east4-docker.pkg.dev/official-unofficial/docker/slack-search-agent
// Built by examples/slack_search_agent/Dockerfile via Cloud Build.
package main

import (
	"log"
	"net"
	"os"

	"google.golang.org/grpc"

	"github.com/google/ax/examples/slack_search_agent/internal/server"
	"github.com/google/ax/proto"
)

func main() {
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8494"
	}
	token := os.Getenv("SLACK_USER_TOKEN")
	if token == "" {
		log.Fatal("SLACK_USER_TOKEN is required (xoxp-... user token with search:read.* scopes)")
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	g := grpc.NewServer()
	proto.RegisterAgentServiceServer(g, server.New(token))
	log.Printf("slack_search_agent listening on %s", addr)
	if err := g.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
