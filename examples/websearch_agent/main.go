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

// Command websearch_agent serves AX's AgentService gRPC on :8494. On
// every Connect call it takes the search query (from the structured
// AgentStart.subagent_prompt or the legacy messages envelope), makes a
// single Gemini call with Tools=[GoogleSearch{}] only, and streams a
// single AgentResponse containing the grounded text plus citations.
//
// This is the agent-as-tool workaround for Vertex Enterprise rejecting
// `Tools: [FunctionDeclarations + GoogleSearch]` in the same call —
// the AX planner can't combine the two, so the websearch_agent makes a
// SECOND Gemini call internally with just GoogleSearch.
//
// Image: us-east4-docker.pkg.dev/official-unofficial/docker/websearch-agent
// Built by examples/websearch_agent/Dockerfile via Cloud Build.
package main

import (
	"context"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"

	"github.com/google/ax/examples/websearch_agent/internal/server"
	"github.com/google/ax/proto"
)

func main() {
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8494"
	}

	project := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if project == "" {
		log.Fatal("GOOGLE_CLOUD_PROJECT is required (e.g. official-unofficial)")
	}
	location := os.Getenv("GOOGLE_CLOUD_LOCATION")
	if location == "" {
		location = "global"
	}
	model := os.Getenv("GEMINI_MODEL")
	if model == "" {
		model = "gemini-3-flash-preview"
	}

	// genai.NewClient (with BackendVertexAI) requires either
	// GOOGLE_GENAI_USE_VERTEXAI=true OR explicit Backend in ClientConfig.
	// We set Backend in code so the env var isn't strictly required, but
	// log a warning if it's missing so operators notice misconfigured
	// Deployments.
	if os.Getenv("GOOGLE_GENAI_USE_VERTEXAI") == "" {
		log.Printf("warning: GOOGLE_GENAI_USE_VERTEXAI is not set; relying on explicit Backend=BackendVertexAI in ClientConfig")
	}

	ctx := context.Background()
	srv, err := server.New(ctx, project, location, model)
	if err != nil {
		log.Fatalf("server.New: %v", err)
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	g := grpc.NewServer()
	proto.RegisterAgentServiceServer(g, srv)
	log.Printf("websearch_agent listening on %s (project=%s location=%s model=%s)", addr, project, location, model)
	if err := g.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
