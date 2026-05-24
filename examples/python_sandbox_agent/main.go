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

// Command python_sandbox_agent serves AX's AgentService gRPC on :8494 inside
// a kubernetes-sigs/agent-sandbox pod. It executes the Python source in
// the latest user message and streams stdout/stderr back as the response.
//
// This is the user-code half of the agent-sandbox integration. AX's
// controller calls into this server via RemoteAgent (constructed by
// AgentSandboxAgent.Connect). The gVisor isolation comes from the
// surrounding Sandbox CR; this process just needs to run Python safely
// in the local container.
//
// Image: <registry>/<project>/python-sandbox-agent
// Built by examples/python_sandbox_agent/Dockerfile.
package main

import (
	"log"
	"net"
	"os"

	"google.golang.org/grpc"

	"github.com/google/ax/examples/python_sandbox_agent/internal/server"
	"github.com/google/ax/proto"
)

func main() {
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8494"
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	g := grpc.NewServer()
	proto.RegisterAgentServiceServer(g, server.New())
	log.Printf("python_sandbox_agent listening on %s", addr)
	if err := g.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
