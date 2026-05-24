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

package agent

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/google/ax/internal/agent"
	"github.com/google/ax/internal/auth"
	"github.com/google/ax/internal/experimental/k8s/agentsandbox"
	"github.com/google/ax/proto"
)

// agentSandboxClient is the slice of *agentsandbox.Client this agent needs.
// Tests inject a fake; production passes the real client.
type agentSandboxClient interface {
	CreateSandbox(ctx context.Context, name string) (*agentsandbox.Sandbox, error)
	DeleteSandbox(ctx context.Context, name string) error
}

// AgentSandboxAgent runs an Agent inside a kubernetes-sigs/agent-sandbox
// Sandbox CR. Parallels SubstrateAgent (which uses Substrate's ate.Actor
// resources) but targets the GA-stable agent-sandbox CRDs instead.
//
// The user-code container inside the sandbox is expected to serve AX's
// AgentService gRPC on the configured port — same contract a RemoteAgent
// dials directly. agent-sandbox is responsible only for the pod
// lifecycle.
type AgentSandboxAgent struct {
	client agentSandboxClient
	config AgentSandboxAgentConfig
}

// AgentSandboxAgentConfig configures the runtime agent. The yaml-tagged
// variant lives in internal/config; this is the post-parse internal one.
type AgentSandboxAgentConfig struct {
	ID        string
	Namespace string
	Template  string
	Port      int // Port the AgentService is bound to inside the sandbox pod
	Protocol  string
	Auth      auth.Auth
	Headers   auth.Headers
}

// NewAgentSandboxAgent builds an Agent that creates Sandbox CRs through
// the supplied client and dials whatever AgentService is reachable at the
// sandbox pod's IP.
func NewAgentSandboxAgent(client agentSandboxClient, config AgentSandboxAgentConfig) (*AgentSandboxAgent, error) {
	if client == nil {
		return nil, fmt.Errorf("sandbox client is required")
	}
	if config.Port == 0 {
		config.Port = 8494 // Default AX AgentService port, same as SubstrateAgent.
	}
	return &AgentSandboxAgent{client: client, config: config}, nil
}

// Connect implements agent.Agent. Mirrors SubstrateAgent.Connect step-for-step
// except the sandbox lifecycle uses agent-sandbox CRDs instead of ate.Actor.
func (a *AgentSandboxAgent) Connect(
	ctx context.Context,
	conversationID, execID string,
	start *proto.AgentStart,
	e agent.Executor,
	o agent.OutputHandler,
) error {
	// 1. Provision (or adopt) the Sandbox keyed by execID.
	sb, err := a.client.CreateSandbox(ctx, execID)
	if err != nil {
		return fmt.Errorf("create sandbox %q: %w", execID, err)
	}
	if sb.PodIP == "" {
		// Best-effort cleanup so we don't leak a CR after a soft failure.
		_ = a.client.DeleteSandbox(context.Background(), execID)
		return fmt.Errorf("sandbox %q has no pod IP", execID)
	}

	workerAddr := fmt.Sprintf("%s:%d", sb.PodIP, a.config.Port)

	// 2. Bridge to the user-code AgentService at the pod IP.
	var activeAgent agent.Agent
	switch strings.ToLower(a.config.Protocol) {
	case "", "axp":
		activeAgent, err = agent.NewRemoteAgent(agent.RemoteAgentConfig{
			Address:    workerAddr,
			Reconnect:  true,
			MaxRetries: 3,
		})
	case "a2a":
		activeAgent, err = NewA2AAgent(ctx, A2AAgentConfig{
			ID:                a.config.ID,
			Address:           workerAddr,
			Auth:              a.config.Auth,
			Headers:           a.config.Headers,
			Stateless:         true,
			OverrideCardHosts: true,
		})
	default:
		_ = a.client.DeleteSandbox(context.Background(), execID)
		return fmt.Errorf("agentsandbox agent %s: invalid protocol %q", a.config.ID, a.config.Protocol)
	}
	if err != nil {
		_ = a.client.DeleteSandbox(context.Background(), execID)
		return fmt.Errorf("connect to sandbox-hosted agent: %w", err)
	}
	defer activeAgent.Close()

	// 3. Tear down the sandbox when the conversation turn ends. Unlike
	// Substrate's suspend-back-into-warm-pool model, agent-sandbox sandboxes
	// are 1:1 with executions — Delete is the right verb. The
	// SandboxWarmPool refills in the background so the next execID claims
	// an already-warm pod.
	defer func() {
		log.Printf("Deleting agent-sandbox Sandbox %s", execID)
		// Use a fresh background context so cleanup runs even if `ctx` was
		// cancelled by the caller (matches SubstrateAgent's pattern).
		bg, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := a.client.DeleteSandbox(bg, execID); err != nil {
			log.Printf("Failed to delete Sandbox %s: %v", execID, err)
		}
	}()

	return activeAgent.Connect(ctx, conversationID, execID, start, e, o)
}

// Close releases any persistent resources. The agent-sandbox client holds
// a controller-runtime client whose lifecycle is owned by the registry;
// nothing to do here.
func (a *AgentSandboxAgent) Close() error { return nil }
