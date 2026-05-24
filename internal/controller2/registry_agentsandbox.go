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

package controller2

import (
	"context"
	"fmt"

	"github.com/google/ax/internal/agent"
	"github.com/google/ax/internal/config"
	expagent "github.com/google/ax/internal/experimental/agent"
	"github.com/google/ax/internal/experimental/k8s/agentsandbox"
)

// agentSandboxClientFactory builds an agentsandbox client given a
// (namespace, template) pair. Injectable via SetAgentSandboxClientFactory
// so tests can supply a fake without round-tripping through the kube
// in-cluster config loader.
type agentSandboxClientFactory func(namespace, template string) (interface {
	CreateSandbox(ctx context.Context, name string) (*agentsandbox.Sandbox, error)
	DeleteSandbox(ctx context.Context, name string) error
}, error)

var defaultAgentSandboxClientFactory agentSandboxClientFactory = func(namespace, template string) (interface {
	CreateSandbox(ctx context.Context, name string) (*agentsandbox.Sandbox, error)
	DeleteSandbox(ctx context.Context, name string) error
}, error) {
	return agentsandbox.NewClient(namespace, template)
}

// SetAgentSandboxClientFactory overrides the factory used by
// RegisterAgentSandbox to build sandbox clients. Test-only.
func (r *Registry) SetAgentSandboxClientFactory(f agentSandboxClientFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.agentSandboxFactory = f
}

func (r *Registry) effectiveAgentSandboxFactory() agentSandboxClientFactory {
	if r.agentSandboxFactory != nil {
		return r.agentSandboxFactory
	}
	return defaultAgentSandboxClientFactory
}

// RegisterAgentSandbox registers an agent backed by a Sandbox CR.
// Parallels RegisterATE; the only difference is the K8s API surface used
// to provision sandboxes (agents.x-k8s.io/Sandbox vs ate.dev/Actor).
func (r *Registry) RegisterAgentSandbox(_ context.Context, cfg config.AgentSandboxAgentConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := validateID(cfg.ID); err != nil {
		return err
	}
	if _, ok := r.agents[cfg.ID]; ok {
		return fmt.Errorf("agent %s already registered", cfg.ID)
	}
	if cfg.Namespace == "" {
		return fmt.Errorf("agent %s: namespace is required", cfg.ID)
	}
	if cfg.Template == "" {
		return fmt.Errorf("agent %s: template is required", cfg.ID)
	}

	client, err := r.effectiveAgentSandboxFactory()(cfg.Namespace, cfg.Template)
	if err != nil {
		return fmt.Errorf("agent %s: build sandbox client: %w", cfg.ID, err)
	}

	a, err := expagent.NewAgentSandboxAgent(client, expagent.AgentSandboxAgentConfig{
		ID:        cfg.ID,
		Namespace: cfg.Namespace,
		Template:  cfg.Template,
		Port:      cfg.Port,
		Protocol:  cfg.Protocol,
		Auth:      cfg.Auth,
		Headers:   cfg.Headers,
	})
	if err != nil {
		return fmt.Errorf("agent %s: build sandbox agent: %w", cfg.ID, err)
	}

	r.agents[cfg.ID] = a
	r.agentInfo[cfg.ID] = &agent.AgentInfo{
		ID:          cfg.ID,
		Name:        cfg.Name,
		Description: cfg.Description,
		Metadata:    cfg.Metadata,
	}
	return nil
}
