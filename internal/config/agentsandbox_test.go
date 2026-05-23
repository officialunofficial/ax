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

package config

import (
	"testing"

	"gopkg.in/yaml.v3"
)

// TestRegistryConfig_ParsesAgentSandboxAgents covers the round-trip
// from yaml -> RegistryConfig.AgentSandboxAgents. cliutil iterates this
// slice and calls registry.RegisterAgentSandbox for each entry, so the
// field has to surface from the yaml correctly.
func TestRegistryConfig_ParsesAgentSandboxAgents(t *testing.T) {
	const src = `
registry:
  agent_sandbox_agents:
    - id: py
      name: Python Sandbox Agent
      description: Executes Python in a gVisor sandbox
      namespace: agent-platform
      template: python-sandbox-template
      port: 8494
      protocol: axp
      metadata:
        team: uno
`
	type wrapper struct {
		Registry RegistryConfig `yaml:"registry"`
	}
	var w wrapper
	if err := yaml.Unmarshal([]byte(src), &w); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if len(w.Registry.AgentSandboxAgents) != 1 {
		t.Fatalf("want 1 AgentSandboxAgent, got %d", len(w.Registry.AgentSandboxAgents))
	}
	got := w.Registry.AgentSandboxAgents[0]
	want := AgentSandboxAgentConfig{
		ID:          "py",
		Name:        "Python Sandbox Agent",
		Description: "Executes Python in a gVisor sandbox",
		Namespace:   "agent-platform",
		Template:    "python-sandbox-template",
		Port:        8494,
		Protocol:    "axp",
		Metadata:    map[string]string{"team": "uno"},
	}
	if got.ID != want.ID || got.Name != want.Name || got.Description != want.Description ||
		got.Namespace != want.Namespace || got.Template != want.Template ||
		got.Port != want.Port || got.Protocol != want.Protocol ||
		got.Metadata["team"] != want.Metadata["team"] {
		t.Errorf("parsed AgentSandboxAgentConfig mismatch:\n got=%+v\nwant=%+v", got, want)
	}
}

// TestRegistryConfig_AgentSandboxAgentsOptional confirms the field is
// optional — a config that omits the section must still parse cleanly.
func TestRegistryConfig_AgentSandboxAgentsOptional(t *testing.T) {
	const src = `
registry:
  remote_agents:
    - id: r1
      address: localhost:50051
`
	type wrapper struct {
		Registry RegistryConfig `yaml:"registry"`
	}
	var w wrapper
	if err := yaml.Unmarshal([]byte(src), &w); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	if w.Registry.AgentSandboxAgents != nil {
		t.Errorf("AgentSandboxAgents should be nil/empty when omitted, got %v", w.Registry.AgentSandboxAgents)
	}
}
