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
	"strings"
	"sync"
	"testing"

	"github.com/google/ax/internal/config"
	"github.com/google/ax/internal/experimental/k8s/agentsandbox"
)

// fakeAgentSandboxClient satisfies the interface RegisterAgentSandbox expects.
type fakeAgentSandboxClient struct{}

func (fakeAgentSandboxClient) CreateSandbox(_ context.Context, name string) (*agentsandbox.Sandbox, error) {
	return &agentsandbox.Sandbox{Name: name, PodIP: "10.0.0.1"}, nil
}
func (fakeAgentSandboxClient) DeleteSandbox(_ context.Context, _ string) error { return nil }

// newRegistryWithFakeFactory builds a Registry with a sandbox factory that
// returns the fake client every time, recording the namespace/template
// pairs it was invoked with.
func newRegistryWithFakeFactory(t *testing.T) (*Registry, *factoryRecorder) {
	t.Helper()
	r := NewRegistry()
	rec := &factoryRecorder{}
	r.SetAgentSandboxClientFactory(func(namespace, template string) (interface {
		CreateSandbox(ctx context.Context, name string) (*agentsandbox.Sandbox, error)
		DeleteSandbox(ctx context.Context, name string) error
	}, error) {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		rec.calls = append(rec.calls, [2]string{namespace, template})
		return fakeAgentSandboxClient{}, nil
	})
	return r, rec
}

type factoryRecorder struct {
	mu    sync.Mutex
	calls [][2]string
}

func (r *factoryRecorder) Calls() [][2]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([][2]string, len(r.calls))
	copy(out, r.calls)
	return out
}

func TestRegisterAgentSandbox_HappyPath(t *testing.T) {
	r, rec := newRegistryWithFakeFactory(t)
	err := r.RegisterAgentSandbox(context.Background(), config.AgentSandboxAgentConfig{
		ID:        "py",
		Namespace: "agent-platform",
		Template:  "python-sandbox-template",
	})
	if err != nil {
		t.Fatalf("RegisterAgentSandbox: %v", err)
	}
	got, err := r.Get("py")
	if err != nil {
		t.Fatalf("registry should contain py: %v", err)
	}
	if got == nil {
		t.Fatal("got nil agent")
	}
	if calls := rec.Calls(); len(calls) != 1 || calls[0] != [2]string{"agent-platform", "python-sandbox-template"} {
		t.Errorf("factory invocations = %v, want one call with (agent-platform, python-sandbox-template)", calls)
	}
}

func TestRegisterAgentSandbox_RejectsEmptyID(t *testing.T) {
	r, _ := newRegistryWithFakeFactory(t)
	err := r.RegisterAgentSandbox(context.Background(), config.AgentSandboxAgentConfig{
		Namespace: "ns",
		Template:  "tmpl",
	})
	if err == nil {
		t.Fatal("expected ID validation error")
	}
}

func TestRegisterAgentSandbox_RejectsMissingNamespace(t *testing.T) {
	r, _ := newRegistryWithFakeFactory(t)
	err := r.RegisterAgentSandbox(context.Background(), config.AgentSandboxAgentConfig{
		ID:       "py",
		Template: "tmpl",
	})
	if err == nil || !strings.Contains(err.Error(), "namespace") {
		t.Fatalf("expected namespace-required error, got %v", err)
	}
}

func TestRegisterAgentSandbox_RejectsMissingTemplate(t *testing.T) {
	r, _ := newRegistryWithFakeFactory(t)
	err := r.RegisterAgentSandbox(context.Background(), config.AgentSandboxAgentConfig{
		ID:        "py",
		Namespace: "ns",
	})
	if err == nil || !strings.Contains(err.Error(), "template") {
		t.Fatalf("expected template-required error, got %v", err)
	}
}

func TestRegisterAgentSandbox_RejectsDuplicate(t *testing.T) {
	r, _ := newRegistryWithFakeFactory(t)
	cfg := config.AgentSandboxAgentConfig{ID: "py", Namespace: "ns", Template: "tmpl"}
	if err := r.RegisterAgentSandbox(context.Background(), cfg); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	err := r.RegisterAgentSandbox(context.Background(), cfg)
	if err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("expected duplicate-registration error, got %v", err)
	}
}

func TestRegisterAgentSandbox_PopulatesAgentInfo(t *testing.T) {
	r, _ := newRegistryWithFakeFactory(t)
	err := r.RegisterAgentSandbox(context.Background(), config.AgentSandboxAgentConfig{
		ID:          "py",
		Name:        "Python Sandbox Agent",
		Description: "Executes Python in a gVisor sandbox",
		Namespace:   "agent-platform",
		Template:    "python-sandbox-template",
		Metadata:    map[string]string{"team": "example"},
	})
	if err != nil {
		t.Fatalf("RegisterAgentSandbox: %v", err)
	}
	info, err := r.GetInfo("py")
	if err != nil {
		t.Fatalf("GetInfo: %v", err)
	}
	if info.Name != "Python Sandbox Agent" {
		t.Errorf("info.Name = %q", info.Name)
	}
	if info.Description != "Executes Python in a gVisor sandbox" {
		t.Errorf("info.Description = %q", info.Description)
	}
	if info.Metadata["team"] != "example" {
		t.Errorf("info.Metadata missing team label: %v", info.Metadata)
	}
}
