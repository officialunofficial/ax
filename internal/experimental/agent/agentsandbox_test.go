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
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/google/ax/internal/agent"
	"github.com/google/ax/internal/experimental/k8s/agentsandbox"
	"github.com/google/ax/proto"
)

type fakeSandboxClient struct {
	mu          sync.Mutex
	createCalls []string
	deleteCalls []string
	createResp  *agentsandbox.Sandbox
	createErr   error
}

func (f *fakeSandboxClient) CreateSandbox(_ context.Context, name string) (*agentsandbox.Sandbox, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls = append(f.createCalls, name)
	if f.createErr != nil {
		return nil, f.createErr
	}
	if f.createResp != nil {
		cp := *f.createResp
		return &cp, nil
	}
	return &agentsandbox.Sandbox{Name: name, PodIP: "10.0.0.5"}, nil
}

func (f *fakeSandboxClient) DeleteSandbox(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls = append(f.deleteCalls, name)
	return nil
}

func TestNewAgentSandboxAgent_RequiresClient(t *testing.T) {
	if _, err := NewAgentSandboxAgent(nil, AgentSandboxAgentConfig{}); err == nil {
		t.Fatal("expected error when client is nil")
	}
}

func TestNewAgentSandboxAgent_DefaultsPortTo8494(t *testing.T) {
	a, err := NewAgentSandboxAgent(&fakeSandboxClient{}, AgentSandboxAgentConfig{ID: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if a.config.Port != 8494 {
		t.Errorf("default port = %d, want 8494", a.config.Port)
	}
}

func TestConnect_FailsWhenCreateSandboxErrors(t *testing.T) {
	fc := &fakeSandboxClient{createErr: errors.New("boom")}
	a, _ := NewAgentSandboxAgent(fc, AgentSandboxAgentConfig{ID: "x"})
	err := a.Connect(context.Background(), "conv-1", "exec-1", nil, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "create sandbox") {
		t.Fatalf("expected create-sandbox error, got %v", err)
	}
	// No DeleteSandbox call when the create itself failed.
	if len(fc.deleteCalls) != 0 {
		t.Errorf("unexpected DeleteSandbox after Create failure: %v", fc.deleteCalls)
	}
}

func TestConnect_DeletesSandboxWhenPodIPMissing(t *testing.T) {
	fc := &fakeSandboxClient{
		createResp: &agentsandbox.Sandbox{Name: "exec-1", PodIP: ""},
	}
	a, _ := NewAgentSandboxAgent(fc, AgentSandboxAgentConfig{ID: "x"})
	if err := a.Connect(context.Background(), "conv-1", "exec-1", nil, nil, nil); err == nil {
		t.Fatal("expected error when PodIP missing")
	}
	if len(fc.deleteCalls) != 1 || fc.deleteCalls[0] != "exec-1" {
		t.Errorf("expected DeleteSandbox cleanup, got %v", fc.deleteCalls)
	}
}

func TestConnect_RejectsUnknownProtocol(t *testing.T) {
	fc := &fakeSandboxClient{}
	a, _ := NewAgentSandboxAgent(fc, AgentSandboxAgentConfig{ID: "x", Protocol: "bananas"})
	err := a.Connect(context.Background(), "conv-1", "exec-1", nil, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "invalid protocol") {
		t.Fatalf("expected invalid-protocol error, got %v", err)
	}
	if len(fc.deleteCalls) != 1 {
		t.Errorf("expected cleanup after protocol rejection, got %v", fc.deleteCalls)
	}
}

func TestConnect_DeletesSandboxOnInnerAgentError(t *testing.T) {
	// We can't easily inject a fake activeAgent because Connect builds its
	// own RemoteAgent. Instead drive it with an unreachable port (0)
	// so RemoteAgent connect fails — Connect should still call
	// DeleteSandbox via the defer.
	fc := &fakeSandboxClient{
		createResp: &agentsandbox.Sandbox{Name: "exec-1", PodIP: "127.0.0.1"},
	}
	// Empty Address path is hard; rely on the dialer rejecting :0.
	a, _ := NewAgentSandboxAgent(fc, AgentSandboxAgentConfig{ID: "x", Port: 1})
	// RemoteAgent dials grpc — that might succeed (lazy dial) so we use a
	// proper executor/handler shape and let it fail at Connect.
	err := a.Connect(context.Background(), "conv-1", "exec-1", &proto.AgentStart{}, nil, noopOutput)
	// We don't care about the specific error; we DO care that we cleaned up.
	_ = err
	// We must have called DeleteSandbox at least once.
	if len(fc.deleteCalls) == 0 || fc.deleteCalls[0] != "exec-1" {
		t.Errorf("expected DeleteSandbox after inner error, got %v", fc.deleteCalls)
	}
}

func noopOutput(_ *proto.AgentOutputs) error { return nil }

func TestClose_ReturnsNil(t *testing.T) {
	a, _ := NewAgentSandboxAgent(&fakeSandboxClient{}, AgentSandboxAgentConfig{ID: "x"})
	if err := a.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// Sanity check: AgentSandboxAgent satisfies agent.Agent at compile time.
var _ agent.Agent = (*AgentSandboxAgent)(nil)
