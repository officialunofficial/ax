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

package harness

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/google/ax/internal/experimental/k8s/agentsandbox"
	"github.com/google/ax/proto"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// sandboxClient is the minimal slice of *agentsandbox.Client the harness
// depends on, narrowed for testability.
type sandboxClient interface {
	CreateSandbox(ctx context.Context, name string) (*agentsandbox.Sandbox, error)
	DeleteSandbox(ctx context.Context, name string) error
}

// AgentSandboxHarness manages execution sessions in gVisor pods provisioned
// by kubernetes-sigs/agent-sandbox. Parallels SubstrateHarness; the only
// thing that differs is the pod-lifecycle layer (Sandbox CR vs ate.Actor).
// The HarnessService gRPC contract spoken by ax-harness inside the pod is
// the same.
type AgentSandboxHarness struct {
	client    sandboxClient
	port      int
	dialOpts  []grpc.DialOption
	// dialFn is overridable in tests to point at a bufconn or other in-memory
	// transport instead of a real TCP address.
	dialFn func(ctx context.Context, addr string, opts ...grpc.DialOption) (*grpc.ClientConn, error)
}

// AgentSandboxHarnessOption configures the harness at construction.
type AgentSandboxHarnessOption func(*AgentSandboxHarness)

// WithDialFn replaces the gRPC dial function (tests).
func WithDialFn(fn func(ctx context.Context, addr string, opts ...grpc.DialOption) (*grpc.ClientConn, error)) AgentSandboxHarnessOption {
	return func(h *AgentSandboxHarness) { h.dialFn = fn }
}

// NewAgentSandboxHarness builds an AgentSandboxHarness against the given
// agent-sandbox client. The default gRPC port matches what cmd/axharness
// listens on (50053) — same as Substrate, since the harness binary is
// identical.
func NewAgentSandboxHarness(client sandboxClient, port int, dialOpts []grpc.DialOption, opts ...AgentSandboxHarnessOption) (*AgentSandboxHarness, error) {
	if client == nil {
		return nil, errors.New("sandbox client is required")
	}
	if port == 0 {
		port = 50053
	}
	if len(dialOpts) == 0 {
		dialOpts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	h := &AgentSandboxHarness{
		client:   client,
		port:     port,
		dialOpts: dialOpts,
	}
	for _, opt := range opts {
		opt(h)
	}
	if h.dialFn == nil {
		h.dialFn = func(_ context.Context, addr string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
			return grpc.NewClient(addr, opts...)
		}
	}
	return h, nil
}

// Start implements Harness. Creates or adopts a Sandbox CR named after the
// conversationID, then dials the harness gRPC server inside the resulting
// pod.
func (h *AgentSandboxHarness) Start(ctx context.Context, conversationID string) (Execution, error) {
	if conversationID == "" {
		return nil, errors.New("AgentSandboxHarness needs a non-empty conversationID")
	}
	sb, err := h.client.CreateSandbox(ctx, conversationID)
	if err != nil {
		return nil, fmt.Errorf("create sandbox %q: %w", conversationID, err)
	}
	if sb.PodIP == "" {
		return nil, fmt.Errorf("sandbox %q has no pod IP", conversationID)
	}
	addr := fmt.Sprintf("%s:%d", sb.PodIP, h.port)
	conn, err := h.dialFn(ctx, addr, h.dialOpts...)
	if err != nil {
		// Best-effort cleanup so we don't leak the sandbox on dial errors.
		_ = h.client.DeleteSandbox(context.Background(), conversationID)
		return nil, fmt.Errorf("dial harness at %s: %w", addr, err)
	}
	return &agentSandboxExecution{
		harness:        h,
		conversationID: conversationID,
		execID:         uuid.NewString(),
		conn:           conn,
		client:         proto.NewHarnessServiceClient(conn),
	}, nil
}

// agentSandboxExecution mirrors substrateExecution. The bidirectional
// HarnessService.Connect protocol is identical; only the surrounding
// lifecycle (sandbox create/delete vs actor create/suspend) differs.
type agentSandboxExecution struct {
	harness        *AgentSandboxHarness
	conversationID string
	execID         string
	conn           *grpc.ClientConn
	client         proto.HarnessServiceClient

	mu      sync.Mutex
	pending []*proto.Message
}

func (e *agentSandboxExecution) ID() string { return e.execID }

func (e *agentSandboxExecution) Queue(_ context.Context, msg ...*proto.Message) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.pending = append(e.pending, msg...)
	return nil
}

func (e *agentSandboxExecution) Run(ctx context.Context, handler Handler) error {
	e.mu.Lock()
	inputs := e.pending
	e.pending = nil
	e.mu.Unlock()

	stream, err := e.client.Connect(ctx)
	if err != nil {
		return fmt.Errorf("open harness stream: %w", err)
	}
	if err := stream.Send(&proto.HarnessMessage{Messages: inputs}); err != nil {
		return fmt.Errorf("send inputs: %w", err)
	}
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("close send direction: %w", err)
	}
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("recv from harness stream: %w", err)
		}
		for _, m := range resp.Messages {
			if err := handler.OnMessage(ctx, e.execID, m); err != nil {
				return err
			}
		}
	}
	return handler.OnComplete(ctx, e.execID)
}

func (e *agentSandboxExecution) Close(ctx context.Context) error {
	if e.conn != nil {
		_ = e.conn.Close()
	}
	// Best-effort sandbox cleanup. Errors here are logged-but-not-returned
	// because the conversation is already over from AX's perspective.
	return e.harness.client.DeleteSandbox(ctx, e.conversationID)
}
