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
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/ax/internal/experimental/k8s/agentsandbox"
	"github.com/google/ax/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// fakeSandboxClient implements sandboxClient for unit tests.
type fakeSandboxClient struct {
	mu         sync.Mutex
	created    []string
	deleted    []string
	createResp *agentsandbox.Sandbox
	createErr  error
	deleteErr  error
}

func (f *fakeSandboxClient) CreateSandbox(_ context.Context, name string) (*agentsandbox.Sandbox, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created = append(f.created, name)
	if f.createErr != nil {
		return nil, f.createErr
	}
	if f.createResp != nil {
		// Return a copy so the test can inspect the response w/o data race.
		cp := *f.createResp
		return &cp, nil
	}
	return &agentsandbox.Sandbox{Name: name, Namespace: "agent-platform", PodIP: "10.0.0.1"}, nil
}

func (f *fakeSandboxClient) DeleteSandbox(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, name)
	return f.deleteErr
}

// fakeHarnessServer implements proto.HarnessServiceServer for the bufconn
// gRPC fixture below. It echoes whatever messages the client sends back via
// Recv -> Send so tests can assert handler callbacks happen.
type fakeHarnessServer struct {
	proto.UnimplementedHarnessServiceServer
	echoMessages []*proto.Message
}

func (s *fakeHarnessServer) Connect(stream proto.HarnessService_ConnectServer) error {
	// Drain inputs so the client can CloseSend, then emit the canned echo.
	for {
		_, err := stream.Recv()
		if err != nil {
			break
		}
	}
	return stream.Send(&proto.HarnessMessage{Messages: s.echoMessages})
}

// newBufconnHarness spins up a gRPC server on a bufconn listener and
// returns an AgentSandboxHarness wired to dial through it. The returned
// fakeHarnessServer is the in-memory backend; tests use it to drive the
// canonical Connect stream payload.
func newBufconnHarness(t *testing.T, client sandboxClient, srv *fakeHarnessServer) *AgentSandboxHarness {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	g := grpc.NewServer()
	proto.RegisterHarnessServiceServer(g, srv)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(func() { g.Stop() })

	dialFn := func(_ context.Context, _ string, _ ...grpc.DialOption) (*grpc.ClientConn, error) {
		// grpc.NewClient with passthrough+bufconn contact dialer is the
		// modern non-deprecated way to plug a bufconn into a client.
		return grpc.NewClient("passthrough:///bufnet",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
	}

	h, err := NewAgentSandboxHarness(client, 50053, nil, WithDialFn(dialFn))
	if err != nil {
		t.Fatalf("NewAgentSandboxHarness: %v", err)
	}
	return h
}

func TestNewAgentSandboxHarness_RequiresClient(t *testing.T) {
	if _, err := NewAgentSandboxHarness(nil, 0, nil); err == nil {
		t.Fatal("expected error when client is nil")
	}
}

func TestStart_RequiresConversationID(t *testing.T) {
	h, _ := NewAgentSandboxHarness(&fakeSandboxClient{}, 0, nil)
	if _, err := h.Start(context.Background(), ""); err == nil {
		t.Fatal("expected error for empty conversationID")
	}
}

func TestStart_PropagatesCreateError(t *testing.T) {
	fakeClient := &fakeSandboxClient{createErr: errors.New("boom")}
	h := newBufconnHarness(t, fakeClient, &fakeHarnessServer{})
	if _, err := h.Start(context.Background(), "conv-1"); err == nil {
		t.Fatal("expected Start to fail when CreateSandbox errors")
	}
	// We do NOT call DeleteSandbox on create-error; the sandbox never
	// existed in the first place.
	if len(fakeClient.deleted) != 0 {
		t.Errorf("unexpected DeleteSandbox after failed Create: %v", fakeClient.deleted)
	}
}

func TestStart_RejectsSandboxWithoutPodIP(t *testing.T) {
	fakeClient := &fakeSandboxClient{
		createResp: &agentsandbox.Sandbox{Name: "conv-1", Namespace: "agent-platform", PodIP: ""},
	}
	h := newBufconnHarness(t, fakeClient, &fakeHarnessServer{})
	_, err := h.Start(context.Background(), "conv-1")
	if err == nil {
		t.Fatal("expected error when sandbox has no PodIP")
	}
}

func TestStart_ReturnsExecutionWithUniqueID(t *testing.T) {
	h := newBufconnHarness(t, &fakeSandboxClient{}, &fakeHarnessServer{})
	exec1, err := h.Start(context.Background(), "conv-1")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	exec2, err := h.Start(context.Background(), "conv-2")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if exec1.ID() == exec2.ID() {
		t.Errorf("execution IDs collided: %q", exec1.ID())
	}
}

// captureHandler records all OnMessage / OnComplete callbacks for assertions.
type captureHandler struct {
	mu       sync.Mutex
	messages []*proto.Message
	complete bool
}

func (c *captureHandler) OnMessage(_ context.Context, _ string, m *proto.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messages = append(c.messages, m)
	return nil
}

func (c *captureHandler) OnComplete(_ context.Context, _ string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.complete = true
	return nil
}

func TestRun_StreamsServerMessagesIntoHandler(t *testing.T) {
	srv := &fakeHarnessServer{
		echoMessages: []*proto.Message{
			{Role: "assistant"},
			{Role: "assistant"},
		},
	}
	h := newBufconnHarness(t, &fakeSandboxClient{}, srv)
	exec, err := h.Start(context.Background(), "conv-stream")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(context.Background()) })

	hdl := &captureHandler{}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := exec.Run(ctx, hdl); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(hdl.messages) != 2 {
		t.Fatalf("want 2 handler messages, got %d", len(hdl.messages))
	}
	if !hdl.complete {
		t.Error("OnComplete was never called")
	}
}

func TestQueue_BuffersUntilRun(t *testing.T) {
	h := newBufconnHarness(t, &fakeSandboxClient{}, &fakeHarnessServer{})
	exec, err := h.Start(context.Background(), "conv-q")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = exec.Close(context.Background()) })

	if err := exec.Queue(context.Background(), &proto.Message{Role: "user"}, &proto.Message{Role: "user"}); err != nil {
		t.Fatalf("Queue: %v", err)
	}
	// Inspect the internal buffer via type assertion so we don't expose a
	// test-only method on Execution.
	ase := exec.(*agentSandboxExecution)
	ase.mu.Lock()
	got := len(ase.pending)
	ase.mu.Unlock()
	if got != 2 {
		t.Errorf("pending buffer = %d, want 2", got)
	}
}

func TestClose_DeletesSandbox(t *testing.T) {
	fakeClient := &fakeSandboxClient{}
	h := newBufconnHarness(t, fakeClient, &fakeHarnessServer{})
	exec, err := h.Start(context.Background(), "conv-close")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := exec.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(fakeClient.deleted) != 1 || fakeClient.deleted[0] != "conv-close" {
		t.Errorf("expected DeleteSandbox called for conv-close, got %v", fakeClient.deleted)
	}
}
