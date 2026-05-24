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

package server

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/google/ax/proto"
)

// fakePythonExecutor records what was passed to Run and returns canned
// output without touching python3. Lets us assert the server's plumbing
// independent of the host's Python.
type fakePythonExecutor struct {
	called   []string
	stdout   string
	stderr   string
	exitCode int
	runErr   error
}

func (f *fakePythonExecutor) Run(_ context.Context, source string) (string, string, int, error) {
	f.called = append(f.called, source)
	return f.stdout, f.stderr, f.exitCode, f.runErr
}

// newTestClient spins up a Server on bufconn and returns an AgentService
// client wired to it. Tests can call Connect normally.
func newTestClient(t *testing.T, srv *Server) proto.AgentServiceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	g := grpc.NewServer()
	proto.RegisterAgentServiceServer(g, srv)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return proto.NewAgentServiceClient(conn)
}

// userMessage builds a Message with text content under the "user" role.
func userMessage(text string) *proto.Message {
	return &proto.Message{
		Role: "user",
		Content: &proto.Content{
			Type: &proto.Content_Text{Text: &proto.TextContent{Text: text}},
		},
	}
}

// TestConnect_PrefersStructuredSubagentPrompt locks in the planner's
// modern dispatch contract: when AgentStart.subagent_prompt is set,
// it's used as the Python source verbatim, regardless of what (if
// anything) Messages contains.
func TestConnect_PrefersStructuredSubagentPrompt(t *testing.T) {
	exec := &fakePythonExecutor{stdout: "42\n", exitCode: 0}
	srv := New(WithExecutor(exec))
	client := newTestClient(t, srv)

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		ConversationId: "conv-structured",
		ExecId:         "exec-structured",
		Start: &proto.AgentStart{
			SubagentPrompt: "print(7*6)",
			// Messages intentionally empty — planner-driven dispatch
			// (see proto/ax.proto AgentStart.subagent_prompt).
		},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if got := exec.called; len(got) != 1 || got[0] != "print(7*6)" {
		t.Errorf("executor.Run got %v, want one call with SubagentPrompt body", got)
	}
}

func TestConnect_ExecutesUserPython(t *testing.T) {
	exec := &fakePythonExecutor{stdout: "42\n", exitCode: 0}
	srv := New(WithExecutor(exec))
	client := newTestClient(t, srv)

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		ConversationId: "conv-1",
		ExecId:         "exec-1",
		Start: &proto.AgentStart{
			Messages: []*proto.Message{userMessage("print(7*6)")},
		},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if got := exec.called; len(got) != 1 || got[0] != "print(7*6)" {
		t.Errorf("executor.Run got %v, want one call with the user source", got)
	}
	text := resp.GetOutputs().GetMessages()[0].GetContent().GetText().Text
	if !strings.Contains(text, "42") {
		t.Errorf("response text missing stdout, got %q", text)
	}
	if !strings.Contains(text, "exit_code=0") {
		t.Errorf("response missing exit_code=0, got %q", text)
	}
}

func TestConnect_PicksLatestUserMessage(t *testing.T) {
	exec := &fakePythonExecutor{stdout: "ok\n"}
	srv := New(WithExecutor(exec))
	client := newTestClient(t, srv)

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{
			Messages: []*proto.Message{
				userMessage("first"),
				userMessage("second"),
				userMessage("print('final')"),
			},
		},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if got := exec.called[0]; got != "print('final')" {
		t.Errorf("executor got %q, want last message %q", got, "print('final')")
	}
}

func TestConnect_RejectsMissingStart(t *testing.T) {
	srv := New(WithExecutor(&fakePythonExecutor{}))
	client := newTestClient(t, srv)

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_, err = stream.Recv()
	if err == nil {
		t.Fatal("expected error for missing Start")
	}
}

func TestConnect_RejectsNoUserText(t *testing.T) {
	srv := New(WithExecutor(&fakePythonExecutor{}))
	client := newTestClient(t, srv)

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{Messages: []*proto.Message{{Role: "user"}}}, // no content
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_, err = stream.Recv()
	if err == nil {
		t.Fatal("expected error for no text content")
	}
}

func TestConnect_SurfacesNonZeroExitCode(t *testing.T) {
	exec := &fakePythonExecutor{stderr: "Traceback...\n", exitCode: 1}
	srv := New(WithExecutor(exec))
	client := newTestClient(t, srv)

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{Messages: []*proto.Message{userMessage("raise SystemExit(1)")}},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	text := resp.GetOutputs().GetMessages()[0].GetContent().GetText().Text
	if !strings.Contains(text, "exit_code=1") {
		t.Errorf("response missing exit_code=1, got %q", text)
	}
	if !strings.Contains(text, "Traceback") {
		t.Errorf("response missing stderr, got %q", text)
	}
}

func TestConnect_SurfacesExecutorError(t *testing.T) {
	exec := &fakePythonExecutor{runErr: errors.New("python3 not found")}
	srv := New(WithExecutor(exec))
	client := newTestClient(t, srv)

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{Messages: []*proto.Message{userMessage("anything")}},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	text := resp.GetOutputs().GetMessages()[0].GetContent().GetText().Text
	if !strings.Contains(text, "python execution failed") {
		t.Errorf("response missing executor-error message, got %q", text)
	}
}

func TestConnect_HonorsExecTimeout(t *testing.T) {
	// Timeout enforcement is exercised via ctx propagation; use a fake
	// executor that records whether the ctx had a deadline.
	hadDeadline := false
	exec := pythonExecutorFunc(func(ctx context.Context, _ string) (string, string, int, error) {
		_, ok := ctx.Deadline()
		hadDeadline = ok
		return "", "", 0, nil
	})
	srv := New(WithExecutor(exec), WithExecTimeout(50*time.Millisecond))
	client := newTestClient(t, srv)

	stream, err := client.Connect(context.Background(), &proto.AgentRequest{
		Start: &proto.AgentStart{Messages: []*proto.Message{userMessage("noop")}},
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if !hadDeadline {
		t.Error("expected executor ctx to carry a deadline")
	}
}

type pythonExecutorFunc func(context.Context, string) (string, string, int, error)

func (f pythonExecutorFunc) Run(ctx context.Context, source string) (string, string, int, error) {
	return f(ctx, source)
}
