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

// Package server implements the proto.AgentService for the
// python_sandbox_agent. Split from package main so it's importable in tests.
package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"time"

	"google.golang.org/grpc"

	"github.com/google/ax/proto"
)

// Server is the AgentService implementation. Each Connect RPC pulls the
// latest user-message text, runs it as Python, and streams stdout + a
// terminal AgentEnd back to the AX controller.
type Server struct {
	proto.UnimplementedAgentServiceServer

	// executor lets tests inject a fake without spawning a real subprocess.
	executor pythonExecutor

	// execTimeout caps how long a single tool call can run.
	execTimeout time.Duration
}

// pythonExecutor runs the given source as Python 3 and returns stdout +
// stderr + exit code. Implementations should respect ctx for cancellation.
type pythonExecutor interface {
	Run(ctx context.Context, source string) (stdout, stderr string, exitCode int, err error)
}

// New builds a Server with sane defaults. The python executor uses the
// `python3` binary that must be present in the container PATH.
func New(opts ...Option) *Server {
	s := &Server{
		executor:    &subprocessPythonExecutor{},
		execTimeout: 60 * time.Second,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Option configures a Server at construction.
type Option func(*Server)

// WithExecutor injects a custom pythonExecutor (tests).
func WithExecutor(e pythonExecutor) Option {
	return func(s *Server) { s.executor = e }
}

// WithExecTimeout overrides the per-call execution timeout.
func WithExecTimeout(d time.Duration) Option {
	return func(s *Server) { s.execTimeout = d }
}

// Connect implements proto.AgentService. Single-turn: read the last user
// message's text, execute it as Python, stream one AgentResponse, return.
func (s *Server) Connect(req *proto.AgentRequest, stream grpc.ServerStreamingServer[proto.AgentResponse]) error {
	start := req.GetStart()
	if start == nil {
		return errors.New("AgentRequest.Start is required")
	}
	source, ok := lastUserText(start.Messages)
	if !ok {
		return errors.New("no user message with text content found")
	}

	ctx, cancel := context.WithTimeout(stream.Context(), s.execTimeout)
	defer cancel()

	stdout, stderr, exitCode, runErr := s.executor.Run(ctx, source)

	body := fmt.Sprintf("exit_code=%d\nstdout:\n%s\nstderr:\n%s", exitCode, stdout, stderr)
	if runErr != nil {
		body = fmt.Sprintf("python execution failed: %v\n\n%s", runErr, body)
	}

	resp := &proto.AgentResponse{
		ConversationId: req.ConversationId,
		ExecId:         req.ExecId,
		Type: &proto.AgentResponse_Outputs{
			Outputs: &proto.AgentOutputs{
				Messages: []*proto.Message{{
					Role: "assistant",
					Content: &proto.Content{
						Type: &proto.Content_Text{
							Text: &proto.TextContent{Text: body},
						},
					},
				}},
			},
		},
	}
	if err := stream.Send(resp); err != nil {
		return fmt.Errorf("send response: %w", err)
	}
	return nil
}

// lastUserText returns the text content of the most recent user-role
// message. Filters out non-text content.
func lastUserText(msgs []*proto.Message) (string, bool) {
	for i := len(msgs) - 1; i >= 0; i-- {
		m := msgs[i]
		if m == nil {
			continue
		}
		t := m.GetContent().GetText()
		if t == nil {
			continue
		}
		return t.Text, true
	}
	return "", false
}

// subprocessPythonExecutor is the production executor; runs `python3 -c
// <source>` and captures stdio. The surrounding Sandbox/gVisor pod
// provides the actual isolation — this just needs to exec the local
// python3 binary.
type subprocessPythonExecutor struct{}

func (subprocessPythonExecutor) Run(ctx context.Context, source string) (string, string, int, error) {
	cmd := exec.CommandContext(ctx, "python3", "-c", source)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	exitCode := 0
	if exitErr, ok := err.(*exec.ExitError); ok {
		exitCode = exitErr.ExitCode()
		err = nil // non-zero exit is expected for some programs; not a runner error
	}
	return stdout.String(), stderr.String(), exitCode, err
}
