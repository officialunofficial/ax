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

package gemini

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/ax/internal/agent"
	"github.com/google/ax/internal/config"
	"github.com/google/ax/proto"
	"google.golang.org/genai"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestProtoToContents(t *testing.T) {
	inputs := []*proto.Message{
		{
			Role: "user",
			Content: &proto.Content{
				Type: &proto.Content_Text{
					Text: &proto.TextContent{Text: "hello"},
				},
			},
		},
		{
			Role: "model",
			Content: &proto.Content{
				Type: &proto.Content_ToolCall{
					ToolCall: &proto.ToolCallContent{
						Id: "call-123",
						Type: &proto.ToolCallContent_FunctionCall{
							FunctionCall: &proto.FunctionCallContent{
								Name: "test_tool",
								Arguments: &structpb.Struct{
									Fields: map[string]*structpb.Value{
										"arg1": structpb.NewStringValue("val1"),
									},
								},
							},
						},
					},
				},
			},
		},
	}

	contents := protoToContents(inputs)

	if len(contents) != 2 {
		t.Fatalf("expected 2 contents, got %d", len(contents))
	}

	if contents[0].Role != "user" {
		t.Errorf("expected role user, got %s", contents[0].Role)
	}
	if contents[0].Parts[0].Text != "hello" {
		t.Errorf("expected text hello, got %s", contents[0].Parts[0].Text)
	}

	if contents[1].Role != "model" {
		t.Errorf("expected role model, got %s", contents[1].Role)
	}
	fc := contents[1].Parts[0].FunctionCall
	if fc == nil {
		t.Fatal("expected function call")
	}
	if fc.Name != "test_tool" {
		t.Errorf("expected function name test_tool, got %s", fc.Name)
	}
}

func TestHandleConfirmationAnswer(t *testing.T) {
	inputs := []*proto.Message{
		{
			Role: "model",
			Content: &proto.Content{
				Type: &proto.Content_ToolCall{
					ToolCall: &proto.ToolCallContent{
						Id: "call-123",
						Type: &proto.ToolCallContent_FunctionCall{
							FunctionCall: &proto.FunctionCallContent{
								Name: "bash",
								Arguments: &structpb.Struct{
									Fields: map[string]*structpb.Value{
										"command": structpb.NewStringValue("ls"),
									},
								},
							},
						},
					},
				},
			},
		},
		{
			Role: "user",
			Content: &proto.Content{
				Type: &proto.Content_Confirmation{
					Confirmation: &proto.ConfirmationContent{
						Id:       "call-123",
						Decision: &proto.ConfirmationContent_Approval{Approval: &proto.ApprovalDecision{Approved: true}},
					},
				},
			},
		},
	}

	p := &geminiPlannerAgent{
		bashTool: &BashTool{},
	}

	fc, approved := p.handleConfirmationAnswer(inputs)

	if fc == nil {
		t.Fatal("expected function call")
	}
	if fc.ID != "call-123" {
		t.Errorf("expected ID call-123, got %s", fc.ID)
	}
	if !approved {
		t.Error("expected approved to be true")
	}
}

type mockContentGenerator struct {
	generateContentFunc func(ctx context.Context, model string, contents []*genai.Content, config *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error)
}

func (m *mockContentGenerator) GenerateContent(ctx context.Context, model string, contents []*genai.Content, config *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
	if m.generateContentFunc != nil {
		return m.generateContentFunc(ctx, model, contents, config)
	}
	return nil, nil
}

type mockExecutor struct {
	execFunc func(ctx context.Context, conversationID string, execID string, start *proto.AgentStart, o agent.OutputHandler) (proto.State, error)
}

func (m *mockExecutor) Exec(ctx context.Context, conversationID string, execID string, start *proto.AgentStart, o agent.OutputHandler) (proto.State, error) {
	if m.execFunc != nil {
		return m.execFunc(ctx, conversationID, execID, start, o)
	}
	return proto.State_STATE_COMPLETED, nil
}

type mockAgentRegistry struct {
	listFunc    func() []string
	getInfoFunc func(id string) (*agent.AgentInfo, error)
}

func (m *mockAgentRegistry) List() []string {
	if m.listFunc != nil {
		return m.listFunc()
	}
	return nil
}

func (m *mockAgentRegistry) GetInfo(id string) (*agent.AgentInfo, error) {
	if m.getInfoFunc != nil {
		return m.getInfoFunc(id)
	}
	return nil, nil
}

func TestAgentsToTools_Parameters(t *testing.T) {
	registry := &mockAgentRegistry{
		listFunc: func() []string {
			return []string{"test-agent"}
		},
		getInfoFunc: func(id string) (*agent.AgentInfo, error) {
			return &agent.AgentInfo{
				ID:          "test-agent",
				Name:        "Test Agent",
				Description: "A test agent",
			}, nil
		},
	}

	tools, err := agentsToTools(registry)
	if err != nil {
		t.Fatalf("agentsToTools failed: %v", err)
	}

	if len(tools) != 1 {
		t.Fatalf("expected 1 tool, got %d", len(tools))
	}

	decl := tools[0].FunctionDeclarations[0]
	if decl.Name != "test_agent" {
		t.Errorf("expected name test_agent, got %s", decl.Name)
	}

	params := decl.Parameters
	if params == nil {
		t.Fatal("expected parameters")
	}

	if params.Type != genai.TypeObject {
		t.Errorf("expected type object, got %v", params.Type)
	}

	if _, ok := params.Properties["history"]; !ok {
		t.Error("expected 'history' property")
	}
	if _, ok := params.Properties["prompt"]; !ok {
		t.Error("expected 'prompt' property")
	}

	required := params.Required
	if len(required) != 2 {
		t.Errorf("expected 2 required properties, got %d", len(required))
	}

	foundHistory := false
	foundPrompt := false
	for _, r := range required {
		if r == "history" {
			foundHistory = true
		}
		if r == "prompt" {
			foundPrompt = true
		}
	}
	if !foundHistory {
		t.Error("expected 'history' to be required")
	}
	if !foundPrompt {
		t.Error("expected 'prompt' to be required")
	}
}

func TestHandleSubagentCall_Success(t *testing.T) {
	mockGen := &mockContentGenerator{
		generateContentFunc: func(ctx context.Context, model string, contents []*genai.Content, config *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
			return &genai.GenerateContentResponse{
				Candidates: []*genai.Candidate{
					{
						Content: &genai.Content{
							Parts: []*genai.Part{
								{Text: "Subagent work summary."},
							},
						},
					},
				},
			}, nil
		},
	}

	mockExec := &mockExecutor{
		execFunc: func(ctx context.Context, conversationID string, execID string, start *proto.AgentStart, o agent.OutputHandler) (proto.State, error) {
			o(&proto.AgentOutputs{
				Messages: []*proto.Message{
					{
						Role: "model",
						Content: &proto.Content{
							Type: &proto.Content_Text{
								Text: &proto.TextContent{Text: "Subagent output step 1"},
							},
						},
					},
				},
			})
			return proto.State_STATE_COMPLETED, nil
		},
	}

	p := &geminiPlannerAgent{
		client: mockGen,
		config: GeminiPlannerConfig{
			GeminiConfig: &config.GeminiConfig{Model: "test-model"},
		},
	}

	fc := &genai.FunctionCall{
		Name: "test-subagent",
		Args: map[string]any{
			"history": "Previous history summary",
			"prompt":  "Current user prompt",
		},
	}

	var handlerCalled bool
	var handlerOutput *proto.AgentOutputs
	handler := func(outgoing *proto.AgentOutputs) error {
		handlerCalled = true
		handlerOutput = outgoing
		return nil
	}

	err := p.handleSubagentCall(context.Background(), "test-conv", fc, nil, nil, mockExec, handler)
	if err != nil {
		t.Fatalf("handleSubagentCall failed: %v", err)
	}

	if !handlerCalled {
		t.Fatal("expected handler to be called")
	}

	if len(handlerOutput.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(handlerOutput.Messages))
	}
	msg := handlerOutput.Messages[0]
	tr := msg.Content.GetToolResult()
	if tr == nil {
		t.Fatal("expected tool result")
	}
	fr := tr.GetFunctionResult()
	if fr == nil {
		t.Fatal("expected function result")
	}
	if fr.Name != "test-subagent" {
		t.Errorf("expected name test-subagent, got %s", fr.Name)
	}
	resp := fr.GetResponse()
	if resp == nil {
		t.Fatal("expected response")
	}
	resultVal := resp.Fields["result"].GetStringValue()
	if resultVal != "Subagent output step 1\n" {
		t.Errorf("expected 'Subagent output step 1\\n', got %s", resultVal)
	}
}

func TestHandleSubagentCall_MissingArgs(t *testing.T) {
	p := &geminiPlannerAgent{}
	fc := &genai.FunctionCall{
		Name: "test-subagent",
		Args: map[string]any{},
	}

	err := p.handleSubagentCall(context.Background(), "test-conv", fc, nil, nil, nil, nil)
	if err == nil {
		t.Fatal("expected error due to missing arguments")
	}
	if !strings.Contains(err.Error(), "missing or invalid 'prompt' argument") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestHandleSubagentCall_Failure(t *testing.T) {
	mockExec := &mockExecutor{
		execFunc: func(ctx context.Context, conversationID string, execID string, start *proto.AgentStart, o agent.OutputHandler) (proto.State, error) {
			return proto.State_STATE_UNSPECIFIED, fmt.Errorf("subagent execution failed")
		},
	}

	p := &geminiPlannerAgent{}
	fc := &genai.FunctionCall{
		Name: "test-subagent",
		Args: map[string]any{
			"history": "Previous history summary",
			"prompt":  "Current user prompt",
		},
	}

	var handlerOutput *proto.AgentOutputs
	handler := func(outgoing *proto.AgentOutputs) error {
		handlerOutput = outgoing
		return nil
	}

	err := p.handleSubagentCall(context.Background(), "test-conv", fc, nil, nil, mockExec, handler)
	if err != nil {
		t.Fatalf("handleSubagentCall failed: %v", err)
	}

	msg := handlerOutput.Messages[0]
	tr := msg.Content.GetToolResult()
	fr := tr.GetFunctionResult()
	resp := fr.GetResponse()
	resultVal := resp.Fields["result"].GetStringValue()
	if resultVal != "sub agent failed" {
		t.Errorf("expected 'sub agent failed', got %s", resultVal)
	}
	errVal := resp.Fields["error"].GetStringValue()
	if !strings.Contains(errVal, "subagent execution failed") {
		t.Errorf("expected error to contain 'subagent execution failed', got %s", errVal)
	}
}

func TestHandleSubagentCall_Panic(t *testing.T) {
	mockExec := &mockExecutor{
		execFunc: func(ctx context.Context, conversationID string, execID string, start *proto.AgentStart, o agent.OutputHandler) (proto.State, error) {
			panic("something went wrong")
		},
	}

	p := &geminiPlannerAgent{}
	fc := &genai.FunctionCall{
		Name: "test-subagent",
		Args: map[string]any{
			"history": "Previous history summary",
			"prompt":  "Current user prompt",
		},
	}

	var handlerOutput *proto.AgentOutputs
	handler := func(outgoing *proto.AgentOutputs) error {
		handlerOutput = outgoing
		return nil
	}

	err := p.handleSubagentCall(context.Background(), "test-conv", fc, nil, nil, mockExec, handler)
	if err != nil {
		t.Fatalf("handleSubagentCall failed: %v", err)
	}

	msg := handlerOutput.Messages[0]
	tr := msg.Content.GetToolResult()
	fr := tr.GetFunctionResult()
	resp := fr.GetResponse()
	resultVal := resp.Fields["result"].GetStringValue()
	if resultVal != "sub agent failed" {
		t.Errorf("expected 'sub agent failed', got %s", resultVal)
	}
	errVal := resp.Fields["error"].GetStringValue()
	if !strings.Contains(errVal, "panic: something went wrong") {
		t.Errorf("expected error to contain 'panic: something went wrong', got %s", errVal)
	}
	if !strings.Contains(errVal, "goroutine") {
		t.Errorf("expected stack trace in error, got %s", errVal)
	}
}

// TestNewGeminiPlannerAgent_NoSkillsPrompt verifies that the Gemini planner agent
// system prompt does not contain the available_skills block when skills are disabled.
func TestNewGeminiPlannerAgent_NoSkillsPrompt(t *testing.T) {
	t.Setenv("GEMINI_API_KEY", "mock-key")
	t.Setenv("SKILLS_DIR", "")

	registry := &mockAgentRegistry{}
	cfg := GeminiPlannerConfig{
		GeminiConfig: &config.GeminiConfig{
			SystemPrompt: "You are AX.",
		},
		SkillsDir: "",
	}

	agent, err := NewGeminiPlannerAgent(context.Background(), registry, cfg)
	if err != nil {
		t.Fatalf("NewGeminiPlannerAgent failed: %v", err)
	}

	p, ok := agent.(*geminiPlannerAgent)
	if !ok {
		t.Fatalf("expected agent to be *geminiPlannerAgent, got %T", agent)
	}

	prompt := p.config.GeminiConfig.SystemPrompt
	if strings.Contains(prompt, "<available_skills>") {
		t.Errorf("expected system prompt to not contain '<available_skills>', got: %s", prompt)
	}
}


// TestProcess_AppendsNativeToolsFromConfig asserts that native Gemini
// tools listed in GeminiConfig.Tools (e.g. "google_search") get added
// to the GenerateContent request alongside the registered AX subagent
// function declarations. This is how the planner gets web-grounding
// without us building a websearch subagent.
func TestProcess_AppendsNativeToolsFromConfig(t *testing.T) {
	var captured *genai.GenerateContentConfig
	mockGen := &mockContentGenerator{
		generateContentFunc: func(ctx context.Context, model string, contents []*genai.Content, cfg *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
			captured = cfg
			return &genai.GenerateContentResponse{
				Candidates: []*genai.Candidate{{
					Content: &genai.Content{Parts: []*genai.Part{{Text: "ok"}}},
				}},
			}, nil
		},
	}

	registry := &mockAgentRegistry{
		listFunc: func() []string { return nil },
	}

	p := &geminiPlannerAgent{
		client:   mockGen,
		registry: registry,
		config: GeminiPlannerConfig{
			GeminiConfig: &config.GeminiConfig{
				Model:        "test-model",
				SystemPrompt: "test",
				Tools:        []string{"google_search"},
			},
		},
	}

	start := &proto.AgentStart{Messages: []*proto.Message{{
		Role: "user",
		Content: &proto.Content{
			Type: &proto.Content_Text{Text: &proto.TextContent{Text: "what is the news"}},
		},
	}}}
	_, _, err := p.process(context.Background(), "conv-native-tools", start, nil, func(o *proto.AgentOutputs) error { return nil })
	if err != nil {
		t.Fatalf("process: %v", err)
	}

	if captured == nil {
		t.Fatal("GenerateContent was never called")
	}
	foundGoogleSearch := false
	for _, tool := range captured.Tools {
		if tool != nil && tool.GoogleSearch != nil {
			foundGoogleSearch = true
		}
	}
	if !foundGoogleSearch {
		t.Errorf("captured config.Tools does not contain a GoogleSearch entry; got %d tools", len(captured.Tools))
	}
}

// TestProcess_MergesNativeToolsWithFunctionDeclarations locks in the Gemini
// docs requirement: when native tools (like GoogleSearch) coexist with
// custom function declarations, they must share ONE Tool object. Splitting
// across multiple Tool entries causes Gemini 3 to emit the native tool's
// name as a regular function call instead of auto-executing it server-side
// (verified empirically against gemini-3-flash-preview on Vertex).
//
// Expected shape: every FunctionDeclaration the planner produced for
// registered AX agents lives on the SAME *genai.Tool that carries the
// GoogleSearch (or other built-in) declaration.
func TestProcess_MergesNativeToolsWithFunctionDeclarations(t *testing.T) {
	var captured *genai.GenerateContentConfig
	mockGen := &mockContentGenerator{
		generateContentFunc: func(ctx context.Context, model string, contents []*genai.Content, cfg *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
			captured = cfg
			return &genai.GenerateContentResponse{
				Candidates: []*genai.Candidate{{
					Content: &genai.Content{Parts: []*genai.Part{{Text: "ok"}}},
				}},
			}, nil
		},
	}

	registry := &mockAgentRegistry{
		listFunc: func() []string { return []string{"agent-a", "agent-b"} },
		getInfoFunc: func(id string) (*agent.AgentInfo, error) {
			return &agent.AgentInfo{ID: id, Name: id, Description: "test"}, nil
		},
	}

	p := &geminiPlannerAgent{
		client:   mockGen,
		registry: registry,
		config: GeminiPlannerConfig{
			GeminiConfig: &config.GeminiConfig{
				Model:        "test-model",
				SystemPrompt: "test",
				Tools:        []string{"google_search"},
			},
		},
	}

	start := &proto.AgentStart{Messages: []*proto.Message{{
		Role: "user",
		Content: &proto.Content{
			Type: &proto.Content_Text{Text: &proto.TextContent{Text: "x"}},
		},
	}}}
	_, _, err := p.process(context.Background(), "conv-merged", start, nil, func(o *proto.AgentOutputs) error { return nil })
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if captured == nil {
		t.Fatal("GenerateContent was never called")
	}

	// Find the Tool carrying GoogleSearch and assert it ALSO has the
	// function declarations (not split across separate Tool entries).
	var gsTool *genai.Tool
	for _, tool := range captured.Tools {
		if tool != nil && tool.GoogleSearch != nil {
			gsTool = tool
			break
		}
	}
	if gsTool == nil {
		t.Fatal("no Tool carries GoogleSearch")
	}
	if len(gsTool.FunctionDeclarations) < 2 {
		t.Errorf("GoogleSearch Tool has %d FunctionDeclarations; want >=2 (agent-a + agent-b merged onto same Tool)", len(gsTool.FunctionDeclarations))
	}
	// Belt+suspenders: assert there are NOT also separate Tool entries
	// each holding one FunctionDeclaration (the old multi-Tool shape).
	if len(captured.Tools) != 1 {
		t.Errorf("captured.Tools has %d entries; want 1 merged Tool (combining FunctionDeclarations + GoogleSearch on one Tool is required by Gemini docs)", len(captured.Tools))
	}
}

// TestProcess_NoToolsWhenRegistryAndNativeBothEmpty guards against Vertex
// "400 INVALID_ARGUMENT: Tool must contain at least one of
// function_declarations, google_search, url_context, code_execution".
// When the agent registry is empty AND no native Gemini tools are
// configured, process() must not send a zero-valued *genai.Tool to the
// model — it should send no tools at all (nil or empty).
//
// Boot-time / empty-config deployments hit this when the planner runs
// with no registered subagents and no native tool list.
func TestProcess_NoToolsWhenRegistryAndNativeBothEmpty(t *testing.T) {
	var captured *genai.GenerateContentConfig
	mockGen := &mockContentGenerator{
		generateContentFunc: func(ctx context.Context, model string, contents []*genai.Content, cfg *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
			captured = cfg
			return &genai.GenerateContentResponse{
				Candidates: []*genai.Candidate{{
					Content: &genai.Content{Parts: []*genai.Part{{Text: "ok"}}},
				}},
			}, nil
		},
	}

	registry := &mockAgentRegistry{
		listFunc: func() []string { return nil },
	}

	p := &geminiPlannerAgent{
		client:   mockGen,
		registry: registry,
		config: GeminiPlannerConfig{
			GeminiConfig: &config.GeminiConfig{
				Model:        "test-model",
				SystemPrompt: "test",
				// Tools intentionally empty
			},
		},
	}

	start := &proto.AgentStart{Messages: []*proto.Message{{
		Role: "user",
		Content: &proto.Content{
			Type: &proto.Content_Text{Text: &proto.TextContent{Text: "hi"}},
		},
	}}}
	_, _, err := p.process(context.Background(), "conv-empty", start, nil, func(o *proto.AgentOutputs) error { return nil })
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if captured == nil {
		t.Fatal("GenerateContent was never called")
	}

	// We must not send a zero-valued *genai.Tool{}. Either nil Tools or
	// an empty slice is acceptable; a single empty Tool is not.
	if len(captured.Tools) == 0 {
		return // nil or empty — good
	}
	for i, tool := range captured.Tools {
		if tool == nil {
			continue
		}
		empty := len(tool.FunctionDeclarations) == 0 &&
			tool.GoogleSearch == nil &&
			tool.URLContext == nil &&
			tool.CodeExecution == nil &&
			tool.GoogleMaps == nil
		if empty {
			t.Errorf("captured.Tools[%d] is a zero-valued *genai.Tool; Vertex will reject this with 400 INVALID_ARGUMENT", i)
		}
	}
}

// fakeNativeTool is a minimal Tool stub that lets tests inject a Tool
// carrying native Gemini fields (GoogleSearch, URLContext, etc.) into
// the agentsToTools nativeTools variadic and, through it, the process()
// merge loop.
type fakeNativeTool struct {
	name  string
	tools []*genai.Tool
}

func (f *fakeNativeTool) Name() string            { return f.name }
func (f *fakeNativeTool) FuncDecl() []*genai.Tool { return f.tools }
func (f *fakeNativeTool) SystemPrompt() string    { return "" }
func (f *fakeNativeTool) HandleCall(ctx context.Context, fc *genai.FunctionCall, o agent.OutputHandler) error {
	return nil
}
func (f *fakeNativeTool) HandleExecute(ctx context.Context, fc *genai.FunctionCall, approved bool, o agent.OutputHandler) error {
	return nil
}

// TestProcess_MergePreservesNativeFieldsFromRawTools guards against a
// latent bug in the process() merge loop: when agentsToTools' nativeTools
// variadic produces a *genai.Tool with native fields set
// (GoogleSearch / URLContext / CodeExecution / GoogleMaps), the merge
// loop must copy those fields onto mergedTool — not just
// FunctionDeclarations.
//
// No current caller plumbs nativeTools into agentsToTools, so this is a
// latent bug, but future callers would silently lose native tools.
func TestProcess_MergePreservesNativeFieldsFromRawTools(t *testing.T) {
	var captured *genai.GenerateContentConfig
	mockGen := &mockContentGenerator{
		generateContentFunc: func(ctx context.Context, model string, contents []*genai.Content, cfg *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
			captured = cfg
			return &genai.GenerateContentResponse{
				Candidates: []*genai.Candidate{{
					Content: &genai.Content{Parts: []*genai.Part{{Text: "ok"}}},
				}},
			}, nil
		},
	}

	registry := &mockAgentRegistry{
		listFunc: func() []string { return nil },
	}

	// nativeTool emits a *genai.Tool with GoogleSearch set (the kind of
	// shape future callers of agentsToTools(registry, nativeTools…)
	// could pass in).
	nativeTool := &fakeNativeTool{
		name: "fake_native",
		tools: []*genai.Tool{{
			GoogleSearch: &genai.GoogleSearch{},
		}},
	}

	p := &geminiPlannerAgent{
		client:      mockGen,
		registry:    registry,
		nativeTools: []Tool{nativeTool},
		config: GeminiPlannerConfig{
			GeminiConfig: &config.GeminiConfig{
				Model:        "test-model",
				SystemPrompt: "test",
			},
		},
	}

	start := &proto.AgentStart{Messages: []*proto.Message{{
		Role: "user",
		Content: &proto.Content{
			Type: &proto.Content_Text{Text: &proto.TextContent{Text: "hi"}},
		},
	}}}
	_, _, err := p.process(context.Background(), "conv-native-merge", start, nil, func(o *proto.AgentOutputs) error { return nil })
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if captured == nil {
		t.Fatal("GenerateContent was never called")
	}

	if len(captured.Tools) == 0 {
		t.Fatal("captured.Tools is empty; expected a Tool carrying GoogleSearch")
	}
	if captured.Tools[0].GoogleSearch == nil {
		t.Errorf("merged Tool dropped GoogleSearch from raw native tool; merge loop only copied FunctionDeclarations")
	}
}
