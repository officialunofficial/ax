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

package agentsandbox

import (
	"context"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	sandboxv1alpha1 "sigs.k8s.io/agent-sandbox/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// newTestClient constructs a Client backed by a controller-runtime fake
// client with the agent-sandbox types registered. Tests use this instead
// of touching a real cluster.
func newTestClient(t *testing.T, opts ...Option) *Client {
	t.Helper()
	// controller-runtime's fake client treats Status as a subresource only
	// when explicitly told. Without this, Status().Update() succeeds
	// silently but the value doesn't survive a subsequent Get().
	fakeK8s := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&sandboxv1alpha1.Sandbox{}).
		Build()
	allOpts := append([]Option{
		WithCtrlClient(fakeK8s),
		// Fast polling so tests don't sit on the default 500ms.
		WithReadyPollInterval(5 * time.Millisecond),
		WithReadyTimeout(2 * time.Second),
	}, opts...)
	c, err := NewClient("agent-platform", "python-sandbox-template", allOpts...)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

func TestNewClient_RequiresNamespace(t *testing.T) {
	if _, err := NewClient("", "any-template"); err == nil {
		t.Fatal("expected error when namespace is empty")
	}
}

func TestNewClient_RequiresTemplate(t *testing.T) {
	if _, err := NewClient("any-ns", ""); err == nil {
		t.Fatal("expected error when template is empty")
	}
}

func TestCreateSandbox_CreatesCRAndWaitsForPodIP(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	// Simulate the controller populating .status.podIP shortly after Create.
	go func() {
		time.Sleep(20 * time.Millisecond)
		var sb sandboxv1alpha1.Sandbox
		_ = c.k8s.Get(ctx, types.NamespacedName{Name: "conv-1", Namespace: "agent-platform"}, &sb)
		sb.Status.PodIPs = []string{"10.0.0.42"}
		_ = c.k8s.Status().Update(ctx, &sb)
	}()

	got, err := c.CreateSandbox(ctx, "conv-1")
	if err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	if got.Name != "conv-1" || got.Namespace != "agent-platform" || got.PodIP != "10.0.0.42" {
		t.Errorf("unexpected sandbox: %+v", got)
	}
}

func TestCreateSandbox_RequiresName(t *testing.T) {
	c := newTestClient(t)
	if _, err := c.CreateSandbox(context.Background(), ""); err == nil {
		t.Fatal("expected error for empty name")
	}
}

func TestCreateSandbox_TimesOutWhenPodIPNeverArrives(t *testing.T) {
	c := newTestClient(t, WithReadyTimeout(80*time.Millisecond))
	_, err := c.CreateSandbox(context.Background(), "stuck")
	if err == nil || !strings.Contains(err.Error(), "did not become ready") {
		t.Fatalf("expected ready-timeout error, got %v", err)
	}
}

func TestCreateSandbox_AdoptsExistingSandbox(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	// Pre-create a Sandbox with status.podIP already set — simulates
	// reconnecting to a conversation whose pod survived ax-server restart.
	existing := &sandboxv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: "conv-2", Namespace: "agent-platform"},
	}
	if err := c.k8s.Create(ctx, existing); err != nil {
		t.Fatalf("seed Create: %v", err)
	}
	existing.Status.PodIPs = []string{"10.0.0.99"}
	if err := c.k8s.Status().Update(ctx, existing); err != nil {
		t.Fatalf("seed Status update: %v", err)
	}

	got, err := c.CreateSandbox(ctx, "conv-2")
	if err != nil {
		t.Fatalf("CreateSandbox should adopt existing: %v", err)
	}
	if got.PodIP != "10.0.0.99" {
		t.Errorf("want adopted PodIP 10.0.0.99, got %q", got.PodIP)
	}
}

func TestCreateSandbox_LabelsTheResource(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	// Drive the create then inspect the CR before status flips.
	created := make(chan struct{})
	go func() {
		time.Sleep(20 * time.Millisecond)
		var sb sandboxv1alpha1.Sandbox
		_ = c.k8s.Get(ctx, types.NamespacedName{Name: "conv-lbl", Namespace: "agent-platform"}, &sb)
		want := "python-sandbox-template"
		if sb.Labels["ax.google/sandbox-template"] != want {
			t.Errorf("label ax.google/sandbox-template = %q, want %q",
				sb.Labels["ax.google/sandbox-template"], want)
		}
		if sb.Labels["ax.google/managed"] != "true" {
			t.Errorf("missing ax.google/managed=true label")
		}
		sb.Status.PodIPs = []string{"10.0.0.1"}
		_ = c.k8s.Status().Update(ctx, &sb)
		close(created)
	}()

	if _, err := c.CreateSandbox(ctx, "conv-lbl"); err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	<-created
}

func TestDeleteSandbox_RemovesCR(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	if err := c.k8s.Create(ctx, &sandboxv1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: "to-delete", Namespace: "agent-platform"},
	}); err != nil {
		t.Fatalf("seed Create: %v", err)
	}
	if err := c.DeleteSandbox(ctx, "to-delete"); err != nil {
		t.Fatalf("DeleteSandbox: %v", err)
	}
	var sb sandboxv1alpha1.Sandbox
	err := c.k8s.Get(ctx, types.NamespacedName{Name: "to-delete", Namespace: "agent-platform"}, &sb)
	if err == nil || !apierrors.IsNotFound(err) {
		t.Errorf("expected NotFound after delete, got err=%v", err)
	}
}

func TestDeleteSandbox_IdempotentOnMissing(t *testing.T) {
	c := newTestClient(t)
	// Sandbox never existed — Delete should still succeed.
	if err := c.DeleteSandbox(context.Background(), "ghost"); err != nil {
		t.Errorf("DeleteSandbox of missing CR should be idempotent, got %v", err)
	}
}

// Ensure scheme registers the v1alpha1 types so the fake client can encode
// them. If this fails the rest of the suite errors are red herrings.
func TestSchemeKnowsSandboxKind(t *testing.T) {
	gvk := sandboxv1alpha1.GroupVersion.WithKind("Sandbox")
	if !scheme.Recognizes(gvk) {
		t.Fatalf("scheme does not recognize %v", gvk)
	}
	// And via the dynamic client API we should be able to construct a typed
	// object using the right group/version.
	obj := &sandboxv1alpha1.Sandbox{}
	if obj.GetObjectKind().GroupVersionKind().Kind == "" {
		// New unset objects have empty TypeMeta — this is fine, but we make
		// sure the client can set it from scheme on demand.
		gvks, _, err := scheme.ObjectKinds(obj)
		if err != nil || len(gvks) == 0 {
			t.Fatalf("scheme.ObjectKinds for Sandbox: gvks=%v err=%v", gvks, err)
		}
	}
	// Unused helper kept to ensure we depend on client package; remove
	// once we add a test that uses it directly.
	_ = client.Object(obj)
}
