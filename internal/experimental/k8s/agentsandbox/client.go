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

// Package agentsandbox wraps the kubernetes-sigs/agent-sandbox API as a
// minimal client tailored to AX's lifecycle needs (create/wait-ready/delete).
// Parallels internal/experimental/k8s/ate.Client but targets the
// agent-sandbox CRDs instead of Substrate's ate.dev/v1alpha1 resources.
package agentsandbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	sandboxv1alpha1 "sigs.k8s.io/agent-sandbox/api/v1alpha1"
	extv1alpha1 "sigs.k8s.io/agent-sandbox/extensions/api/v1alpha1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// scheme is the runtime.Scheme used by the controller-runtime client. We
// register the upstream agent-sandbox types alongside core K8s.
var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(sandboxv1alpha1.AddToScheme(scheme))
	utilruntime.Must(extv1alpha1.AddToScheme(scheme))
}

// Client manages Sandbox CRs in a single namespace for a single template.
// One Client per harness backend instance.
type Client struct {
	k8s       ctrlclient.Client
	namespace string
	template  string

	// readyTimeout caps how long CreateSandbox will wait for the controller
	// to populate .status.podIP. 120s matches upstream's defaults; cold
	// image pulls on a fresh node can exceed this without a warm pool.
	readyTimeout time.Duration
	// readyPollInterval is how often we re-fetch the Sandbox while waiting.
	readyPollInterval time.Duration
}

// Option configures a Client at construction.
type Option func(*Client)

// WithReadyTimeout overrides the per-CreateSandbox wait timeout.
func WithReadyTimeout(d time.Duration) Option {
	return func(c *Client) { c.readyTimeout = d }
}

// WithReadyPollInterval overrides how often we re-check Sandbox readiness.
func WithReadyPollInterval(d time.Duration) Option {
	return func(c *Client) { c.readyPollInterval = d }
}

// WithCtrlClient injects a pre-built controller-runtime client. Tests use
// this to provide the fake client.
func WithCtrlClient(c ctrlclient.Client) Option {
	return func(cl *Client) { cl.k8s = c }
}

// NewClient builds a Client. If WithCtrlClient is not provided, the
// in-cluster kubeconfig is used (with KUBECONFIG fallback for local dev).
func NewClient(namespace, template string, opts ...Option) (*Client, error) {
	if namespace == "" {
		return nil, errors.New("namespace is required")
	}
	if template == "" {
		return nil, errors.New("template is required")
	}
	c := &Client{
		namespace:         namespace,
		template:          template,
		readyTimeout:      2 * time.Minute,
		readyPollInterval: 500 * time.Millisecond,
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.k8s == nil {
		cfg, err := loadKubeConfig()
		if err != nil {
			return nil, fmt.Errorf("loading kubeconfig: %w", err)
		}
		k, err := ctrlclient.New(cfg, ctrlclient.Options{Scheme: scheme})
		if err != nil {
			return nil, fmt.Errorf("building k8s client: %w", err)
		}
		c.k8s = k
	}
	return c, nil
}

func loadKubeConfig() (*rest.Config, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(),
		&clientcmd.ConfigOverrides{},
	).ClientConfig()
}

// Sandbox is the small subset of state callers need after a successful
// CreateSandbox. We avoid leaking the full sandboxv1alpha1.Sandbox here so
// callers don't grow dependencies on the upstream API types.
type Sandbox struct {
	Name      string
	Namespace string
	PodIP     string
}

// CreateSandbox creates a SandboxClaim referencing the configured
// SandboxTemplate. The agent-sandbox controller resolves the claim by
// adopting a pod from a matching SandboxWarmPool (or by creating a fresh
// Sandbox if no warm replica is available), and populates
// status.sandbox.podIPs once the pod is ready.
//
// Blocks until status.sandbox.podIPs[0] is populated or readyTimeout
// elapses. Creating a raw Sandbox CR with an empty PodSpec fails server
// admission (`spec.podTemplate.spec.containers: Required value`), so the
// SandboxClaim path is the only template-driven route.
//
// If a SandboxClaim with the same name already exists, CreateSandbox
// treats it as an adopt — useful for resuming a conversation whose
// underlying pod survived an ax-server restart.
func (c *Client) CreateSandbox(ctx context.Context, name string) (*Sandbox, error) {
	if name == "" {
		return nil, errors.New("sandbox name is required")
	}
	claim := newClaimObject(name, c.namespace, c.template)
	if err := c.k8s.Create(ctx, claim); err != nil && !apierrors.IsAlreadyExists(err) {
		return nil, fmt.Errorf("creating SandboxClaim %s/%s: %w", c.namespace, name, err)
	}
	return c.waitForReady(ctx, name)
}

// DeleteSandbox tears down a SandboxClaim. The agent-sandbox controller
// garbage-collects the underlying Sandbox CR. Idempotent — already-gone
// is fine.
func (c *Client) DeleteSandbox(ctx context.Context, name string) error {
	claim := &extv1alpha1.SandboxClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: c.namespace},
	}
	if err := c.k8s.Delete(ctx, claim); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("deleting SandboxClaim %s/%s: %w", c.namespace, name, err)
	}
	return nil
}

// waitForReady polls the SandboxClaim until status.sandbox.podIPs has at
// least one entry, or the timeout elapses. Returns a populated Sandbox
// handle on success.
func (c *Client) waitForReady(ctx context.Context, name string) (*Sandbox, error) {
	deadline := time.Now().Add(c.readyTimeout)
	for {
		var got extv1alpha1.SandboxClaim
		err := c.k8s.Get(ctx, types.NamespacedName{Name: name, Namespace: c.namespace}, &got)
		if err != nil {
			return nil, fmt.Errorf("get SandboxClaim %s/%s: %w", c.namespace, name, err)
		}
		if ips := got.Status.SandboxStatus.PodIPs; len(ips) > 0 && ips[0] != "" {
			return &Sandbox{Name: name, Namespace: c.namespace, PodIP: ips[0]}, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("SandboxClaim %s/%s did not become ready within %s", c.namespace, name, c.readyTimeout)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(c.readyPollInterval):
		}
	}
}

// newClaimObject builds a SandboxClaim CR referencing the configured
// SandboxTemplate. The controller handles all the actual container
// spec defaulting + warm pool adoption.
func newClaimObject(name, namespace, template string) *extv1alpha1.SandboxClaim {
	return &extv1alpha1.SandboxClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"ax.google/sandbox-template": template,
				"ax.google/managed":          "true",
			},
		},
		Spec: extv1alpha1.SandboxClaimSpec{
			TemplateRef: extv1alpha1.SandboxTemplateRef{Name: template},
		},
	}
}
