# Agent Sandbox harness backend (WIP)

This package will gain an `AgentSandboxHarness` type that implements the
`Harness` interface against `kubernetes-sigs/agent-sandbox` CRDs, in
parallel to the existing `SubstrateHarness`.

## Why

Agent Substrate (the default AX backend today) requires privileged
container access (`atelet` DaemonSet) and the
`certificates.k8s.io/v1beta1` alpha API. Neither is available on
managed GKE — Autopilot blocks privileged containers, and even
`--enable-kubernetes-alpha` clusters don't expose the `ClusterTrustBundle`
/ `PodCertificateRequest` APIs Substrate depends on.

`kubernetes-sigs/agent-sandbox` (GA v0.4.6) provides equivalent
gVisor-isolated sandbox pods via `agents.x-k8s.io/v1alpha1.Sandbox` +
`extensions.agents.x-k8s.io/v1alpha1.{SandboxTemplate,SandboxWarmPool}`,
works on Autopilot, and is what Google's GKE docs document as the GA
production path for agent sandboxing.

This backend lets AX target agent-sandbox instead of Substrate.

## Mapping

| Substrate (`ate.dev`) | agent-sandbox (`agents.x-k8s.io`) |
|---|---|
| `WorkerPool` (M pods shared by N actors) | `SandboxWarmPool` (M pre-warmed pods adopted 1:1) |
| `ActorTemplate` | `SandboxTemplate` |
| `Actor` (N:M multiplexed) | `Sandbox` (1:1, no multiplexing) |
| `ateomPodIp` for gRPC dial target | `Sandbox.status.podIP` |
| `CreateActor(id)` → assigns to a worker | `create_sandbox` adopts from warmpool, returns ready pod |
| `SuspendActor(id)` → CRIU snapshot | (TBD) GKE Pod Snapshots via `PodSnapshotSandboxClient` |

Conceptually 1:1 except for density: Substrate multiplexes N actors onto
M workers (per their demo: 250:8); agent-sandbox is 1:1. For uno scale
(dozens of mentions/day) 1:1 is fine and the extra simplicity is welcome.

## Selection

A new env gate `AX_AGENT_SANDBOX=1` selects this backend, mutually
exclusive with `AX_SUBSTRATE=1`. Both unset → in-process (existing
behavior).

## Files

- `internal/harness/agentsandbox.go` — `AgentSandboxHarness` + `agentSandboxExecution`
- `internal/experimental/k8s/agentsandbox/client.go` — k8s client wrapper
- `internal/harness/agentsandbox_test.go` — unit tests with mocked client
- `internal/server/server_agentsandbox.go` — selection in server startup
