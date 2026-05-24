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
M workers (per their demo: 250:8); agent-sandbox is 1:1. For modest scale
(dozens of conversations/day) 1:1 is fine and the extra simplicity is welcome.

## Selection

A new env gate `AX_AGENT_SANDBOX=1` selects this backend, mutually
exclusive with `AX_SUBSTRATE=1`. Both unset → in-process (existing
behavior).

## Files

- `internal/experimental/k8s/agentsandbox/client.go` — k8s client wrapper ✅
- `internal/experimental/k8s/agentsandbox/client_test.go` — 10 tests ✅
- `internal/harness/agentsandbox.go` — `AgentSandboxHarness` + `agentSandboxExecution` ✅
- `internal/harness/agentsandbox_test.go` — 8 tests ✅
- `internal/controller2/registry_agentsandbox.go` — TODO, blocked on upstream
- `cmd/ax/main.go` env-gated selection — TODO, blocked on upstream

## Why we stopped at Phase B

`NewSubstrateHarness` is **never called anywhere** in AX `main` as of 2026-05-23.
The harness type exists; the wiring into a running `ax serve` does not.
Wiring happens at the Agent registry layer (`controller2/Registry`), not the
Harness layer — these are distinct abstractions:

- `harness.Harness` (this file): per-conversation execution boundary; what we
  implemented in `agentsandbox.go`.
- `agent.Agent` (controller2/registry): per-task callable invokable by name;
  what AX's runtime actually dispatches to. `RegisterATE` registers a
  `SubstrateAgent` from `internal/experimental/agent/`, not a
  `SubstrateHarness` from this package.

Upstream has an in-progress `u/anj/harness-interface-2` branch that adds an
`Antigravity` harness + new `controller2/registry` wiring (commit `7b7d8bd`,
12 files changed). `SUBSTRATE_REFACTOR_CLEANUPS.md` also lists "Update
HarnessService with the actual protocol" — confirming the protocol layer
is still moving.

If we add a `RegisterAgentSandbox` to the registry now, it will conflict
with whichever pattern upstream merges. So we hold here until either:

  1. `u/anj/harness-interface-2` (or its successor) merges to main, OR
  2. We get explicit signal from upstream on the harness↔agent contract.

When that happens the integration work is small:
  - mirror whatever `RegisterAntigravity` (or equivalent) ends up looking like
  - thread the `AgentSandboxHarness` through that registration
  - env-gate selection (`AX_AGENT_SANDBOX=1`) in `cmd/ax/main.go`
