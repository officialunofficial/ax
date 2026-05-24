# python_sandbox_agent

User-code half of the `feat/agent-sandbox-backend` integration. Implements
`proto.AgentService` over gRPC on `:8494` inside a
`kubernetes-sigs/agent-sandbox` Sandbox pod. The body of `Connect()`
extracts the last user-role message's text, runs it as Python 3, and
streams stdout/stderr back as a single `AgentResponse`.

## Where it fits

```
AX server (ax serve)
     │
     │ AgentSandboxAgent (this branch)
     ▼
SandboxClaim → Sandbox CR → gVisor pod  ← this image runs here
                            └─ python_sandbox_agent gRPC :8494
                            └─ python3 (subprocess executor)
```

The surrounding Sandbox (gVisor) provides the kernel-level isolation that
makes it safe to run untrusted LLM-generated code. This binary itself is
small and unprivileged — it just runs `python3 -c <source>`.

## Build

```bash
docker build \
  -f examples/python_sandbox_agent/Dockerfile \
  -t <registry>/<project>/python-sandbox-agent:<tag> .
```

Push the resulting image to a registry your `SandboxTemplate` can pull from.

## Configure AX to use it

In `ax.yaml`:

```yaml
registry:
  agent_sandbox_agents:
    - id: py
      name: Python Sandbox Agent
      namespace: agent-platform
      template: python-sandbox-template
      port: 8494
      protocol: axp
```

And in `agent-platform` namespace, a `SandboxTemplate` that points at
this image:

```yaml
apiVersion: extensions.agents.x-k8s.io/v1alpha1
kind: SandboxTemplate
metadata:
  name: python-sandbox-template
  namespace: agent-platform
spec:
  podTemplate:
    spec:
      runtimeClassName: gvisor
      restartPolicy: Never
      automountServiceAccountToken: false
      containers:
        - name: agent
          image: <registry>/<project>/python-sandbox-agent:latest
          ports: [{ containerPort: 8494 }]
          readinessProbe:
            tcpSocket: { port: 8494 }
            periodSeconds: 2
            failureThreshold: 30
          resources:
            requests: { cpu: 100m, memory: 256Mi }
            limits:   { cpu: 500m, memory: 512Mi }
```

## Smoke test (no K8s)

```bash
go run ./examples/python_sandbox_agent &
grpcurl -plaintext \
  -d '{"start":{"messages":[{"role":"user","content":{"text":{"text":"print(7*6)"}}}]}}' \
  localhost:8494 ax.AgentService/Connect
# => exit_code=0\nstdout:\n42\n\nstderr:\n
```

## Tests

```bash
go test ./examples/python_sandbox_agent/...
```

7 tests, all PASS — covering the executor injection contract, last-user
extraction, error paths, exec timeout, and non-zero exit handling.
