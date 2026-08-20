# Running Sandboxes on the Substrate Backend (POC)

This example shows how to run E2B-compatible sandboxes on the
[Substrate](https://github.com/agent-substrate/substrate) backend instead of the
default Sandbox CR backend.

> **Status: proof of concept.** See [Limitations](#limitations) before using it
> for anything beyond evaluation.

## Concepts

The substrate backend splits a sandbox into three resources, each owned by a
different plane:

| Concept | Owned by | Role |
|---|---|---|
| **WorkerPool** | `SandboxSet` (declarative) | Capacity: how many worker pods and what shape. |
| **ActorTemplate** | E2B template build API (imperative) | An immutable build artifact — the "image" an actor is created from. |
| **Actor** | E2B create API (imperative) | One running sandbox instance placed on a pool's worker. |

Key differences from the default backend:

- A substrate-backed `SandboxSet` materializes **only a WorkerPool**. It never
  creates `Sandbox` or `SandboxClaim` CRs.
- Actor lifecycle (create / pause / resume / kill) is driven imperatively by
  `sandbox-manager` through the Substrate control gRPC API.
- Sub-second resume comes from restoring an actor's snapshot rather than cold
  starting a Pod.

## 0. Prerequisites

- A Kubernetes cluster running Substrate (SPIRE-free; the Substrate control
  plane, WorkerPool controller and CRDs must be installed and reachable).
- `sandbox-manager` started with the substrate flags:

  ```bash
  sandbox-manager \
    --substrate-addr substrate-api.substrate-system.svc:50051 \
    --substrate-ca-file /etc/substrate/ca.pem \
    --substrate-pause-image registry.k8s.io/pause:3.10.2@sha256:<digest> \
    --substrate-snapshots-location s3://ack-ate-snapshots \
    --substrate-sandbox-class gvisor \
    --substrate-hibernate-mode suspend
  ```

  Use `--substrate-addr insecure://host:50051` to dial in plaintext for local
  testing. When the address is empty, sandbox-manager keeps the default Sandbox
  CR backend.

## 1. Declare capacity

Apply the [SandboxSet](./sandboxset.yaml). The controller reconciles it into a
Substrate `WorkerPool`:

```bash
kubectl apply -f sandboxset.yaml
kubectl get workerpool -n ate-demo-counter
```

Only three annotations are substrate-specific — everything else is a normal pod
template:

- `agents.kruise.io/backend: substrate` — route to the substrate backend.
- `substrate.agents.kruise.io/sandbox-class` — `gvisor` or `microvm`.
- `substrate.agents.kruise.io/hibernate-mode` — `suspend` (free the worker) or
  `pause` (keep the worker).

The first container's image becomes the pool's `ateomImage` and its resources
become the per-worker pod resources.

## 2. Build a template and run a sandbox

[`demo.py`](./demo.py) walks the full flow: build an ActorTemplate through the
three-phase E2B build API, wait for it to become ready, then create / pause /
resume / kill a sandbox.

```bash
pip install e2b requests

export E2B_DOMAIN=sandbox-manager.example.com:8080
export E2B_API_KEY=<team-api-key>

python demo.py \
  --template counter \
  --from-image registry.example.com/counter@sha256:<digest> \
  --start-cmd "/ko-app/counter" \
  --ready-cmd "http://localhost:8080/readyz" \
  --sandboxset counter
```

### Template build is REST, not the SDK builder

The E2B SDK's high-level `Template.build()` assumes a build pipeline
(`RUN`/`COPY` steps). The substrate backend does not run a build pipeline — it
only accepts a prebuilt, **digest-pinned** `fromImage` — so the demo calls the
build endpoints directly:

1. `POST /v3/templates` → reserves `{templateID, buildID}`.
2. `POST /v2/templates/{templateID}/builds/{buildID}` → submits `fromImage`,
   `startCmd`, `readyCmd`; sandbox-manager creates the ActorTemplate.
3. `GET /templates/{templateID}/builds/{buildID}/status` → poll until `ready`.

### Choosing a worker pool

When several `SandboxSet`s exist in a namespace, pin a sandbox to one pool by
passing its name as create metadata:

```python
Sandbox.create(
    template="counter",
    metadata={"e2b.agents.kruise.io/sandboxset": "counter-hipri"},
)
```

Omit the key to let the actor land on any eligible pool in the namespace. This
maps to the actor's `worker_selector`, which narrows the pools the template
allows.

## Limitations

This POC deliberately does not yet support:

1. **Metadata persistence.** The sandbox-ID → actor mapping is kept in
   sandbox-manager memory. A restart orphans running actors — they keep a worker
   but are no longer reachable through the API. Reconciling them requires an
   external step.
2. **Build steps.** Only `fromImage` (digest-pinned) is honored; `RUN`/`COPY`
   steps and `fromTemplate` are rejected.
3. **Non-HTTP readiness checks.** Only `wait_for_url` / `wait_for_port` map onto
   the actor's HTTP probe; `wait_for_process` / `wait_for_file` are rejected.
4. **Clone, checkpoint and CSI mounts** on substrate actors.
5. **Rolling upgrade** of running actors when a template changes; existing
   actors are not migrated.
