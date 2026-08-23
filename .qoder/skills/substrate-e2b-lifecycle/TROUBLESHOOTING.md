# Troubleshooting

Symptom to cause. Every entry cost a debugging round; the error text is quoted as
it appears so a search lands here.

Read the actual error before matching. Several of these produce a `404` whose body
is the only thing that separates them.

## The error body decides which gate rejected you

A sandbox request passes several checks that all answer `404`. The body names the
one that fired:

| Body | Gate |
|---|---|
| `Sandbox route not found, maybe it is crashed or killed` | the route is absent from the proxy table, or its owner does not match |
| `is not owned` | the owner annotation does not match, or the sandbox is still pooled |
| `is not healthy (state <x>)` | the state is outside the allow-list the endpoint accepts |

Confirm which one by log rather than by inference — the middleware logs
`failed to get owner of sandbox` for an absent route and `sandbox owner mismatch`
for a rejected one:

```bash
kubectl logs -n $NS $P -c controller --since=10m \
  | grep -iE "owner mismatch|failed to get owner|<request_id>"
```

## buildTemplate

**`must be pinned by digest (contain '@sha256:...')`** — `fromImage` carries a tag.
Read a digest from a template that already exists:

```bash
kubectl get actortemplates -n $ATESPACE -o json \
  | python3 -c "import json,sys;[print(c['image']) for i in json.load(sys.stdin)['items'] for c in i['spec']['containers']]"
```

**`Unsupported value: "microsandbox": supported values: "gvisor", "microvm"`** — the
CRD's enum lost its customisation. See `DEPLOY-REFERENCE.md`. Existing templates
keep working, so only a fresh build reveals this.

**`namespace and template name are required`** — the request used the admin key,
which resolves to no namespace. Use a team key.

**`ensure atespace`, with no matching RPC in the control-plane log** — the gRPC call
carried no credentials and was rejected by the authentication interceptor before
being recorded. Check the token file and its audience.

**Status stays `building` past a few minutes** — a golden actor is waiting for a
worker. Look for a crash-looping worker pod holding the assignment:

```bash
kubectl-ate get workers
kubectl get pods -n $ATESPACE
```

## create

**`state` is empty in a `201` response** — the client's Substrate proto and the
deployed control plane disagree about the actor status field, which moved from an
enum to a message. Align the `go.mod` pin with the deployed control-plane commit.

**500 on the first API-key creation, `nil pointer` at `routes.go`** — the namespace
lookup went through an informer cache the Substrate backend does not have
(`Infra.GetCache()` returns nil). Any code path reaching for that cache needs the
same treatment.

## pause and resume

**`GET` answers `404 Sandbox route not found` after pause** — a suspended actor has
no address, and a route was withheld for that reason. The route is what makes the
sandbox addressable at all, not only the gateway's forwarding entry, so it must be
published with an empty IP.

**A paused sandbox disappears from `GET` and `LIST` while Substrate shows
`ACTOR_STATE_SUSPENDED`** — the phase reported outward was `suspended`, which the
sandbox API vocabulary does not contain, so every allow-list and state filter
dropped it. The pause itself succeeded; only the reported name was foreign.

**`route mutation rejected` in the log** — the route failed validation and never
entered the table, so every later request answers not-found. The store requires a
resource version that is a canonical positive integer and that advances past the
one already recorded, so a route with none, or one that repeats a version, is
refused:

```bash
kubectl logs -n $NS $P -c controller --since=10m | grep -i "route mutation"
```

`rejected` is invalid; `ignored` with `ReasonStaleResourceVersion` means the version
did not advance — which is what a route-only change such as withdrawing an endpoint
must bump for itself, since Substrate does not version an actor for it.

## list

**`GET /v2/sandboxes` returns `[]` while the sandboxes exist** — three causes, in
the order worth checking:

1. The paginator drops any sandbox whose claim-time annotation is empty, because it
   sorts on that annotation.
2. The default state filter admits only `running` and `paused`, so any other
   reported state is filtered out.
3. More than one replica is running, and the request reached a pod whose in-memory
   store does not hold the sandbox.

## Controller crash-loops

**`failed to wait for sandboxupdateops-controller caches to sync`** — misleading:
controllers start concurrently and the one that failed may not be the one named. A
missing `ate.dev` permission is the usual cause, since the SandboxSet controller
watches WorkerPool. Confirm in the log and by asking directly:

```bash
kubectl auth can-i list workerpools.ate.dev --as=system:sandbox-controller-manager
```

**`unknown flag: --<name>`** — the Deployment passes a flag the image does not
have, or the reverse. Apply the deployment patch before setting a new image, never
after.

**`secrets is forbidden`, silently** — the key-storage manager's cache has no logger
set, so controller-runtime's internal errors are discarded. A permission failure
there shows up only as behaviour, never as a log line. Set a logger on that manager
before trusting its silence.

## Cluster access

**`ali cs`, `kubectl` time out against the API server** — check reachability before
concluding anything about the cluster's contents. `agent-sandbox-controller` runs as
a managed addon in the meta cluster and is invisible to `kubectl get deploy -A`
against the user cluster; read its logs through the meta cluster's SLS project.

**`api.$E2B_DOMAIN` resolves but nothing answers** — the public DNS record points at
the wrong address. Pin the ALB with `curl --resolve`, or hijack
`socket.getaddrinfo` in Python.
