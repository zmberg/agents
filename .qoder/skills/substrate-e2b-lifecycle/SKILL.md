---
name: substrate-e2b-lifecycle
description: Deploy sandbox-manager against a Substrate microsandbox backend and verify the full E2B lifecycle end to end.
disable-model-invocation: true
---

# Substrate + E2B lifecycle

Deploy `sandbox-manager` with the Substrate backend, then drive one sandbox through
buildTemplate → create → pause → resume → kill over the E2B API.

Two independent parts. Run [Deploy](#deploy) when the config is not in place or an
image changed; run [Verify](#verify) to prove the lifecycle works. Verify is safe
to run alone against a cluster already deployed.

`DEPLOY-REFERENCE.md` holds the manifests and the one-time cluster setup that
Deploy applies. `TROUBLESHOOTING.md` maps a failing symptom to its cause — reach
for it the moment a step reports something other than its expected result, because
every entry there is a trap that already cost a debugging round.

## Environment

Set these once per session. They are placeholders; fill each from your own
environment. `CLUSTER_ID` and the kubeconfig come from your cluster access, the E2B
domain and its ALB hostname from whoever operates the endpoint, and `ATESPACE` is
the team you hold a key for.

```bash
export KUBECONFIG=<path-to-kubeconfig>
export CLUSTER_ID=<cluster-id>                             # ACK, cn-hongkong
export NS=sandbox-system                                  # sandbox-manager
export ATESPACE=<team>                                     # = E2B team name = k8s namespace
export E2B_DOMAIN=<e2b-domain>
export ALB_HOST=<alb-hostname>                             # the ALB fronting the E2B API
export CA=<repo>/bin/e2b-doc-example/certs/ca-fullchain.pem
```

Public DNS for `api.$E2B_DOMAIN` resolves to the wrong address. Reach the API by
pinning the ALB, which every command below does:

```bash
export ALB_IP=$(dig +short $ALB_HOST | head -1)
R() { curl -sS -m 120 --cacert "$CA" \
        --resolve "api.$E2B_DOMAIN:443:$ALB_IP" \
        -H "X-API-KEY: $KEY" -H "Content-Type: application/json" "$@"; }
```

A team key is required; the admin key resolves to no namespace and Substrate then
rejects the build. Read it from the cluster and keep it out of files you commit:

```bash
export KEY=$(kubectl get secret e2b-key-store -n $NS -o json | python3 -c "
import json,sys,base64
for v in json.load(sys.stdin).get('data',{}).values():
    j=json.loads(base64.b64decode(v))
    if (j.get('team') or {}).get('name')=='$ATESPACE': print(j['key']); break
")
```

## Deploy

Apply in this order. Each step states what proves it landed.

1. **Confirm the one-time cluster setup exists.** `DEPLOY-REFERENCE.md` lists what
   must already be present: the ActorTemplate CRD's `microsandbox` enum value, the
   RBAC rules, the `substrate-ca` secret, the SandboxConfig, and the hand-built
   worker pods. Verify each; a missing one fails a later step in a way that reads
   like a code bug.

2. **Apply the deployment patch, then set the image.** The patch carries the
   `--substrate-*` arguments and the projected-token volume:

   ```bash
   kubectl patch deploy sandbox-manager -n $NS \
     --type=strategic --patch-file=DEPLOY-REFERENCE-patch.yaml
   kubectl set image deploy/sandbox-manager -n $NS controller=<image>
   kubectl rollout status deploy/sandbox-manager -n $NS --timeout=300s
   ```

   Patch first. A strategic merge replaces `args` wholesale, so setting the image
   first leaves the new binary running with the old argument list until the patch
   lands, and a removed flag crashes it.

3. **Keep it at one replica.** The metadata store is process memory, so replicas
   do not see each other's sandboxes and a round-robin request fails at random:

   ```bash
   kubectl scale deploy sandbox-manager -n $NS --replicas=1
   ```

4. **Read the startup log.** It reports the recovery pass over the actors Substrate
   already holds:

   ```bash
   P=$(kubectl get pods -n $NS -o name | grep sandbox-manager | head -1)
   kubectl logs -n $NS $P -c controller --since=5m | grep -iE "recovered substrate actors|route mutation"
   ```

   Expect `recovered substrate actors into the metadata store` and, for each actor,
   `route mutation completed`. `route mutation rejected` means the route was
   refused and every sandbox is about to answer not-found.

## Verify

Run the steps in order against one fresh template. Reuse of an existing template
hides a broken buildTemplate, which is how a working-looking lifecycle once ran on
a template built weeks earlier.

Every actor state below is read from Substrate directly, which is the authority:

```bash
kubectl-ate get actors -A          # built from the fork_github branch
kubectl-ate get workers
```

### 1. buildTemplate

Three calls: reserve the IDs, start the build, poll until ready.

`fromImage` must be pinned by digest (`repo@sha256:...`); a tag is rejected with
400. Read a digest from a template that already exists:

```bash
IMAGE=$(kubectl get actortemplates -n $ATESPACE -o json \
  | python3 -c "import json,sys;print(json.load(sys.stdin)['items'][0]['spec']['containers'][0]['image'])")
```

```bash
IDS=$(R -X POST -d '{"alias":"lifecycle-check","cpuCount":1,"memoryMB":512}' \
        https://api.$E2B_DOMAIN/v3/templates)
TID=$(echo "$IDS" | python3 -c "import json,sys;print(json.load(sys.stdin)['templateID'])")
BID=$(echo "$IDS" | python3 -c "import json,sys;print(json.load(sys.stdin)['buildID'])")

R -X POST -d "{\"fromImage\":\"$IMAGE\",\"startCmd\":\"\",\"readyCmd\":\"\"}" \
  -H "x-e2b-kruise-template-worker-selector: workload=microsandbox" \
  -H "x-e2b-kruise-template-container-name: app" \
  -H "x-e2b-kruise-template-snapshots-location: local://msb-snapshots" \
  https://api.$E2B_DOMAIN/v2/templates/$TID/builds/$BID

R https://api.$E2B_DOMAIN/templates/$TID/builds/$BID/status
```

The three `x-e2b-kruise-` headers carry what the E2B protocol cannot express —
placement, container name, and snapshot location. They are optional, and omitting
the worker selector lets the template land on any pool the class allows.

Expected: `202` from the first two calls, then `status` walking
`building` → `ready` in roughly two minutes. Confirm the artefact and its class:

```bash
kubectl get actortemplates -n $ATESPACE
```

### 2. create

```bash
CR=$(R -X POST -d "{\"templateID\":\"$TID\",\"timeout\":1800}" \
       https://api.$E2B_DOMAIN/sandboxes)
SID=$(echo "$CR" | python3 -c "import json,sys;print(json.load(sys.stdin)['sandboxID'])")
```

Expected: `201` with `state` reading `running`, within a couple of seconds. An
empty `state` means the client and the deployed control plane disagree about the
actor status field.

### 3. pause

```bash
R -X POST https://api.$E2B_DOMAIN/sandboxes/$SID/pause
R https://api.$E2B_DOMAIN/sandboxes/$SID
```

Expected: `204` in about 15s, then `state` reading `paused`. The default hibernate
mode suspends the actor, so its worker returns to the pool — confirm with
`kubectl-ate get workers` that the assigned worker now reads `FREE`, and that the
actor reads `ACTOR_STATE_SUSPENDED`. A paused sandbox stays addressable: the `GET`
must answer `200`, not `404`.

### 4. resume

```bash
R -X POST -d '{"timeout":1800}' https://api.$E2B_DOMAIN/sandboxes/$SID/resume
R https://api.$E2B_DOMAIN/sandboxes/$SID
```

Expected: `204` in a few seconds, `state` back to `running`, and a worker assigned
again — commonly a different one, since the actor restores from its snapshot.

### 5. kill

```bash
R -X DELETE https://api.$E2B_DOMAIN/sandboxes/$SID
```

Expected: `204`, a subsequent `GET` answering `404`, the actor gone from
`kubectl-ate get actors`, and its worker back to `FREE`.

### 6. Account for every worker

```bash
kubectl-ate get workers
kubectl-ate get actors -A
```

The run is complete only when no actor from it survives and every worker it touched
reads `FREE`. A leaked worker stays occupied indefinitely, so treat a stray
`ASSIGNED` as a failure of this skill rather than leftover noise.

One actor does survive by design: each build leaves a golden actor in the
`ate-golden` atespace, suspended and holding no worker. They accumulate across runs
and a failed build leaves a `CRASHED` one behind. Delete a template's golden actor
along with the template when clearing up:

```bash
kubectl-ate get actors -A | grep ate-golden
kubectl delete actortemplate <name> -n $ATESPACE
```

## Optional: restart recovery

The metadata store is process memory, so a restart would orphan every actor. To
prove recovery works, create a sandbox, restart the deployment, and confirm the
sandbox is still listable, fetchable, and killable:

```bash
kubectl rollout restart deploy/sandbox-manager -n $NS
kubectl rollout status deploy/sandbox-manager -n $NS --timeout=300s
R https://api.$E2B_DOMAIN/v2/sandboxes
R -X DELETE https://api.$E2B_DOMAIN/sandboxes/$SID
```

A recovered sandbox carries no owner and no timeout, because Substrate stores
neither. It is reachable by any key of its own team and it never expires on its
own.
