# Deploy reference

The manifests and the one-time cluster setup that `SKILL.md` step 1 and 2 apply.
Every value here was read from a working cluster; treat a mismatch as this file
being stale, not the cluster being wrong.

## Deployment patch

`sandbox-manager` selects the Substrate backend by `--substrate-addr` being set.
The other flags configure it:

```yaml
spec:
  template:
    spec:
      containers:
        - name: controller
          args:
            # Selecting a backend. Present means Substrate, absent means the CR backend.
            - --substrate-addr=api.ate-system.svc:443
            # The CA that signs the control-plane serving certificate.
            - --substrate-ca-file=/etc/substrate/ca.crt
            # A ServiceAccount token, re-read per call, presented as gRPC call credentials.
            - --substrate-token-file=/var/run/secrets/substrate/token
            # Where golden snapshots live. Must match the SandboxConfig.
            - --substrate-snapshots-location=local://msb-snapshots
            # The sandbox class every template is built for.
            - --substrate-sandbox-class=microsandbox
            # suspend frees the worker on pause; pause keeps it.
            - --substrate-hibernate-mode=suspend
          volumeMounts:
            - name: substrate-ca
              mountPath: /etc/substrate
              readOnly: true
            - name: substrate-token
              mountPath: /var/run/secrets/substrate
              readOnly: true
      volumes:
        - name: substrate-ca
          secret:
            secretName: substrate-ca
            items:
              - key: ca.crt
                path: ca.crt
        - name: substrate-token
          projected:
            sources:
              - serviceAccountToken:
                  path: token
                  # The control plane accepts only this audience.
                  audience: api.ate-system.svc
                  expirationSeconds: 3600
```

The audience is not free-form: the control plane's `ate-api-authentication`
ConfigMap in `ate-system` pins issuer `https://kubernetes.default.svc` and audience
`api.ate-system.svc`. A token minted for anything else is rejected before any RPC
is recorded, so the failure looks like the request never arrived.

The E2B API flags are independent of the backend and already present on a deployed
manager: `--e2b-enable-auth`, `--e2b-admin-key`, `--e2b-key-storage=secret`,
`--e2b-max-timeout`.

## One-time cluster setup

### ActorTemplate CRD: the microsandbox enum value

The CRD in this cluster is customised — upstream Substrate allows only `gvisor` and
`microvm`:

```bash
kubectl get crd actortemplates.ate.dev -o json | python3 -c "
import json,sys
d=json.load(sys.stdin)
print(d['spec']['versions'][0]['schema']['openAPIV3Schema']['properties']['spec']['properties']['sandboxClass']['enum'])
"
# expect ['gvisor', 'microvm', 'microsandbox']
```

Restore it when absent:

```bash
kubectl patch crd actortemplates.ate.dev --type=json -p \
  '[{"op":"add","path":"/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/sandboxClass/enum/-","value":"microsandbox"}]'
```

Re-applying an upstream Substrate CRD manifest drops this value again, and every
new build then fails with `Unsupported value: "microsandbox"` while existing
templates keep working — so the lifecycle appears healthy until someone builds.
Check this value after any Substrate CRD update.

### RBAC

Both ClusterRoles need rules that the repo's generated manifests do not carry:

```bash
kubectl patch clusterrole ack-sandbox-manager-manager --type=json -p '[
  {"op":"add","path":"/rules/-","value":{"apiGroups":["ate.dev"],"resources":["actortemplates"],
   "verbs":["get","list","watch","create","update","patch","delete"]}}]'

kubectl patch clusterrole sandbox-controller-role --type=json -p '[
  {"op":"add","path":"/rules/-","value":{"apiGroups":["ate.dev"],"resources":["workerpools"],
   "verbs":["get","list","watch","create","update","patch","delete"]}}]'
```

`agent-sandbox-controller` watches WorkerPool, so without the second rule its
caches never sync and it crash-loops on leader-election timeout — an error that
names an unrelated controller. Confirm a permission directly rather than inferring
it from behaviour:

```bash
kubectl auth can-i list workerpools.ate.dev --as=system:sandbox-controller-manager
```

`ack-sandbox-manager-manager` additionally holds cluster-wide `secrets` read
access. That is wider than it should be: the key-storage manager builds an
unscoped Secret informer. Narrowing it means scoping that cache to the system
namespace in code.

### substrate-ca secret

The CA that signs the control-plane certificate, copied out of `ate-system`:

```bash
kubectl get secret ate-serving-tls -n ate-system -o jsonpath='{.data.ca\.crt}' \
  | base64 -d > /tmp/substrate-ca.crt
kubectl create secret generic substrate-ca -n sandbox-system \
  --from-file=ca.crt=/tmp/substrate-ca.crt --dry-run=client -o yaml | kubectl apply -f -
```

### SandboxConfig and worker pods

```bash
kubectl get sandboxconfigs -A      # expect msb-default, class microsandbox, default=true
kubectl-ate get workers            # expect the hand-built msb-worker-db-* pods
```

Worker pods are created by hand and must stay that way. Scaling a WorkerPool above
zero does not provision usable microsandbox workers: the ate-controller renders
worker pods with atunnel arguments unconditionally, without branching on the
sandbox class, and `ateom-microsandbox` rejects them
(`flag provided but not defined: -atunnel-listen-address`). Substrate's own
deploy document records this limitation.

A crash-looping worker is worse than none: Substrate still counts it as schedulable
and a golden actor lands on it and waits forever. Scaling the pool back to zero
leaves that actor `CRASHED` with no reschedule, and the template must be rebuilt.

### kubectl-ate

Build it from the internal fork; upstream `main` reads a ClusterTrustBundle that
ACK does not provide, and the fork falls back to the `ate-serving-tls` secret:

```
http://gitlab.alibaba-inc.com/cos/substrate.git   branch fork_github
```

## Team API key

The E2B team name is the Kubernetes namespace, which is also the Substrate
atespace. Creating a key for a team therefore requires that namespace to exist.

```bash
curl ... -H "X-API-KEY: <admin-key>" -X POST \
  -d '{"teamName":"ate-demo-msb","name":"msb-demo-key"}' \
  https://api.$E2B_DOMAIN/api-keys
```

Keys are stored in the `e2b-key-store` Secret in `sandbox-system`. Read one from
there rather than recording it in a file.

## Known constraints

- **One replica.** `MetadataStore` has only an in-memory implementation, so each
  replica holds a private map and a load-balanced request lands on a pod that may
  not know the sandbox. Multi-replica needs a persistent store; Redis is already
  wired into the deployment for quota.
- **Restart orphans nothing, but recovery is lossy.** Startup rebuilds the store
  from `ListActors`, which restores phase, route, template, and pool. Substrate
  stores no owner and no timeout, so a recovered sandbox has neither: it is
  reachable by any key of its own team and never expires on its own.
- **Volumes, clones, and checkpoints are unsupported** in this backend.
