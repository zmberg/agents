/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package substrate

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"k8s.io/klog/v2"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/openkruise/agents/pkg/cache"
	managererrors "github.com/openkruise/agents/pkg/sandbox-manager/errors"
	"github.com/openkruise/agents/pkg/sandbox-manager/infra"
	"github.com/openkruise/agents/pkg/sandboxid"
)

// MetadataKeySandboxSet is the E2B create metadata key that pins a sandbox to a
// specific SandboxSet's worker pool. Absent, the actor is eligible for any pool
// the template allows. E2B create metadata is propagated onto the sandbox as
// annotations, so this is read from the sandbox annotations after the claim
// modifier runs.
const MetadataKeySandboxSet = "e2b.agents.kruise.io/sandboxset"

const (
	// goldenActorAtespace holds the per-template golden actors that produce a
	// template's snapshot. They are not sandboxes and must never be recovered as
	// one.
	goldenActorAtespace = "ate-golden"
	// actorRecoveryPageSize bounds one recovery page. Recovery runs once at
	// startup, so it trades more round trips for a smaller response.
	actorRecoveryPageSize = 200
	// sandboxIDActorPrefixLen is how much of the actor UUID the sandbox ID
	// carries. It keeps the ID short while staying collision-free in practice.
	sandboxIDActorPrefixLen = 8
)

// Infra is the Substrate implementation of infra.Infrastructure.
//
// It owns no Kubernetes informer: actor state lives in the metadata store and
// the template resolver reads ActorTemplates through a client on demand. Routes
// are pushed to the gateway as actors move rather than observed.
type Infra struct {
	control   ateapipb.ControlClient
	store     MetadataStore
	locks     *KeyedLocker
	templates *TemplateResolver
	routes    *routeSource
	// defaultHibernateMode applies when a claim does not resolve a pool-specific
	// mode. Suspend frees the worker, which is the safe default for idle actors.
	defaultHibernateMode string
}

var _ infra.Infrastructure = (*Infra)(nil)

// Run rebuilds the metadata store from the actors Substrate already holds.
//
// The store lives in this process's memory, so a restart would otherwise orphan
// every actor it created: Substrate keeps running them and their workers stay
// occupied, while this process no longer knows they exist and can neither list,
// delete, nor time them out. Recovering them on startup makes those actors
// addressable again so their workers can be reclaimed.
//
// Recovery is best effort. Substrate stores no owner or timeout for an actor, so
// a recovered sandbox has neither: it is visible and deletable, but it is not
// attributed to a user and will not expire on its own. A caller that needs those
// guarantees must not rely on a recovered record.
func (i *Infra) Run(ctx context.Context) error {
	if i.control == nil {
		return errNoControlClient()
	}
	log := klog.FromContext(ctx)

	actors, err := i.listAllActors(ctx)
	if err != nil {
		// A failed recovery leaves orphans behind but must not stop the manager
		// from serving new claims, so report it and carry on.
		log.Error(err, "failed to recover substrate actors; actors created before this restart stay orphaned")
		return nil
	}

	recovered := 0
	for _, actor := range actors {
		meta := metadataFromActor(actor, i.defaultHibernateMode)
		if meta == nil {
			continue
		}
		if _, err := i.store.Get(meta.SandboxID); err == nil {
			// Already known, so a concurrent claim owns the authoritative record.
			continue
		}
		i.store.Put(meta)
		recovered++
		// Publish even for a hibernated actor, which holds no address. The route is
		// what makes the sandbox addressable, so a recovered actor without one
		// could not be inspected, resumed, or deleted.
		i.routes.publish(NewSandbox(meta, i.control, i.store, i.locks))
		log.V(1).Info("recovered substrate actor",
			"sandboxID", meta.SandboxID, "actorID", meta.ActorID,
			"atespace", meta.Atespace, "phase", meta.Phase)
	}
	if recovered > 0 {
		log.Info("recovered substrate actors into the metadata store",
			"count", recovered,
			"note", "recovered sandboxes carry no owner or timeout")
	}
	return nil
}

// listAllActors pages through every actor in every atespace. An empty atespace
// in the request is the cross-atespace form.
func (i *Infra) listAllActors(ctx context.Context) ([]*ateapipb.Actor, error) {
	var (
		actors []*ateapipb.Actor
		token  string
	)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		page, err := i.control.ListActors(ctx, &ateapipb.ListActorsRequest{
			PageSize:  actorRecoveryPageSize,
			PageToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("list substrate actors: %w", err)
		}
		actors = append(actors, page.GetActors()...)
		token = page.GetNextPageToken()
		if token == "" {
			return actors, nil
		}
	}
}

// metadataFromActor projects an actor back onto the record a claim would have
// stored. It returns nil for an actor this backend does not own: a golden actor
// belongs to a template rather than to a sandbox, and an actor without an
// identity cannot be addressed.
func metadataFromActor(actor *ateapipb.Actor, hibernateMode string) *Metadata {
	atespace := actor.GetMetadata().GetAtespace()
	actorID := actor.GetMetadata().GetName()
	if atespace == "" || atespace == goldenActorAtespace || actorID == "" {
		return nil
	}
	phase := phaseOf(actor.GetStatus())
	if phase == "" || phase == PhaseCrashed {
		// A crashed or unknown actor holds no worker worth reclaiming and would
		// only surface as a sandbox that can never serve traffic.
		return nil
	}
	// The sandbox ID is derived, not stored, so it has to be rebuilt exactly the
	// way ClaimSandbox derived it or the recovered sandbox answers to a different
	// name than the one the caller holds.
	sandboxID := deriveSandboxID(atespace, actorID)
	createTime := actor.GetMetadata().GetCreateTime().AsTime()
	return &Metadata{
		SandboxID:         sandboxID,
		ActorID:           actorID,
		Atespace:          atespace,
		Namespace:         atespace,
		ActorTemplateName: actor.GetActorTemplateName(),
		SandboxSetName:    actor.GetStatus().GetWorkerAssignment().GetWorkerPool(),
		Phase:             phase,
		Route:             routeFromActorID(sandboxID, atespace, "", actorID, actor),
		CreateTime:        createTime,
		LastActiveTime:    createTime,
		HibernateMode:     hibernateMode,
	}
}

// deriveSandboxID names the sandbox that fronts an actor. The ID is derived
// rather than stored, so a claim and a recovery must derive it identically or the
// same actor would answer to two different sandbox IDs.
func deriveSandboxID(namespace, actorID string) string {
	if len(actorID) > sandboxIDActorPrefixLen {
		actorID = actorID[:sandboxIDActorPrefixLen]
	}
	return fmt.Sprintf("%s%s%s", namespace, sandboxid.LegacySeparator, actorID)
}

// Stop is a no-op; the gRPC connection is owned by the builder's client.
func (i *Infra) Stop(_ context.Context) {}

// GetCache returns nil: the substrate backend keeps its state in memory rather
// than in an informer-backed cache. Callers must tolerate a nil cache.
func (i *Infra) GetCache() cache.Provider { return nil }

// GetSandboxRouteSource returns the push-driven route source.
func (i *Infra) GetSandboxRouteSource() infra.SandboxRouteSource { return i.routes }

// HasTemplate reports whether a ready ActorTemplate backs the E2B template.
func (i *Infra) HasTemplate(ctx context.Context, opts infra.HasTemplateOptions) bool {
	return i.templates.HasTemplate(ctx, opts.Namespace, opts.Name)
}

// HasCheckpoint always reports false: checkpoints are not modeled yet.
func (i *Infra) HasCheckpoint(context.Context, infra.HasCheckpointOptions) bool { return false }

// LoadDebugInfo surfaces backend counters for the debug endpoint.
func (i *Infra) LoadDebugInfo() map[string]any {
	return map[string]any{
		"backend":       "substrate",
		"sandbox_count": len(i.store.List(ListOptions{})),
		"held_locks":    i.locks.Len(),
	}
}

// SelectSandboxes lists sandboxes owned by a user, optionally scoped to a
// namespace, from the metadata store.
func (i *Infra) SelectSandboxes(_ context.Context, opts infra.SelectSandboxesOptions) ([]infra.Sandbox, error) {
	metas := i.store.List(ListOptions{Owner: opts.User, Namespace: opts.Namespace})
	result := make([]infra.Sandbox, 0, len(metas))
	for _, meta := range metas {
		result = append(result, NewSandbox(meta, i.control, i.store, i.locks))
	}
	return result, nil
}

// GetSandbox looks up one claimed sandbox by ID.
func (i *Infra) GetSandbox(_ context.Context, opts infra.GetSandboxOptions) (infra.Sandbox, error) {
	meta, err := i.store.Get(opts.SandboxID)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", infra.ErrSandboxNotFound, opts.SandboxID)
	}
	return NewSandbox(meta, i.control, i.store, i.locks), nil
}

// SelectSucceededCheckpoints returns nothing: checkpoints are not modeled.
func (i *Infra) SelectSucceededCheckpoints(context.Context, infra.SelectSucceededCheckpointsOptions) ([]infra.CheckpointInfo, error) {
	return nil, nil
}

// ClaimSandbox creates a new actor from the resolved template and resumes it.
//
// Placement follows the two-tier substrate model: the template's workerSelector
// gates which pools are eligible, and the per-actor selector derived from the
// create metadata narrows that set to the SandboxSet the caller chose.
func (i *Infra) ClaimSandbox(ctx context.Context, opts infra.ClaimSandboxOptions) (infra.Sandbox, infra.ClaimMetrics, error) {
	metrics := infra.ClaimMetrics{LockType: infra.LockTypeCreate}
	start := time.Now()
	defer func() { metrics.Total = time.Since(start) }()

	if i.control == nil {
		return nil, metrics, errNoControlClient()
	}

	log := klog.FromContext(ctx)

	resolved, err := i.templates.Resolve(ctx, opts.Namespace, opts.Template)
	if err != nil {
		return nil, metrics, managererrors.NewError(managererrors.ErrorNotFound,
			"resolve template %s/%s: %v", opts.Namespace, opts.Template, err)
	}

	atespace := opts.Namespace
	if err := EnsureAtespace(ctx, i.control, atespace); err != nil {
		return nil, metrics, managererrors.WrapError(managererrors.ErrorInternal, err,
			"ensure atespace %s", atespace)
	}

	actorID := uuid.NewString()
	sandboxID := deriveSandboxID(opts.Namespace, actorID)

	unlock := i.locks.Lock(actorID)
	defer unlock()

	now := time.Now()
	meta := &Metadata{
		SandboxID:         sandboxID,
		ActorID:           actorID,
		Atespace:          atespace,
		Namespace:         opts.Namespace,
		ActorTemplateName: resolved.ActorTemplateName,
		Owner:             opts.User,
		Phase:             PhaseResuming,
		CreateTime:        now,
		LastActiveTime:    now,
		HibernateMode:     i.defaultHibernateMode,
	}
	sbx := NewSandbox(meta, i.control, i.store, i.locks)

	// Run the modifier before touching substrate so E2B create metadata (carried
	// on the sandbox as annotations) and the caller timeout are available. The
	// modifier only mutates the sandbox object; nothing is persisted yet.
	if opts.Modifier != nil {
		if err := opts.Modifier(sbx); err != nil {
			return nil, metrics, fmt.Errorf("apply claim modifier: %w", err)
		}
	}
	sandboxSet := sbx.GetAnnotations()[MetadataKeySandboxSet]
	meta.SandboxSetName = sandboxSet
	meta.Timeout = sbx.GetTimeout()

	log.Info("creating substrate actor",
		"sandboxID", sandboxID, "actorID", actorID,
		"actorTemplate", resolved.ActorTemplateName, "sandboxSet", sandboxSet)

	created, err := i.control.CreateActor(ctx, &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:               &ateapipb.ResourceMetadata{Atespace: atespace, Name: actorID},
			ActorTemplateNamespace: resolved.Namespace,
			ActorTemplateName:      resolved.ActorTemplateName,
			WorkerSelector:         selectorProto(PoolSelector(sandboxSet)),
		},
	})
	if err != nil {
		return nil, metrics, managererrors.WrapError(managererrors.ErrorInternal, err,
			"create actor for template %s", resolved.ActorTemplateName)
	}

	resumed, err := i.control.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: actorRef(atespace, actorID),
		Boot:  false,
	})
	if err != nil {
		// The actor exists but never started; suspend and delete it so a failed
		// claim does not leak a half-created actor holding a worker.
		i.cleanupFailedActor(ctx, atespace, actorID)
		return nil, metrics, managererrors.WrapError(managererrors.ErrorInternal, err,
			"resume newly created actor %s", actorID)
	}

	actor := resumed.GetActor()
	if actor == nil {
		actor = created
	}
	meta.Phase = phaseOf(actor.GetStatus())
	meta.Route = routeFromActorID(sandboxID, opts.Namespace, opts.User, actorID, actor)
	i.store.Put(meta)

	// Publish the route unconditionally. It is not only the gateway's forwarding
	// entry but the record that makes the sandbox addressable at all, which the
	// API layer reads to decide whether a sandbox exists and who owns it.
	// Withholding it for an actor that has no address yet would leave the sandbox
	// unreachable by any request, including the ones that would release it.
	i.routes.publish(sbx)

	return sbx, metrics, nil
}

// CloneSandbox is unsupported: cloning copies a running sandbox's disk, which
// the substrate snapshot model does not expose through this backend yet.
func (i *Infra) CloneSandbox(context.Context, infra.CloneSandboxOptions) (infra.Sandbox, infra.CloneMetrics, error) {
	return nil, infra.CloneMetrics{}, unsupported("clone")
}

// DeleteCheckpoint is unsupported: checkpoints are not modeled.
func (i *Infra) DeleteCheckpoint(context.Context, infra.DeleteCheckpointOptions) error {
	return unsupported("checkpoints")
}

// Volume operations are unsupported in this backend version.
func (i *Infra) CreateVolume(context.Context, infra.CreateVolumeOptions) (*infra.VolumeInfo, error) {
	return nil, unsupported("volumes")
}

func (i *Infra) ListVolumes(context.Context, infra.ListVolumesOptions) ([]*infra.VolumeInfo, error) {
	return nil, unsupported("volumes")
}

func (i *Infra) GetVolume(context.Context, infra.GetVolumeOptions) (*infra.VolumeInfo, error) {
	return nil, unsupported("volumes")
}

func (i *Infra) DeleteVolume(context.Context, infra.DeleteVolumeOptions) error {
	return unsupported("volumes")
}

// cleanupFailedActor best-effort removes an actor that was created but failed to
// resume, so a failed claim does not strand a worker.
func (i *Infra) cleanupFailedActor(ctx context.Context, atespace, actorID string) {
	log := klog.FromContext(ctx).WithValues("actorID", actorID)
	ref := actorRef(atespace, actorID)
	if _, err := i.control.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref}); err != nil && !isNotFound(err) {
		log.V(4).Info("suspend during failed-claim cleanup did not succeed", "err", err)
	}
	if _, err := i.control.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref}); err != nil && !isNotFound(err) {
		log.Error(err, "failed to delete actor during failed-claim cleanup; it may leak a worker")
	}
}

// selectorProto converts a label map to the substrate Selector, returning nil
// for an empty map so substrate treats every pool as eligible.
func selectorProto(labels map[string]string) *ateapipb.Selector {
	if len(labels) == 0 {
		return nil
	}
	return &ateapipb.Selector{MatchLabels: labels}
}
