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
	"io"
	"net/http"
	"strconv"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
	"github.com/openkruise/agents/pkg/sandbox-manager/infra"
	"github.com/openkruise/agents/pkg/sandboxroute"
	"github.com/openkruise/agents/pkg/utils/timeout"
)

// firstRouteResourceVersion is the smallest resource version the route store
// accepts: it validates canonical positive integers, so zero and any form with a
// leading zero are rejected.
const firstRouteResourceVersion int64 = 1

// Sandbox is one Substrate actor presented as an infra.Sandbox.
//
// It carries no Kubernetes object: the embedded ObjectMeta only projects the
// metadata record so that callers relying on metav1.Object keep working. Every
// lifecycle call goes straight to the Substrate control API and updates the
// shared metadata store.
type Sandbox struct {
	metav1.ObjectMeta

	meta    *Metadata
	control ateapipb.ControlClient
	store   MetadataStore
	locks   *KeyedLocker

	// image and podLabels are per-operation values a caller may set before a
	// claim persists. Substrate takes neither, so they are only echoed back.
	image      string
	podLabels  map[string]string
	podAnnos   map[string]string
	timeoutOpt timeout.Options
}

var _ infra.Sandbox = (*Sandbox)(nil)

// NewSandbox wraps a metadata record.
func NewSandbox(meta *Metadata, control ateapipb.ControlClient, store MetadataStore, locks *KeyedLocker) *Sandbox {
	return &Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:              meta.SandboxID,
			Namespace:         meta.Namespace,
			UID:               types.UID(meta.ActorID),
			CreationTimestamp: metav1.NewTime(meta.CreateTime),
			Labels: map[string]string{
				agentsv1alpha1.LabelSandboxID: meta.SandboxID,
			},
			Annotations: map[string]string{
				agentsv1alpha1.AnnotationOwner: meta.Owner,
			},
		},
		meta:       meta,
		control:    control,
		store:      store,
		locks:      locks,
		podLabels:  map[string]string{},
		podAnnos:   map[string]string{},
		timeoutOpt: meta.Timeout,
	}
}

// ref returns the actor reference for control-plane calls.
func (s *Sandbox) ref() *ateapipb.ObjectRef {
	return actorRef(s.meta.Atespace, s.meta.ActorID)
}

// lock serializes lifecycle calls on this actor.
func (s *Sandbox) lock() func() {
	if s.locks == nil {
		return func() {}
	}
	return s.locks.Lock(s.meta.ActorID)
}

// Pause hibernates the actor. The mode chosen by the owning SandboxSet decides
// whether the worker stays assigned: pause snapshots locally and keeps it,
// suspend snapshots externally and frees it for another actor.
func (s *Sandbox) Pause(ctx context.Context, opts infra.PauseOptions) error {
	if s.control == nil {
		return errNoControlClient()
	}
	unlock := s.lock()
	defer unlock()

	log := klog.FromContext(ctx).WithValues("sandboxID", s.meta.SandboxID, "actorID", s.meta.ActorID)
	if opts.Timeout != nil {
		s.timeoutOpt = *opts.Timeout
	}

	var err error
	switch s.meta.HibernateMode {
	case agentsv1alpha1.HibernateModePause:
		log.V(4).Info("pausing actor, keeping its worker")
		_, err = s.control.PauseActor(ctx, &ateapipb.PauseActorRequest{Actor: s.ref()})
	default:
		log.V(4).Info("suspending actor, freeing its worker")
		_, err = s.control.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: s.ref()})
	}
	if err != nil {
		// Already hibernated is the state the caller asked for.
		if isAlreadyHibernated(err) {
			s.setPhase(hibernatedPhase(s.meta.HibernateMode))
			return nil
		}
		return fmt.Errorf("hibernate actor %s: %w", s.meta.ActorID, err)
	}

	s.setPhase(hibernatedPhase(s.meta.HibernateMode))
	// A hibernated actor has no reachable address, so drop the route's endpoint
	// to stop the gateway from sending traffic to a freed worker.
	s.clearRouteEndpoint()
	return nil
}

// Resume restores the actor from its latest snapshot.
func (s *Sandbox) Resume(ctx context.Context, opts infra.ResumeOptions) error {
	if s.control == nil {
		return errNoControlClient()
	}
	unlock := s.lock()
	defer unlock()

	if opts.Timeout != nil {
		s.timeoutOpt = *opts.Timeout
	}

	// boot=false restores from the golden or latest snapshot instead of starting
	// the workload cold, which is the whole point of the substrate backend.
	resp, err := s.control.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: s.ref(), Boot: false})
	if err != nil {
		return fmt.Errorf("resume actor %s: %w", s.meta.ActorID, err)
	}
	s.applyActor(resp.GetActor())
	return nil
}

// Kill suspends the actor if it still holds a worker and then deletes it.
// Substrate only deletes suspended actors, so the suspend is not optional.
func (s *Sandbox) Kill(ctx context.Context) error {
	if s.control == nil {
		return errNoControlClient()
	}
	unlock := s.lock()
	defer unlock()

	log := klog.FromContext(ctx).WithValues("sandboxID", s.meta.SandboxID, "actorID", s.meta.ActorID)

	if holdsWorker(s.meta.Phase) {
		if _, err := s.control.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: s.ref()}); err != nil {
			if isNotFound(err) {
				s.forget()
				return nil
			}
			// Fall through to delete: substrate may already consider the actor
			// suspended, and reporting a failure here would strand the record.
			log.V(4).Info("suspend before delete failed, attempting delete anyway", "err", err)
		}
	}

	if _, err := s.control.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: s.ref()}); err != nil {
		if isNotFound(err) {
			s.forget()
			return nil
		}
		return fmt.Errorf("delete actor %s: %w", s.meta.ActorID, err)
	}

	s.forget()
	return nil
}

// InplaceRefresh re-reads the actor so phase and route reflect substrate.
func (s *Sandbox) InplaceRefresh(ctx context.Context, _ bool) error {
	if s.control == nil {
		return errNoControlClient()
	}
	actor, err := s.control.GetActor(ctx, &ateapipb.GetActorRequest{Actor: s.ref()})
	if err != nil {
		if isNotFound(err) {
			s.forget()
			return fmt.Errorf("%w: actor %s", infra.ErrSandboxNotFound, s.meta.ActorID)
		}
		return fmt.Errorf("get actor %s: %w", s.meta.ActorID, err)
	}
	s.applyActor(actor)
	return nil
}

// SaveTimeoutWithPolicy records the sandbox lifetime in the metadata store.
// Substrate has no timeout concept, so sandbox-manager owns the deadline.
func (s *Sandbox) SaveTimeoutWithPolicy(
	_ context.Context,
	opts infra.SaveTimeoutOptions,
	policy timeout.UpdatePolicy,
) (infra.TimeoutUpdateResult, error) {
	next := opts.Timeout
	if policy == timeout.UpdatePolicyExtendOnly && !extendsTimeout(s.timeoutOpt, next) {
		return infra.TimeoutUpdateResult{Updated: false}, nil
	}

	s.timeoutOpt = next
	now := time.Now()
	if s.store != nil {
		s.store.Update(s.meta.SandboxID, func(m *Metadata) {
			m.Timeout = next
			m.LastActiveTime = now
		})
	}
	s.meta.Timeout = next
	s.meta.LastActiveTime = now
	return infra.TimeoutUpdateResult{Updated: true}, nil
}

// --- projections of the metadata record ---

func (s *Sandbox) GetSandboxID() string { return s.meta.SandboxID }
func (s *Sandbox) GetIP() string        { return s.meta.Route.IP }
func (s *Sandbox) Phase() string        { return s.meta.Phase }
func (s *Sandbox) GetTemplate() string  { return s.meta.ActorTemplateName }

func (s *Sandbox) GetState() (string, string) { return s.meta.Phase, "" }

func (s *Sandbox) GetRoute() (sandboxroute.Route, error) { return s.meta.Route, nil }

func (s *Sandbox) GetClaimTime() (time.Time, error) { return s.meta.CreateTime, nil }

func (s *Sandbox) GetTimeout() timeout.Options     { return s.timeoutOpt }
func (s *Sandbox) SetTimeout(o timeout.Options)    { s.timeoutOpt = o }
func (s *Sandbox) SetImage(image string)           { s.image = image }
func (s *Sandbox) GetImage() string                { return s.image }
func (s *Sandbox) GetPodLabels() map[string]string { return s.podLabels }
func (s *Sandbox) SetPodLabels(l map[string]string) {
	s.podLabels = l
}
func (s *Sandbox) GetPodAnnotations() map[string]string { return s.podAnnos }
func (s *Sandbox) SetPodAnnotations(a map[string]string) {
	s.podAnnos = a
}

// GetResource reports no resources: an actor's shape comes from the WorkerPool
// it lands on, not from the actor itself.
func (s *Sandbox) GetResource() infra.SandboxResource { return infra.SandboxResource{} }

// The substrate backend does not issue gateway access tokens yet.
func (s *Sandbox) GetTrafficAccessToken() string           { return "" }
func (s *Sandbox) GetTrafficAccessTokenExpiration() string { return "" }

// --- capabilities the substrate backend does not provide ---

// IsRecycleEnabled reports false: recycling returns a sandbox to a warm pool,
// which has no meaning for an actor created on demand.
func (s *Sandbox) IsRecycleEnabled() bool { return false }

func (s *Sandbox) TriggerRecycle(context.Context) error {
	return unsupported("recycle")
}

func (s *Sandbox) Request(context.Context, string, string, int, io.Reader) (*http.Response, error) {
	return nil, unsupported("direct sandbox requests")
}

func (s *Sandbox) CSIMount(context.Context, string, string) error {
	return unsupported("CSI mounts")
}

// CreateCheckpoint is unsupported: substrate snapshots an actor through
// pause/suspend, and mapping those onto E2B checkpoints is not designed yet.
func (s *Sandbox) CreateCheckpoint(context.Context, infra.CreateCheckpointOptions) (string, error) {
	return "", unsupported("checkpoints")
}

func (s *Sandbox) CreateNetworkPolicy(context.Context, infra.SandboxNetworkConfig) error {
	return unsupported("network policies")
}

func (s *Sandbox) UpdateNetworkPolicy(context.Context, infra.SandboxNetworkConfig) error {
	return unsupported("network policies")
}

func (s *Sandbox) SelectNetworkPolicy(context.Context) (*infra.SandboxNetworkConfig, error) {
	return nil, unsupported("network policies")
}

// --- internal state transitions ---

// applyActor copies observable actor state into the record and the store.
func (s *Sandbox) applyActor(actor *ateapipb.Actor) {
	if actor == nil {
		return
	}
	phase := phaseOf(actor.GetStatus())
	route := routeFromActor(s.meta, actor)
	pool := actor.GetStatus().GetWorkerAssignment().GetWorkerPool()

	s.meta.Phase = phase
	s.meta.Route = route
	if pool != "" {
		s.meta.SandboxSetName = pool
	}

	if s.store != nil {
		s.store.Update(s.meta.SandboxID, func(m *Metadata) {
			m.Phase = phase
			m.Route = route
			if pool != "" {
				m.SandboxSetName = pool
			}
		})
	}
}

func (s *Sandbox) setPhase(phase string) {
	s.meta.Phase = phase
	if s.store != nil {
		s.store.Update(s.meta.SandboxID, func(m *Metadata) { m.Phase = phase })
	}
}

// clearRouteEndpoint drops the address of a hibernated actor while keeping its
// identity, so the gateway stops routing but the sandbox stays addressable by ID.
//
// The resource version is advanced because the route store ignores an upsert that
// does not move past the version it already holds. Without that the withdrawal
// would be dropped and the gateway would keep sending traffic to a freed worker.
func (s *Sandbox) clearRouteEndpoint() {
	nextVersion := nextRouteResourceVersion(s.meta.Route.ResourceVersion)
	s.meta.Route.IP = ""
	s.meta.Route.State = s.meta.Phase
	s.meta.Route.ResourceVersion = nextVersion
	if s.store != nil {
		s.store.Update(s.meta.SandboxID, func(m *Metadata) {
			m.Route.IP = ""
			m.Route.State = m.Phase
			m.Route.ResourceVersion = nextVersion
		})
	}
}

// nextRouteResourceVersion returns the version that supersedes current.
//
// Substrate does not bump the actor's version for a route-only change, so a
// locally driven withdrawal has to advance the fence itself. An unparsable or
// absent current version restarts at the first valid one rather than failing,
// which keeps a malformed record from permanently pinning the route.
func nextRouteResourceVersion(current string) string {
	version, err := strconv.ParseInt(current, 10, 64)
	if err != nil || version < firstRouteResourceVersion {
		return strconv.FormatInt(firstRouteResourceVersion, 10)
	}
	return strconv.FormatInt(version+1, 10)
}

// forget removes every trace of a deleted actor.
func (s *Sandbox) forget() {
	if s.store != nil {
		s.store.Delete(s.meta.SandboxID)
	}
}

// routeFromActor projects an actor onto its gateway route.
func routeFromActor(meta *Metadata, actor *ateapipb.Actor) sandboxroute.Route {
	return routeFromActorID(meta.SandboxID, meta.Namespace, meta.Owner, meta.ActorID, actor)
}

// routeFromActorID projects an actor onto a route from raw identity fields, for
// callers that have not yet assembled a Metadata record.
func routeFromActorID(sandboxID, namespace, owner, actorID string, actor *ateapipb.Actor) sandboxroute.Route {
	route := sandboxroute.Route{
		ID:              sandboxID,
		Namespace:       namespace,
		Name:            sandboxID,
		UID:             types.UID(actorID),
		Owner:           owner,
		ResourceVersion: routeResourceVersion(actor),
	}
	if actor != nil {
		route.IP = actor.GetStatus().GetWorkerAssignment().GetWorkerPodIp()
		route.State = phaseOf(actor.GetStatus())
	}
	return route
}

// routeResourceVersion projects an actor's version onto the resource version a
// route is fenced by.
//
// The route store rejects a route whose resource version is absent or not a
// canonical positive integer, and it ignores one that does not advance past the
// version already recorded for the same object. An actor's metadata version is
// monotonic per actor, which is exactly that contract, so it is reused directly.
//
// A version below the first valid value is raised to it rather than left empty:
// substrate reports zero for an actor it has not versioned yet, and an empty
// resource version would make the route invalid and drop the sandbox from the
// gateway entirely.
func routeResourceVersion(actor *ateapipb.Actor) string {
	version := actor.GetMetadata().GetVersion()
	if version < firstRouteResourceVersion {
		version = firstRouteResourceVersion
	}
	return strconv.FormatInt(version, 10)
}

// phaseOf collapses an actor status onto the phase reported to E2B. Transient
// states report their destination so a caller polling for "paused" is not
// confused by an intermediate "pausing".
func phaseOf(status *ateapipb.ActorStatus) string {
	switch status.GetState() {
	case ateapipb.ActorState_ACTOR_STATE_RUNNING:
		return PhaseRunning
	case ateapipb.ActorState_ACTOR_STATE_RESUMING:
		return PhaseResuming
	case ateapipb.ActorState_ACTOR_STATE_PAUSED, ateapipb.ActorState_ACTOR_STATE_PAUSING:
		return PhasePaused
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDED, ateapipb.ActorState_ACTOR_STATE_SUSPENDING:
		return PhaseSuspended
	case ateapipb.ActorState_ACTOR_STATE_CRASHED:
		return PhaseCrashed
	case ateapipb.ActorState_ACTOR_STATE_DELETING:
		// A deleting actor no longer serves traffic and never returns to a
		// serving state, so it reads as crashed rather than as a live phase that
		// would keep the gateway routing to it.
		return PhaseCrashed
	default:
		return ""
	}
}

// holdsWorker reports whether an actor in this phase still occupies a worker and
// therefore must be suspended before deletion.
func holdsWorker(phase string) bool {
	switch phase {
	case PhaseRunning, PhaseResuming, PhasePaused:
		return true
	default:
		return false
	}
}

// hibernatedPhase maps a hibernate mode onto the phase it produces.
func hibernatedPhase(mode string) string {
	if mode == agentsv1alpha1.HibernateModePause {
		return PhasePaused
	}
	return PhaseSuspended
}

// extendsTimeout reports whether next pushes either deadline further out. Under
// ExtendOnly a caller must never be able to shorten a lifetime another caller
// already extended.
func extendsTimeout(current, next timeout.Options) bool {
	if !next.ShutdownTime.IsZero() && next.ShutdownTime.After(current.ShutdownTime) {
		return true
	}
	if !next.PauseTime.IsZero() && next.PauseTime.After(current.PauseTime) {
		return true
	}
	return false
}
