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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
	"github.com/openkruise/agents/pkg/sandboxroute"
)

// A route the store rejects never reaches the gateway, and the sandbox then
// answers no request and cannot be deleted through the API. Every route this
// backend projects therefore has to be acceptable to the store.
func TestRouteFromActorIDIsAcceptedByTheStore(t *testing.T) {
	tests := []struct {
		name  string
		actor *ateapipb.Actor
		wantR string
		wantI string
	}{
		{
			name:  "a versioned actor carries its version onto the route",
			actor: runningActor(),
			wantR: "3",
			wantI: "10.0.0.5",
		},
		{
			// Substrate reports zero before it has versioned an actor, and zero is
			// not a canonical resource version.
			name: "an unversioned actor is raised to the first valid version",
			actor: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Version: 0},
				Status: &ateapipb.ActorStatus{
					State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
				},
			},
			wantR: "1",
		},
		{
			// GetRoute is called on paths that have no actor response to hand.
			name:  "a missing actor still yields a storable route",
			actor: nil,
			wantR: "1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			route := routeFromActorID("team-a--abcd1234", "team-a", "alice", "abcd1234-uid", tt.actor)
			assert.Equal(t, tt.wantR, route.ResourceVersion)
			assert.Equal(t, tt.wantI, route.IP)

			// The store is the real arbiter, so assert against it rather than
			// against a copy of its validation rules.
			store := sandboxroute.NewStore()
			result := store.Upsert(route)
			require.Equal(t, sandboxroute.EventResultApplied, result.Result,
				"route rejected: %s", result.Reason)

			stored, ok := store.Get(route.ID)
			require.True(t, ok, "route must be retrievable by sandbox ID")
			assert.Equal(t, "team-a", stored.Namespace)
			assert.Equal(t, "alice", stored.Owner)
		})
	}
}

// Hibernation withdraws the endpoint but keeps the identity. The store ignores an
// upsert that does not advance the version, so the withdrawal has to move it.
func TestClearRouteEndpointSupersedesTheStoredRoute(t *testing.T) {
	store := NewInMemoryMetadataStore()
	meta := &Metadata{
		SandboxID: "team-a--abcd1234",
		ActorID:   "abcd1234-uid",
		Namespace: "team-a",
		Owner:     "alice",
		Phase:     PhaseRunning,
		Route:     routeFromActorID("team-a--abcd1234", "team-a", "alice", "abcd1234-uid", runningActor()),
	}
	store.Put(meta)
	sbx := NewSandbox(meta, nil, store, NewKeyedLocker())

	routes := sandboxroute.NewStore()
	require.Equal(t, sandboxroute.EventResultApplied, routes.Upsert(meta.Route).Result)

	sbx.setPhase(PhaseSuspended)
	sbx.clearRouteEndpoint()

	withdrawn, err := sbx.GetRoute()
	require.NoError(t, err)
	assert.Empty(t, withdrawn.IP, "a hibernated actor has no reachable address")
	assert.Equal(t, PhaseSuspended, withdrawn.State)
	assert.Equal(t, "4", withdrawn.ResourceVersion, "the version must supersede the stored one")

	// The withdrawal must actually land, or the gateway keeps routing to a freed
	// worker.
	require.Equal(t, sandboxroute.EventResultApplied, routes.Upsert(withdrawn).Result)
	stored, ok := routes.Get(meta.SandboxID)
	require.True(t, ok)
	assert.Empty(t, stored.IP)

	// The store copy must agree with the sandbox copy, or a later reader would
	// resurrect the stale endpoint.
	persisted, err := store.Get(meta.SandboxID)
	require.NoError(t, err)
	assert.Empty(t, persisted.Route.IP)
	assert.Equal(t, "4", persisted.Route.ResourceVersion)
}

// The listing paginator drops any sandbox whose claim-time annotation is empty,
// so a record without one is invisible to a list no matter how it is filtered.
// This backend has no Sandbox object to carry the annotation, so it has to
// project one.
func TestNewSandboxCarriesTheAnnotationsAListNeeds(t *testing.T) {
	claimedAt := time.Date(2026, 8, 21, 10, 30, 0, 0, time.UTC)

	tests := []struct {
		name      string
		owner     string
		wantOwner string
	}{
		{name: "a claimed sandbox reports its owner", owner: "alice", wantOwner: "alice"},
		{
			// Substrate stores no owner, so a recovered record has none. It must
			// still carry a claim time or it could never be listed.
			name: "a recovered sandbox reports no owner but still a claim time",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sbx := NewSandbox(&Metadata{
				SandboxID:  "team-a--abcd1234",
				ActorID:    "abcd1234-uid",
				Namespace:  "team-a",
				Owner:      tt.owner,
				CreateTime: claimedAt,
			}, nil, nil, nil)

			annotations := sbx.GetAnnotations()
			assert.Equal(t, tt.wantOwner, annotations[agentsv1alpha1.AnnotationOwner])
			assert.Equal(t, "2026-08-21T10:30:00Z", annotations[agentsv1alpha1.AnnotationClaimTime])
		})
	}
}

// A caller filters on the states the sandbox API defines, so a phase outside that
// vocabulary makes a sandbox unreachable and unlistable however healthy it is.
func TestSandboxGetStateSpeaksTheAPIVocabulary(t *testing.T) {
	apiStates := []string{
		agentsv1alpha1.SandboxStateCreating,
		agentsv1alpha1.SandboxStateAvailable,
		agentsv1alpha1.SandboxStateRunning,
		agentsv1alpha1.SandboxStatePaused,
		agentsv1alpha1.SandboxStateDead,
	}

	tests := []struct {
		name  string
		phase string
		want  string
	}{
		{name: "running passes through", phase: PhaseRunning, want: agentsv1alpha1.SandboxStateRunning},
		{name: "paused passes through", phase: PhasePaused, want: agentsv1alpha1.SandboxStatePaused},
		{
			// Hibernating by releasing the worker is a substrate concern; the
			// caller only sees that the sandbox is paused.
			name:  "suspended reads as paused",
			phase: PhaseSuspended,
			want:  agentsv1alpha1.SandboxStatePaused,
		},
		{
			// Not yet reachable, which is what creating conveys.
			name:  "resuming reads as creating",
			phase: PhaseResuming,
			want:  agentsv1alpha1.SandboxStateCreating,
		},
		{name: "crashed reads as dead", phase: PhaseCrashed, want: agentsv1alpha1.SandboxStateDead},
		{
			// An unknown phase must stay distinguishable from a real state rather
			// than be reported as some healthy one.
			name:  "an absent phase stays absent",
			phase: "",
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sbx := NewSandbox(&Metadata{
				SandboxID: "team-a--abcd1234",
				Namespace: "team-a",
				Phase:     tt.phase,
			}, nil, nil, nil)

			state, reason := sbx.GetState()
			assert.Equal(t, tt.want, state)
			assert.Empty(t, reason, "substrate reports a status, not a diagnostic message")

			if tt.phase != "" {
				assert.Contains(t, apiStates, state,
					"a state outside the API vocabulary is filtered out everywhere")
			}
			// The internal phase stays available for backend decisions.
			assert.Equal(t, tt.phase, sbx.Phase())
		})
	}
}

func TestNextRouteResourceVersion(t *testing.T) {
	tests := []struct {
		name    string
		current string
		want    string
	}{
		{name: "a valid version is incremented", current: "7", want: "8"},
		{name: "an absent version restarts at the first valid one", current: "", want: "1"},
		{
			// A malformed record must not pin the route forever.
			name: "an unparsable version restarts at the first valid one", current: "abc", want: "1",
		},
		{name: "zero restarts at the first valid one", current: "0", want: "1"},
		{name: "a negative version restarts at the first valid one", current: "-5", want: "1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, nextRouteResourceVersion(tt.current))
		})
	}
}
