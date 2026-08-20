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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/openkruise/agents/pkg/sandbox-manager/infra"
)

// fakeControl is a scriptable ateapipb.ControlClient. Only the methods the
// backend exercises are implemented; the embedded interface makes the rest
// panic if ever called, which surfaces an untested path instead of hiding it.
type fakeControl struct {
	ateapipb.ControlClient

	atespaces map[string]bool

	createActor  func(*ateapipb.CreateActorRequest) (*ateapipb.Actor, error)
	resumeActor  func(*ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error)
	pauseActor   func(*ateapipb.PauseActorRequest) (*ateapipb.PauseActorResponse, error)
	suspendActor func(*ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error)
	deleteActor  func(*ateapipb.DeleteActorRequest) (*ateapipb.Actor, error)
	getActor     func(*ateapipb.GetActorRequest) (*ateapipb.Actor, error)

	// captured requests for assertions
	lastCreate  *ateapipb.CreateActorRequest
	suspendSeen int
	deleteSeen  int
}

func newFakeControl() *fakeControl {
	return &fakeControl{atespaces: map[string]bool{}}
}

func (f *fakeControl) GetAtespace(_ context.Context, in *ateapipb.GetAtespaceRequest, _ ...grpc.CallOption) (*ateapipb.Atespace, error) {
	if f.atespaces[in.GetAtespace().GetName()] {
		return &ateapipb.Atespace{}, nil
	}
	return nil, status.Error(codes.NotFound, "no atespace")
}

func (f *fakeControl) CreateAtespace(_ context.Context, in *ateapipb.CreateAtespaceRequest, _ ...grpc.CallOption) (*ateapipb.Atespace, error) {
	f.atespaces[in.GetAtespace().GetMetadata().GetName()] = true
	return in.GetAtespace(), nil
}

func (f *fakeControl) CreateActor(_ context.Context, in *ateapipb.CreateActorRequest, _ ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.lastCreate = in
	if f.createActor != nil {
		return f.createActor(in)
	}
	return in.GetActor(), nil
}

func (f *fakeControl) ResumeActor(_ context.Context, in *ateapipb.ResumeActorRequest, _ ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
	if f.resumeActor != nil {
		return f.resumeActor(in)
	}
	return &ateapipb.ResumeActorResponse{Actor: &ateapipb.Actor{
		Status:     ateapipb.Actor_STATUS_RUNNING,
		AteomPodIp: "10.0.0.5",
	}}, nil
}

func (f *fakeControl) PauseActor(_ context.Context, in *ateapipb.PauseActorRequest, _ ...grpc.CallOption) (*ateapipb.PauseActorResponse, error) {
	if f.pauseActor != nil {
		return f.pauseActor(in)
	}
	return &ateapipb.PauseActorResponse{}, nil
}

func (f *fakeControl) SuspendActor(_ context.Context, in *ateapipb.SuspendActorRequest, _ ...grpc.CallOption) (*ateapipb.SuspendActorResponse, error) {
	f.suspendSeen++
	if f.suspendActor != nil {
		return f.suspendActor(in)
	}
	return &ateapipb.SuspendActorResponse{}, nil
}

func (f *fakeControl) DeleteActor(_ context.Context, in *ateapipb.DeleteActorRequest, _ ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.deleteSeen++
	if f.deleteActor != nil {
		return f.deleteActor(in)
	}
	return &ateapipb.Actor{}, nil
}

func (f *fakeControl) GetActor(_ context.Context, in *ateapipb.GetActorRequest, _ ...grpc.CallOption) (*ateapipb.Actor, error) {
	if f.getActor != nil {
		return f.getActor(in)
	}
	return &ateapipb.Actor{Status: ateapipb.Actor_STATUS_RUNNING, AteomPodIp: "10.0.0.5"}, nil
}

// fakeResolver returns a fixed resolved template, standing in for the client
// read the real resolver performs.
func newInfraWithFake(t *testing.T, control ateapipb.ControlClient) *Infra {
	t.Helper()
	r := newResolver(t, actorTemplate("counter", "b1", atev1alpha1.PhaseReady, time.Now()))
	return &Infra{
		control:              control,
		store:                NewInMemoryMetadataStore(),
		locks:                NewKeyedLocker(),
		templates:            r,
		routes:               newRouteSource(),
		defaultHibernateMode: "suspend",
	}
}

func TestInfraClaimSandbox(t *testing.T) {
	ctx := context.Background()

	t.Run("creates and resumes an actor, storing its metadata and route", func(t *testing.T) {
		control := newFakeControl()
		i := newInfraWithFake(t, control)

		sbx, _, err := i.ClaimSandbox(ctx, infra.ClaimSandboxOptions{
			Namespace: "team-a",
			Template:  "counter",
			User:      "alice",
		})
		require.NoError(t, err)
		assert.Equal(t, PhaseRunning, sbx.Phase())
		assert.Equal(t, "10.0.0.5", sbx.GetIP())

		// The atespace is created on demand before the first actor.
		assert.True(t, control.atespaces["team-a"])

		// Metadata is persisted so the sandbox can later be found by ID.
		got, err := i.store.Get(sbx.GetSandboxID())
		require.NoError(t, err)
		assert.Equal(t, "alice", got.Owner)
		assert.Equal(t, "counter-b1", got.ActorTemplateName)
	})

	// The pool-selection metadata must reach the actor's worker_selector, which
	// is how a caller pins a sandbox to a specific SandboxSet's pool.
	t.Run("threads the selected SandboxSet into the actor worker selector", func(t *testing.T) {
		control := newFakeControl()
		i := newInfraWithFake(t, control)

		_, _, err := i.ClaimSandbox(ctx, infra.ClaimSandboxOptions{
			Namespace: "team-a",
			Template:  "counter",
			User:      "alice",
			Modifier: func(s infra.Sandbox) error {
				s.SetAnnotations(map[string]string{MetadataKeySandboxSet: "counter-hipri"})
				return nil
			},
		})
		require.NoError(t, err)

		require.NotNil(t, control.lastCreate.GetActor().GetWorkerSelector())
		assert.Equal(t, "counter-hipri",
			control.lastCreate.GetActor().GetWorkerSelector().GetMatchLabels()["agents.kruise.io/sandboxset"])
	})

	t.Run("leaves the worker selector unset when no pool is chosen", func(t *testing.T) {
		control := newFakeControl()
		i := newInfraWithFake(t, control)

		_, _, err := i.ClaimSandbox(ctx, infra.ClaimSandboxOptions{
			Namespace: "team-a", Template: "counter", User: "alice",
		})
		require.NoError(t, err)
		assert.Nil(t, control.lastCreate.GetActor().GetWorkerSelector())
	})

	// A failed resume must not leak a half-created actor holding a worker.
	t.Run("cleans up when resume fails", func(t *testing.T) {
		control := newFakeControl()
		control.resumeActor = func(*ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error) {
			return nil, status.Error(codes.Internal, "boom")
		}
		i := newInfraWithFake(t, control)

		_, _, err := i.ClaimSandbox(ctx, infra.ClaimSandboxOptions{
			Namespace: "team-a", Template: "counter", User: "alice",
		})
		require.Error(t, err)
		assert.Positive(t, control.suspendSeen, "a failed resume must suspend the actor")
		assert.Positive(t, control.deleteSeen, "a failed resume must delete the actor")
		assert.Empty(t, i.store.List(ListOptions{}), "no metadata must remain for a failed claim")
	})

	t.Run("reports an unknown template as not found", func(t *testing.T) {
		i := newInfraWithFake(t, newFakeControl())
		_, _, err := i.ClaimSandbox(ctx, infra.ClaimSandboxOptions{
			Namespace: "team-a", Template: "missing", User: "alice",
		})
		require.Error(t, err)
	})
}

func TestSandboxKill(t *testing.T) {
	ctx := context.Background()

	t.Run("suspends a running actor before deleting it", func(t *testing.T) {
		control := newFakeControl()
		i := newInfraWithFake(t, control)
		sbx, _, err := i.ClaimSandbox(ctx, infra.ClaimSandboxOptions{
			Namespace: "team-a", Template: "counter", User: "alice",
		})
		require.NoError(t, err)
		control.suspendSeen = 0
		control.deleteSeen = 0

		require.NoError(t, sbx.Kill(ctx))
		assert.Equal(t, 1, control.suspendSeen, "a running actor must be suspended before delete")
		assert.Equal(t, 1, control.deleteSeen)
		_, err = i.store.Get(sbx.GetSandboxID())
		assert.Error(t, err, "metadata must be gone after kill")
	})

	t.Run("treats a missing actor as already killed", func(t *testing.T) {
		control := newFakeControl()
		control.deleteActor = func(*ateapipb.DeleteActorRequest) (*ateapipb.Actor, error) {
			return nil, status.Error(codes.NotFound, "gone")
		}
		i := newInfraWithFake(t, control)
		sbx, _, err := i.ClaimSandbox(ctx, infra.ClaimSandboxOptions{
			Namespace: "team-a", Template: "counter", User: "alice",
		})
		require.NoError(t, err)

		require.NoError(t, sbx.Kill(ctx))
	})
}

func TestSandboxPauseResume(t *testing.T) {
	ctx := context.Background()

	t.Run("suspend mode frees the worker and clears the route endpoint", func(t *testing.T) {
		control := newFakeControl()
		i := newInfraWithFake(t, control)
		sbx, _, err := i.ClaimSandbox(ctx, infra.ClaimSandboxOptions{
			Namespace: "team-a", Template: "counter", User: "alice",
		})
		require.NoError(t, err)

		require.NoError(t, sbx.Pause(ctx, infra.PauseOptions{}))
		assert.Equal(t, PhaseSuspended, sbx.Phase())
		route, _ := sbx.GetRoute()
		assert.Empty(t, route.IP, "a hibernated actor must not keep a routable endpoint")
	})

	t.Run("resume restores the phase and route", func(t *testing.T) {
		control := newFakeControl()
		i := newInfraWithFake(t, control)
		sbx, _, err := i.ClaimSandbox(ctx, infra.ClaimSandboxOptions{
			Namespace: "team-a", Template: "counter", User: "alice",
		})
		require.NoError(t, err)
		require.NoError(t, sbx.Pause(ctx, infra.PauseOptions{}))

		require.NoError(t, sbx.Resume(ctx, infra.ResumeOptions{}))
		assert.Equal(t, PhaseRunning, sbx.Phase())
		assert.Equal(t, "10.0.0.5", sbx.GetIP())
	})
}
