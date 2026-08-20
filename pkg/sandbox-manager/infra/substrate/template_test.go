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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, atev1alpha1.AddToScheme(scheme))
	require.NoError(t, agentsv1alpha1.AddToScheme(scheme))
	return scheme
}

// actorTemplate builds an ActorTemplate as the E2B build API would create it.
func actorTemplate(templateID, buildID string, phase atev1alpha1.PhaseType, created time.Time) *atev1alpha1.ActorTemplate {
	return &atev1alpha1.ActorTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      templateID + "-" + buildID,
			Namespace: "team-a",
			Labels: map[string]string{
				LabelTemplateID: templateID,
				LabelBuildID:    buildID,
			},
			CreationTimestamp: metav1.NewTime(created),
		},
		Status: atev1alpha1.ActorTemplateStatus{Phase: phase},
	}
}

func newResolver(t *testing.T, objs ...client.Object) *TemplateResolver {
	t.Helper()
	builder := fake.NewClientBuilder().WithScheme(testScheme(t))
	if len(objs) > 0 {
		builder = builder.WithObjects(objs...)
	}
	return NewTemplateResolver(builder.Build())
}

func TestBuildStatusOf(t *testing.T) {
	tests := []struct {
		name  string
		phase atev1alpha1.PhaseType
		want  BuildStatus
	}{
		{name: "ready", phase: atev1alpha1.PhaseReady, want: BuildStatusReady},
		{name: "failed", phase: atev1alpha1.PhaseFailed, want: BuildStatusError},
		// Substrate prepares a golden snapshot before a template can serve actors,
		// so every phase short of Ready is still building.
		{name: "initial", phase: atev1alpha1.PhaseInitial, want: BuildStatusBuilding},
		{name: "resuming golden actor", phase: atev1alpha1.PhaseResumeGoldenActor, want: BuildStatusBuilding},
		{name: "waiting for golden actor", phase: atev1alpha1.PhaseWaitGoldenActor, want: BuildStatusBuilding},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpl := actorTemplate("counter", "b1", tt.phase, time.Now())
			assert.Equal(t, tt.want, BuildStatusOf(tmpl))
		})
	}

	t.Run("a nil template is an error, never a usable build", func(t *testing.T) {
		assert.Equal(t, BuildStatusError, BuildStatusOf(nil))
	})
}

func TestTemplateResolverResolve(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	t.Run("resolves the only ready build", func(t *testing.T) {
		r := newResolver(t, actorTemplate("counter", "b1", atev1alpha1.PhaseReady, now))

		got, err := r.Resolve(ctx, "team-a", "counter")
		require.NoError(t, err)
		assert.Equal(t, "counter-b1", got.ActorTemplateName)
		assert.Equal(t, "team-a", got.Namespace)
		assert.Equal(t, "b1", got.BuildID)
	})

	// A template name must behave like an image name: it points at the most
	// recent successful build.
	t.Run("prefers the newest ready build", func(t *testing.T) {
		r := newResolver(t,
			actorTemplate("counter", "b1", atev1alpha1.PhaseReady, now.Add(-2*time.Hour)),
			actorTemplate("counter", "b2", atev1alpha1.PhaseReady, now.Add(-time.Hour)),
			actorTemplate("counter", "b3", atev1alpha1.PhaseReady, now),
		)

		got, err := r.Resolve(ctx, "team-a", "counter")
		require.NoError(t, err)
		assert.Equal(t, "b3", got.BuildID)
	})

	// A newer build that is still building or has failed must not shadow the last
	// build that actually works.
	t.Run("skips newer builds that are not ready", func(t *testing.T) {
		r := newResolver(t,
			actorTemplate("counter", "b1", atev1alpha1.PhaseReady, now.Add(-time.Hour)),
			actorTemplate("counter", "b2", atev1alpha1.PhaseWaitGoldenActor, now),
			actorTemplate("counter", "b3", atev1alpha1.PhaseFailed, now.Add(time.Hour)),
		)

		got, err := r.Resolve(ctx, "team-a", "counter")
		require.NoError(t, err)
		assert.Equal(t, "b1", got.BuildID)
	})

	t.Run("reports no builds distinctly from none ready", func(t *testing.T) {
		r := newResolver(t, actorTemplate("other", "b1", atev1alpha1.PhaseReady, now))

		_, err := r.Resolve(ctx, "team-a", "counter")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no builds")
	})

	t.Run("reports a pending build as not ready", func(t *testing.T) {
		r := newResolver(t, actorTemplate("counter", "b1", atev1alpha1.PhaseWaitGoldenActor, now))

		_, err := r.Resolve(ctx, "team-a", "counter")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "none is ready")
	})

	t.Run("does not cross namespaces", func(t *testing.T) {
		other := actorTemplate("counter", "b1", atev1alpha1.PhaseReady, now)
		other.Namespace = "team-b"
		r := newResolver(t, other)

		_, err := r.Resolve(ctx, "team-a", "counter")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no builds")
	})

	t.Run("rejects missing arguments", func(t *testing.T) {
		r := newResolver(t)
		for _, tc := range []struct{ namespace, template string }{
			{"", "counter"},
			{"team-a", ""},
		} {
			_, err := r.Resolve(ctx, tc.namespace, tc.template)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "required")
		}
	})

	t.Run("an unconfigured resolver errors instead of panicking", func(t *testing.T) {
		var r *TemplateResolver
		_, err := r.Resolve(ctx, "team-a", "counter")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not configured")
	})
}

func TestTemplateResolverHasTemplate(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	tests := []struct {
		name string
		objs []client.Object
		want bool
	}{
		{
			name: "ready build",
			objs: []client.Object{actorTemplate("counter", "b1", atev1alpha1.PhaseReady, now)},
			want: true,
		},
		{
			name: "building only",
			objs: []client.Object{actorTemplate("counter", "b1", atev1alpha1.PhaseWaitGoldenActor, now)},
			want: false,
		},
		{name: "nothing built", objs: nil, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newResolver(t, tt.objs...)
			assert.Equal(t, tt.want, r.HasTemplate(ctx, "team-a", "counter"))
		})
	}
}

func TestTemplateResolverGetBuild(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	r := newResolver(t,
		actorTemplate("counter", "b1", atev1alpha1.PhaseReady, now.Add(-time.Hour)),
		actorTemplate("counter", "b2", atev1alpha1.PhaseFailed, now),
	)

	t.Run("returns the requested build regardless of its status", func(t *testing.T) {
		got, err := r.GetBuild(ctx, "team-a", "counter", "b2")
		require.NoError(t, err)
		assert.Equal(t, "counter-b2", got.Name)
		assert.Equal(t, BuildStatusError, BuildStatusOf(got))
	})

	t.Run("reports an unknown build as not found", func(t *testing.T) {
		_, err := r.GetBuild(ctx, "team-a", "counter", "nope")
		require.Error(t, err)
		assert.True(t, apierrors.IsNotFound(err))
	})
}

func TestTemplateResolverListTemplateIDs(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	// An ActorTemplate applied by hand has no E2B identity and must stay out of
	// the E2B template listing.
	handWritten := actorTemplate("counter", "b0", atev1alpha1.PhaseReady, now)
	handWritten.Name = "hand-written"
	handWritten.Labels = nil

	r := newResolver(t,
		actorTemplate("counter", "b1", atev1alpha1.PhaseReady, now.Add(-time.Hour)),
		actorTemplate("counter", "b2", atev1alpha1.PhaseReady, now),
		actorTemplate("interpreter", "b1", atev1alpha1.PhaseReady, now),
		handWritten,
	)

	grouped, err := r.ListTemplateIDs(ctx, "team-a")
	require.NoError(t, err)

	require.Len(t, grouped, 2)
	require.Len(t, grouped["counter"], 2)
	assert.Equal(t, "b2", grouped["counter"][0].Labels[LabelBuildID], "newest build must come first")
	assert.Equal(t, "b1", grouped["counter"][1].Labels[LabelBuildID])
	assert.Len(t, grouped["interpreter"], 1)
}

func TestPoolSelector(t *testing.T) {
	tests := []struct {
		name           string
		sandboxSetName string
		want           map[string]string
	}{
		{
			name:           "a named pool narrows placement",
			sandboxSetName: "counter-hipri",
			want:           map[string]string{agentsv1alpha1.LabelSandboxSet: "counter-hipri"},
		},
		{
			// Substrate treats a nil selector as "every pool is eligible", so an
			// unset pool must not become a selector that matches nothing.
			name:           "no pool leaves placement unconstrained",
			sandboxSetName: "",
			want:           nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, PoolSelector(tt.sandboxSetName))
		})
	}
}
