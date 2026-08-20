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

package sandboxset

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/openkruise/agents/api/v1alpha1"
)

const ateomImage = "registry.example.com/substrate/ateom-gvisor@sha256:" +
	"5917d2049bc1cf6029855e79ed7d60dc0eb004e9e49ee20f02776ae5d2a633ef"

func TestIsSubstrateBackend(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        bool
	}{
		{name: "substrate backend", annotations: map[string]string{AnnotationBackend: BackendSubstrate}, want: true},
		{name: "no annotations", annotations: nil, want: false},
		{name: "other backend", annotations: map[string]string{AnnotationBackend: "kubernetes"}, want: false},
		{name: "empty value", annotations: map[string]string{AnnotationBackend: ""}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsSubstrateBackend(tt.annotations))
		})
	}
}

func TestHibernateMode(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        string
	}{
		{name: "explicit pause", annotations: map[string]string{AnnotationSubstrateHibernateMode: HibernateModePause}, want: HibernateModePause},
		{name: "explicit suspend", annotations: map[string]string{AnnotationSubstrateHibernateMode: HibernateModeSuspend}, want: HibernateModeSuspend},
		{name: "absent defaults to suspend", annotations: nil, want: HibernateModeSuspend},
		{name: "unknown value defaults to suspend", annotations: map[string]string{AnnotationSubstrateHibernateMode: "freeze"}, want: HibernateModeSuspend},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, HibernateMode(tt.annotations))
		})
	}
}

// substrateSandboxSet returns a minimal substrate-backed SandboxSet whose pod
// template carries only the ateom container.
func substrateSandboxSet(replicas int32) *v1alpha1.SandboxSet {
	return &v1alpha1.SandboxSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "counter",
			Namespace: "ate-demo-counter",
			UID:       types.UID("sandboxset-uid"),
			Annotations: map[string]string{
				AnnotationBackend: BackendSubstrate,
			},
		},
		Spec: v1alpha1.SandboxSetSpec{
			Replicas: replicas,
			EmbeddedSandboxTemplate: v1alpha1.EmbeddedSandboxTemplate{
				Template: &corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "ateom", Image: ateomImage}},
					},
				},
			},
		},
	}
}

func TestBuildWorkerPoolSpec(t *testing.T) {
	tests := []struct {
		name string
		// mutate adapts the baseline SandboxSet for this case.
		mutate  func(*v1alpha1.SandboxSet)
		wantErr string
		// verify asserts on the produced spec; only run when wantErr is empty.
		verify func(*testing.T, *atev1alpha1.WorkerPoolSpec)
	}{
		{
			name: "replicas and ateom image are projected",
			verify: func(t *testing.T, spec *atev1alpha1.WorkerPoolSpec) {
				assert.Equal(t, int32(5), spec.Replicas)
				assert.Equal(t, ateomImage, spec.AteomImage)
				assert.Nil(t, spec.Template, "a bare pod template must not produce worker pod settings")
			},
		},
		{
			name: "sandbox class annotation is projected",
			mutate: func(sbs *v1alpha1.SandboxSet) {
				sbs.Annotations[AnnotationSubstrateSandboxClass] = "microvm"
			},
			verify: func(t *testing.T, spec *atev1alpha1.WorkerPoolSpec) {
				assert.Equal(t, atev1alpha1.SandboxClass("microvm"), spec.SandboxClass)
			},
		},
		{
			name: "absent sandbox class defers to the CRD default",
			verify: func(t *testing.T, spec *atev1alpha1.WorkerPoolSpec) {
				assert.Empty(t, spec.SandboxClass)
			},
		},
		{
			name: "container resources become worker pod resources",
			mutate: func(sbs *v1alpha1.SandboxSet) {
				sbs.Spec.Template.Spec.Containers[0].Resources = corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")},
					Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8")},
				}
			},
			verify: func(t *testing.T, spec *atev1alpha1.WorkerPoolSpec) {
				require.NotNil(t, spec.Template)
				require.NotNil(t, spec.Template.Resources)
				assert.Equal(t, resource.MustParse("4"), spec.Template.Resources.Requests[corev1.ResourceCPU])
				assert.Equal(t, resource.MustParse("8"), spec.Template.Resources.Limits[corev1.ResourceCPU])
			},
		},
		{
			name: "scheduling fields are projected onto the worker pod template",
			mutate: func(sbs *v1alpha1.SandboxSet) {
				sbs.Spec.Template.Spec.NodeSelector = map[string]string{"instance-type": "g7"}
				sbs.Spec.Template.Spec.PriorityClassName = "substrate-worker"
				sbs.Spec.Template.Spec.Tolerations = []corev1.Toleration{{
					Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "worker",
				}}
				sbs.Spec.Template.Spec.Affinity = &corev1.Affinity{
					NodeAffinity: &corev1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{},
					},
				}
			},
			verify: func(t *testing.T, spec *atev1alpha1.WorkerPoolSpec) {
				require.NotNil(t, spec.Template)
				assert.Equal(t, map[string]string{"instance-type": "g7"}, spec.Template.NodeSelector)
				assert.Equal(t, "substrate-worker", spec.Template.PriorityClassName)
				assert.Len(t, spec.Template.Tolerations, 1)
				assert.NotNil(t, spec.Template.NodeAffinity)
			},
		},
		{
			name:    "missing pod template is rejected",
			mutate:  func(sbs *v1alpha1.SandboxSet) { sbs.Spec.Template = nil },
			wantErr: "spec.template is required",
		},
		{
			name: "empty container list is rejected",
			mutate: func(sbs *v1alpha1.SandboxSet) {
				sbs.Spec.Template.Spec.Containers = nil
			},
			wantErr: "must declare the ateom container",
		},
		{
			// A WorkerPool carries a single ateomImage, so extra containers would be
			// silently dropped. Reject instead.
			name: "multiple containers are rejected",
			mutate: func(sbs *v1alpha1.SandboxSet) {
				sbs.Spec.Template.Spec.Containers = append(sbs.Spec.Template.Spec.Containers,
					corev1.Container{Name: "sidecar", Image: "sidecar@sha256:abc"})
			},
			wantErr: "exactly one container",
		},
		{
			name: "empty image is rejected",
			mutate: func(sbs *v1alpha1.SandboxSet) {
				sbs.Spec.Template.Spec.Containers[0].Image = ""
			},
			wantErr: "image must be set",
		},
		{
			// Substrate can only express node affinity, so a pod affinity rule would
			// be silently lost.
			name: "pod affinity is rejected",
			mutate: func(sbs *v1alpha1.SandboxSet) {
				sbs.Spec.Template.Spec.Affinity = &corev1.Affinity{PodAffinity: &corev1.PodAffinity{}}
			},
			wantErr: "nodeAffinity only",
		},
		{
			name: "pod anti-affinity is rejected",
			mutate: func(sbs *v1alpha1.SandboxSet) {
				sbs.Spec.Template.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{}}
			},
			wantErr: "nodeAffinity only",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sbs := substrateSandboxSet(5)
			if tt.mutate != nil {
				tt.mutate(sbs)
			}

			spec, err := buildWorkerPoolSpec(sbs)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, spec)
			if tt.verify != nil {
				tt.verify(t, spec)
			}
		})
	}
}

func newSubstrateReconciler(objs ...client.Object) *Reconciler {
	builder := fake.NewClientBuilder().WithScheme(testScheme).
		WithStatusSubresource(&v1alpha1.SandboxSet{}, &atev1alpha1.WorkerPool{})
	if len(objs) > 0 {
		builder = builder.WithObjects(objs...)
	}
	return &Reconciler{Client: builder.Build(), Scheme: testScheme}
}

func TestReconcileSubstrate(t *testing.T) {
	ctx := context.Background()
	poolKey := types.NamespacedName{Namespace: "ate-demo-counter", Name: "counter"}

	t.Run("creates the worker pool with the sandboxset label and owner reference", func(t *testing.T) {
		sbs := substrateSandboxSet(5)
		r := newSubstrateReconciler(sbs)

		require.NoError(t, r.reconcileSubstrate(ctx, sbs))

		pool := &atev1alpha1.WorkerPool{}
		require.NoError(t, r.Get(ctx, poolKey, pool))
		assert.Equal(t, int32(5), pool.Spec.Replicas)
		assert.Equal(t, ateomImage, pool.Spec.AteomImage)
		// Actors select their pool through this label, so it is a hard contract.
		assert.Equal(t, "counter", pool.Labels[LabelSandboxSet])
		require.Len(t, pool.OwnerReferences, 1)
		assert.Equal(t, "SandboxSet", pool.OwnerReferences[0].Kind)
		assert.True(t, *pool.OwnerReferences[0].Controller)
	})

	t.Run("reconciles replicas on an existing pool", func(t *testing.T) {
		sbs := substrateSandboxSet(5)
		r := newSubstrateReconciler(sbs)
		require.NoError(t, r.reconcileSubstrate(ctx, sbs))

		sbs.Spec.Replicas = 12
		require.NoError(t, r.reconcileSubstrate(ctx, sbs))

		pool := &atev1alpha1.WorkerPool{}
		require.NoError(t, r.Get(ctx, poolKey, pool))
		assert.Equal(t, int32(12), pool.Spec.Replicas)
	})

	t.Run("is idempotent when nothing changed", func(t *testing.T) {
		sbs := substrateSandboxSet(5)
		r := newSubstrateReconciler(sbs)
		require.NoError(t, r.reconcileSubstrate(ctx, sbs))

		first := &atev1alpha1.WorkerPool{}
		require.NoError(t, r.Get(ctx, poolKey, first))

		require.NoError(t, r.reconcileSubstrate(ctx, sbs))

		second := &atev1alpha1.WorkerPool{}
		require.NoError(t, r.Get(ctx, poolKey, second))
		assert.Equal(t, first.ResourceVersion, second.ResourceVersion,
			"an unchanged SandboxSet must not issue a write")
	})

	t.Run("restores the sandboxset label if it is removed", func(t *testing.T) {
		sbs := substrateSandboxSet(5)
		r := newSubstrateReconciler(sbs)
		require.NoError(t, r.reconcileSubstrate(ctx, sbs))

		pool := &atev1alpha1.WorkerPool{}
		require.NoError(t, r.Get(ctx, poolKey, pool))
		delete(pool.Labels, LabelSandboxSet)
		require.NoError(t, r.Update(ctx, pool))

		require.NoError(t, r.reconcileSubstrate(ctx, sbs))

		require.NoError(t, r.Get(ctx, poolKey, pool))
		assert.Equal(t, "counter", pool.Labels[LabelSandboxSet])
	})

	t.Run("propagates a spec projection failure", func(t *testing.T) {
		sbs := substrateSandboxSet(5)
		sbs.Spec.Template = nil
		r := newSubstrateReconciler(sbs)

		err := r.reconcileSubstrate(ctx, sbs)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "build WorkerPool spec")
	})

	t.Run("never creates sandboxes", func(t *testing.T) {
		sbs := substrateSandboxSet(5)
		r := newSubstrateReconciler(sbs)
		require.NoError(t, r.reconcileSubstrate(ctx, sbs))

		var sandboxes v1alpha1.SandboxList
		require.NoError(t, r.List(ctx, &sandboxes))
		assert.Empty(t, sandboxes.Items, "the substrate backend must not own Sandbox CRs")
	})
}
