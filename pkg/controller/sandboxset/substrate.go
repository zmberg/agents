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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
)

// The Substrate backend contract lives in api/v1alpha1 because sandbox-manager
// reads the same annotations and selects on the same label. These aliases keep
// the call sites in this package short.
const (
	AnnotationBackend                = agentsv1alpha1.AnnotationBackend
	BackendSubstrate                 = agentsv1alpha1.BackendSubstrate
	AnnotationSubstrateSandboxClass  = agentsv1alpha1.AnnotationSubstrateSandboxClass
	AnnotationSubstrateHibernateMode = agentsv1alpha1.AnnotationSubstrateHibernateMode
	LabelSandboxSet                  = agentsv1alpha1.LabelSandboxSet
	HibernateModePause               = agentsv1alpha1.HibernateModePause
	HibernateModeSuspend             = agentsv1alpha1.HibernateModeSuspend
)

// IsSubstrateBackend reports whether the annotations select the Substrate backend.
func IsSubstrateBackend(annotations map[string]string) bool {
	return annotations[AnnotationBackend] == BackendSubstrate
}

// HibernateMode returns the hibernate mode selected by the annotations,
// defaulting to suspend so that idle sandboxes release their worker.
func HibernateMode(annotations map[string]string) string {
	if annotations[AnnotationSubstrateHibernateMode] == HibernateModePause {
		return HibernateModePause
	}
	return HibernateModeSuspend
}

// reconcileSubstrate materializes a SandboxSet as Substrate capacity. A
// SandboxSet is only a WorkerPool: it declares how many workers exist and what
// shape they have. Actor templates come from the E2B build API and individual
// actors are created imperatively by sandbox-manager, so this path never
// creates Sandbox CRs, ActorTemplates, or Actors.
func (r *Reconciler) reconcileSubstrate(ctx context.Context, sbs *agentsv1alpha1.SandboxSet) error {
	log := logf.FromContext(ctx).WithValues("backend", BackendSubstrate)

	spec, err := buildWorkerPoolSpec(sbs)
	if err != nil {
		return fmt.Errorf("build WorkerPool spec: %w", err)
	}

	if err := r.ensureWorkerPool(ctx, sbs, spec); err != nil {
		return fmt.Errorf("ensure WorkerPool %s/%s: %w", sbs.Namespace, sbs.Name, err)
	}

	log.V(1).Info("reconciled substrate capacity", "workerPool", sbs.Name, "replicas", spec.Replicas)
	return nil
}

// buildWorkerPoolSpec projects a SandboxSet onto a WorkerPool spec. The pod
// template's first container carries the ateom image and the per-worker
// resources; the remaining scheduling fields map onto WorkerPool's own pod
// template. Fields Substrate cannot express are rejected rather than dropped,
// because a silently ignored scheduling constraint is far harder to diagnose
// than a failed reconcile.
func buildWorkerPoolSpec(sbs *agentsv1alpha1.SandboxSet) (*atev1alpha1.WorkerPoolSpec, error) {
	if sbs.Spec.Template == nil {
		return nil, fmt.Errorf("spec.template is required for the substrate backend")
	}
	podSpec := sbs.Spec.Template.Spec
	if len(podSpec.Containers) == 0 {
		return nil, fmt.Errorf("spec.template.spec.containers must declare the ateom container")
	}
	if len(podSpec.Containers) > 1 {
		return nil, fmt.Errorf("spec.template.spec.containers must declare exactly one container: "+
			"a WorkerPool carries a single ateomImage, got %d containers", len(podSpec.Containers))
	}

	ateom := podSpec.Containers[0]
	if ateom.Image == "" {
		return nil, fmt.Errorf("spec.template.spec.containers[0].image must be set to the ateom image")
	}

	spec := &atev1alpha1.WorkerPoolSpec{
		Replicas:     sbs.Spec.Replicas,
		AteomImage:   ateom.Image,
		SandboxClass: atev1alpha1.SandboxClass(sbs.Annotations[AnnotationSubstrateSandboxClass]),
	}

	template, err := buildWorkerPoolPodTemplate(sbs, ateom)
	if err != nil {
		return nil, err
	}
	spec.Template = template

	return spec, nil
}

// buildWorkerPoolPodTemplate projects the scheduling subset of a PodSpec that a
// WorkerPool can represent. It returns nil when nothing needs to be set so the
// generated WorkerPool stays minimal.
func buildWorkerPoolPodTemplate(
	sbs *agentsv1alpha1.SandboxSet,
	ateom corev1.Container,
) (*atev1alpha1.WorkerPoolPodTemplate, error) {
	podSpec := sbs.Spec.Template.Spec

	if podSpec.Affinity != nil {
		if podSpec.Affinity.PodAffinity != nil || podSpec.Affinity.PodAntiAffinity != nil {
			return nil, fmt.Errorf("spec.template.spec.affinity supports nodeAffinity only: " +
				"a WorkerPool cannot express pod affinity or anti-affinity")
		}
	}

	template := &atev1alpha1.WorkerPoolPodTemplate{
		NodeSelector:      podSpec.NodeSelector,
		Tolerations:       podSpec.Tolerations,
		PriorityClassName: podSpec.PriorityClassName,
	}
	if podSpec.Affinity != nil {
		template.NodeAffinity = podSpec.Affinity.NodeAffinity
	}
	if ateom.Resources.Requests != nil || ateom.Resources.Limits != nil {
		resources := ateom.Resources
		template.Resources = &resources
	}

	if template.NodeSelector == nil && template.Tolerations == nil &&
		template.PriorityClassName == "" && template.NodeAffinity == nil && template.Resources == nil {
		return nil, nil
	}
	return template, nil
}

// ensureWorkerPool creates the WorkerPool or reconciles the fields a SandboxSet
// owns. It leaves everything else on an existing pool untouched so that manual
// or Substrate-side additions survive.
func (r *Reconciler) ensureWorkerPool(
	ctx context.Context,
	sbs *agentsv1alpha1.SandboxSet,
	spec *atev1alpha1.WorkerPoolSpec,
) error {
	existing := &atev1alpha1.WorkerPool{}
	err := r.Get(ctx, client.ObjectKey{Namespace: sbs.Namespace, Name: sbs.Name}, existing)
	if apierrors.IsNotFound(err) {
		return r.createWorkerPool(ctx, sbs, spec)
	}
	if err != nil {
		return err
	}

	clone := existing.DeepCopy()
	clone.Spec.Replicas = spec.Replicas
	clone.Spec.AteomImage = spec.AteomImage
	clone.Spec.SandboxClass = spec.SandboxClass
	clone.Spec.Template = spec.Template
	if clone.Labels == nil {
		clone.Labels = map[string]string{}
	}
	clone.Labels[LabelSandboxSet] = sbs.Name

	if equality.Semantic.DeepEqual(existing.Spec, clone.Spec) &&
		equality.Semantic.DeepEqual(existing.Labels, clone.Labels) {
		return nil
	}
	return r.Update(ctx, clone)
}

func (r *Reconciler) createWorkerPool(
	ctx context.Context,
	sbs *agentsv1alpha1.SandboxSet,
	spec *atev1alpha1.WorkerPoolSpec,
) error {
	pool := &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sbs.Name,
			Namespace: sbs.Namespace,
			Labels: map[string]string{
				LabelSandboxSet: sbs.Name,
			},
		},
		Spec: *spec,
	}
	if err := ctrlutil.SetControllerReference(sbs, pool, r.Scheme); err != nil {
		return fmt.Errorf("set controller reference: %w", err)
	}
	if err := r.Create(ctx, pool); err != nil {
		return client.IgnoreAlreadyExists(err)
	}
	return nil
}
