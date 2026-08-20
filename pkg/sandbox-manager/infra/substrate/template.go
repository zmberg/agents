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
	"sort"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
)

const (
	// LabelTemplateID groups every ActorTemplate built for one E2B template name.
	// An ActorTemplate spec is immutable, so each build produces a new object and
	// this label is what ties those objects back to the user-facing name.
	LabelTemplateID = agentsv1alpha1.LabelE2BTemplateID

	// LabelBuildID identifies the single build that produced an ActorTemplate.
	LabelBuildID = agentsv1alpha1.LabelE2BBuildID
)

// BuildStatus is the E2B build status of a template.
type BuildStatus string

const (
	BuildStatusBuilding BuildStatus = "building"
	BuildStatusReady    BuildStatus = "ready"
	BuildStatusError    BuildStatus = "error"
)

// ResolvedTemplate names the ActorTemplate that backs an E2B template, together
// with the placement inputs a create needs.
type ResolvedTemplate struct {
	ActorTemplateName string
	Namespace         string
	BuildID           string
	// SandboxSetName is the pool the caller asked for; empty leaves placement to
	// the whole namespace.
	SandboxSetName string
	HibernateMode  string
}

// TemplateResolver maps an E2B template name to a ready ActorTemplate.
//
// It reads through a client rather than caching a startup snapshot, so a
// template built after sandbox-manager started is visible without a restart.
type TemplateResolver struct {
	reader client.Reader
}

// NewTemplateResolver returns a resolver backed by reader.
func NewTemplateResolver(reader client.Reader) *TemplateResolver {
	return &TemplateResolver{reader: reader}
}

// Resolve finds the newest ready ActorTemplate for templateName. This mirrors
// how a container image name resolves to its most recent successful build.
func (r *TemplateResolver) Resolve(ctx context.Context, namespace, templateName string) (*ResolvedTemplate, error) {
	if r == nil || r.reader == nil {
		return nil, fmt.Errorf("template resolver is not configured")
	}
	if namespace == "" || templateName == "" {
		return nil, fmt.Errorf("namespace and template name are required")
	}

	templates, err := r.listByTemplateID(ctx, namespace, templateName)
	if err != nil {
		return nil, err
	}
	if len(templates) == 0 {
		return nil, fmt.Errorf("template %s/%s has no builds", namespace, templateName)
	}

	ready := readyTemplates(templates)
	if len(ready) == 0 {
		return nil, fmt.Errorf("template %s/%s has %d build(s) but none is ready",
			namespace, templateName, len(templates))
	}

	newest := ready[0]
	return &ResolvedTemplate{
		ActorTemplateName: newest.Name,
		Namespace:         newest.Namespace,
		BuildID:           newest.Labels[LabelBuildID],
	}, nil
}

// HasTemplate reports whether templateName has at least one ready build.
func (r *TemplateResolver) HasTemplate(ctx context.Context, namespace, templateName string) bool {
	_, err := r.Resolve(ctx, namespace, templateName)
	return err == nil
}

// GetBuild returns the ActorTemplate produced by one specific build.
func (r *TemplateResolver) GetBuild(ctx context.Context, namespace, templateName, buildID string) (*atev1alpha1.ActorTemplate, error) {
	templates, err := r.listByTemplateID(ctx, namespace, templateName)
	if err != nil {
		return nil, err
	}
	for i := range templates {
		if templates[i].Labels[LabelBuildID] == buildID {
			return &templates[i], nil
		}
	}
	return nil, apierrors.NewNotFound(
		atev1alpha1.GroupVersion.WithResource("actortemplates").GroupResource(),
		fmt.Sprintf("%s build %s", templateName, buildID))
}

// ListTemplateIDs groups every ActorTemplate in a namespace by E2B template
// name, newest build first within each group.
func (r *TemplateResolver) ListTemplateIDs(ctx context.Context, namespace string) (map[string][]atev1alpha1.ActorTemplate, error) {
	if r == nil || r.reader == nil {
		return nil, fmt.Errorf("template resolver is not configured")
	}
	var list atev1alpha1.ActorTemplateList
	if err := r.reader.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list actor templates in %s: %w", namespace, err)
	}

	grouped := make(map[string][]atev1alpha1.ActorTemplate)
	for _, tmpl := range list.Items {
		id := tmpl.Labels[LabelTemplateID]
		if id == "" {
			// Not built through the E2B API; it has no E2B template identity.
			continue
		}
		grouped[id] = append(grouped[id], tmpl)
	}
	for id := range grouped {
		sortByCreationDesc(grouped[id])
	}
	return grouped, nil
}

func (r *TemplateResolver) listByTemplateID(ctx context.Context, namespace, templateName string) ([]atev1alpha1.ActorTemplate, error) {
	var list atev1alpha1.ActorTemplateList
	if err := r.reader.List(ctx, &list,
		client.InNamespace(namespace),
		client.MatchingLabels{LabelTemplateID: templateName},
	); err != nil {
		return nil, fmt.Errorf("list actor templates for %s/%s: %w", namespace, templateName, err)
	}
	sortByCreationDesc(list.Items)
	return list.Items, nil
}

// readyTemplates keeps only templates whose golden snapshot is usable.
func readyTemplates(templates []atev1alpha1.ActorTemplate) []atev1alpha1.ActorTemplate {
	ready := make([]atev1alpha1.ActorTemplate, 0, len(templates))
	for _, tmpl := range templates {
		if BuildStatusOf(&tmpl) == BuildStatusReady {
			ready = append(ready, tmpl)
		}
	}
	return ready
}

// sortByCreationDesc orders templates newest first, breaking ties by name so
// that resolution is deterministic when two builds share a timestamp.
func sortByCreationDesc(templates []atev1alpha1.ActorTemplate) {
	sort.SliceStable(templates, func(i, j int) bool {
		ti, tj := templates[i].CreationTimestamp, templates[j].CreationTimestamp
		if ti.Equal(&tj) {
			return templates[i].Name > templates[j].Name
		}
		return ti.After(tj.Time)
	})
}

// BuildStatusOf maps an ActorTemplate phase onto an E2B build status. Substrate
// prepares a golden snapshot before a template can serve actors, so every phase
// short of Ready is still building.
func BuildStatusOf(tmpl *atev1alpha1.ActorTemplate) BuildStatus {
	if tmpl == nil {
		return BuildStatusError
	}
	switch tmpl.Status.Phase {
	case atev1alpha1.PhaseReady:
		return BuildStatusReady
	case atev1alpha1.PhaseFailed:
		return BuildStatusError
	default:
		return BuildStatusBuilding
	}
}

// PoolSelector builds the per-actor placement constraint for a SandboxSet. An
// empty name yields nil, which leaves the actor eligible for every pool the
// template allows.
func PoolSelector(sandboxSetName string) map[string]string {
	if sandboxSetName == "" {
		return nil
	}
	return map[string]string{agentsv1alpha1.LabelSandboxSet: sandboxSetName}
}
