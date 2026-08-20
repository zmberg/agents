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
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"

	agentsv1alpha1 "github.com/openkruise/agents/api/v1alpha1"
	"github.com/openkruise/agents/pkg/sandbox-manager/infra"
)

// InfraBuilder assembles a substrate Infra. It satisfies infra.Builder so the
// sandbox-manager builder can treat it like any other backend.
type InfraBuilder struct {
	instance *Infra
}

var _ infra.Builder = (*InfraBuilder)(nil)

// NewInfraBuilder starts a builder with the in-memory store and keyed locker in
// place; the control client and template reader are supplied through options.
func NewInfraBuilder() *InfraBuilder {
	return &InfraBuilder{
		instance: &Infra{
			store:                NewInMemoryMetadataStore(),
			locks:                NewKeyedLocker(),
			routes:               newRouteSource(),
			defaultHibernateMode: agentsv1alpha1.HibernateModeSuspend,
		},
	}
}

// WithControlClient sets the Substrate control-plane client.
func (b *InfraBuilder) WithControlClient(c *Client) *InfraBuilder {
	b.instance.control = c.Control()
	return b
}

// WithTemplateReader sets the reader used to resolve ActorTemplates. It should
// read through a live client so templates built after startup are visible.
func (b *InfraBuilder) WithTemplateReader(reader client.Reader) *InfraBuilder {
	b.instance.templates = NewTemplateResolver(reader)
	return b
}

// WithDefaultHibernateMode overrides the mode used when a claim resolves no
// pool-specific hibernate mode.
func (b *InfraBuilder) WithDefaultHibernateMode(mode string) *InfraBuilder {
	if mode == agentsv1alpha1.HibernateModePause || mode == agentsv1alpha1.HibernateModeSuspend {
		b.instance.defaultHibernateMode = mode
	}
	return b
}

// Build validates the assembled Infra and returns it.
func (b *InfraBuilder) Build() infra.Infrastructure {
	if b.instance.control == nil {
		panic("substrate infra requires a control client; call WithControlClient")
	}
	if b.instance.templates == nil {
		panic("substrate infra requires a template reader; call WithTemplateReader")
	}
	return b.instance
}

// Validate reports whether the builder has the collaborators Build requires,
// so callers can fail assembly with an error instead of a panic.
func (b *InfraBuilder) Validate() error {
	if b.instance.control == nil {
		return fmt.Errorf("substrate infra requires a control client")
	}
	if b.instance.templates == nil {
		return fmt.Errorf("substrate infra requires a template reader")
	}
	return nil
}
