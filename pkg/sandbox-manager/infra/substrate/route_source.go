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
	"sync"

	"github.com/openkruise/agents/pkg/sandbox-manager/infra"
	"github.com/openkruise/agents/pkg/sandboxroute"
)

// routeSource fans a single subscriber the route changes that the infra emits
// as actors move. Substrate exposes no informer, so unlike the CR backend this
// source is push-driven: SubstrateInfra calls publish/withdraw and the source
// forwards to whoever subscribed for the process lifetime.
type routeSource struct {
	mu      sync.Mutex
	ctx     context.Context
	handler infra.SandboxRouteEventHandler
}

var _ infra.SandboxRouteSource = (*routeSource)(nil)

func newRouteSource() *routeSource { return &routeSource{} }

// Subscribe records the single lifetime subscriber. A second subscription is a
// programming error because the manager subscribes exactly once.
func (s *routeSource) Subscribe(ctx context.Context, handler infra.SandboxRouteEventHandler) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.handler != nil {
		return fmt.Errorf("substrate route source already has a subscriber")
	}
	s.ctx = ctx
	s.handler = handler
	return nil
}

// publish emits an observation for a sandbox that now has a route.
func (s *routeSource) publish(sbx infra.Sandbox) {
	s.mu.Lock()
	handler, ctx := s.handler, s.ctx
	s.mu.Unlock()
	if handler == nil {
		return
	}
	handler(ctx, infra.SandboxRouteEvent{Sandbox: sbx})
}

// withdraw emits a deletion so the gateway drops the sandbox's route.
func (s *routeSource) withdraw(route sandboxroute.Route) {
	s.mu.Lock()
	handler, ctx := s.handler, s.ctx
	s.mu.Unlock()
	if handler == nil {
		return
	}
	handler(ctx, infra.SandboxRouteEvent{Delete: &route})
}
