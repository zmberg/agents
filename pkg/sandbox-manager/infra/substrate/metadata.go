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
	"sync"
	"time"

	"github.com/openkruise/agents/pkg/sandboxroute"
	"github.com/openkruise/agents/pkg/utils/timeout"
)

// Sandbox phases reported to the E2B API. They mirror the actor statuses that
// callers can observe; transient substrate statuses collapse onto these.
const (
	PhaseRunning   = "running"
	PhasePaused    = "paused"
	PhaseSuspended = "suspended"
	PhaseResuming  = "resuming"
	PhaseCrashed   = "crashed"
)

// Metadata is the per-sandbox state that a Sandbox CR would otherwise hold.
// The substrate backend creates no CR, so sandbox-manager owns this record and
// it is the only mapping from a public sandbox ID to its actor.
type Metadata struct {
	SandboxID string
	ActorID   string
	// Atespace is the substrate isolation boundary, always the sandbox namespace.
	Atespace          string
	Namespace         string
	ActorTemplateName string
	// SandboxSetName records the pool the caller selected, empty when the actor
	// may land on any pool in the namespace.
	SandboxSetName string
	Owner          string
	Route          sandboxroute.Route
	Phase          string
	Timeout        timeout.Options
	CreateTime     time.Time
	LastActiveTime time.Time
	HibernateMode  string
}

// ListOptions filters a metadata listing. A zero value matches everything.
type ListOptions struct {
	Owner     string
	Namespace string
}

// MetadataStore keeps the sandbox ID to actor mapping.
//
// Implementations must return copies so that callers cannot mutate stored state
// through a returned pointer.
type MetadataStore interface {
	Get(sandboxID string) (*Metadata, error)
	Put(meta *Metadata)
	Delete(sandboxID string)
	List(opts ListOptions) []*Metadata
	// Update applies mutate to the stored record under the store's lock. It
	// reports whether the sandbox was found.
	Update(sandboxID string, mutate func(*Metadata)) bool
}

// ErrMetadataNotFound reports an unknown sandbox ID.
var ErrMetadataNotFound = fmt.Errorf("sandbox metadata not found")

// InMemoryMetadataStore holds metadata for the lifetime of the process.
//
// Restarting sandbox-manager loses every mapping, which orphans the running
// actors: they keep consuming workers but can no longer be reached or reaped
// through the E2B API. Reconciling those orphans requires listing actors from
// substrate on startup, which this store deliberately does not attempt.
type InMemoryMetadataStore struct {
	mu    sync.RWMutex
	store map[string]*Metadata
}

var _ MetadataStore = (*InMemoryMetadataStore)(nil)

// NewInMemoryMetadataStore returns an empty store.
func NewInMemoryMetadataStore() *InMemoryMetadataStore {
	return &InMemoryMetadataStore{store: make(map[string]*Metadata)}
}

func (s *InMemoryMetadataStore) Get(sandboxID string) (*Metadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	meta, ok := s.store[sandboxID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrMetadataNotFound, sandboxID)
	}
	clone := *meta
	return &clone, nil
}

func (s *InMemoryMetadataStore) Put(meta *Metadata) {
	if meta == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	clone := *meta
	s.store[meta.SandboxID] = &clone
}

func (s *InMemoryMetadataStore) Delete(sandboxID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.store, sandboxID)
}

func (s *InMemoryMetadataStore) List(opts ListOptions) []*Metadata {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*Metadata, 0, len(s.store))
	for _, meta := range s.store {
		if opts.Owner != "" && meta.Owner != opts.Owner {
			continue
		}
		if opts.Namespace != "" && meta.Namespace != opts.Namespace {
			continue
		}
		clone := *meta
		result = append(result, &clone)
	}
	return result
}

func (s *InMemoryMetadataStore) Update(sandboxID string, mutate func(*Metadata)) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	meta, ok := s.store[sandboxID]
	if !ok {
		return false
	}
	mutate(meta)
	return true
}

// KeyedLocker serializes operations that share a key, so concurrent lifecycle
// calls on one actor cannot interleave.
//
// Entries are reference counted and dropped once the last holder releases them,
// which keeps a long-lived process from accumulating a mutex per actor ever
// created.
type KeyedLocker struct {
	mu    sync.Mutex
	locks map[string]*refCountedMutex
}

type refCountedMutex struct {
	mu   sync.Mutex
	refs int
}

// NewKeyedLocker returns an empty locker.
func NewKeyedLocker() *KeyedLocker {
	return &KeyedLocker{locks: make(map[string]*refCountedMutex)}
}

// Lock acquires the mutex for key and returns the matching unlock function.
func (l *KeyedLocker) Lock(key string) func() {
	l.mu.Lock()
	entry, ok := l.locks[key]
	if !ok {
		entry = &refCountedMutex{}
		l.locks[key] = entry
	}
	entry.refs++
	l.mu.Unlock()

	entry.mu.Lock()

	return func() {
		entry.mu.Unlock()

		l.mu.Lock()
		defer l.mu.Unlock()
		entry.refs--
		if entry.refs == 0 {
			delete(l.locks, key)
		}
	}
}

// Len reports how many keys are currently held or waited on. It exists for
// tests and debug output.
func (l *KeyedLocker) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.locks)
}
