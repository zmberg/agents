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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryMetadataStore(t *testing.T) {
	t.Run("get on an unknown sandbox reports not found", func(t *testing.T) {
		store := NewInMemoryMetadataStore()
		_, err := store.Get("missing")
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrMetadataNotFound))
	})

	t.Run("put then get round-trips", func(t *testing.T) {
		store := NewInMemoryMetadataStore()
		store.Put(&Metadata{SandboxID: "sbx-1", ActorID: "actor-1", Phase: PhaseRunning})

		got, err := store.Get("sbx-1")
		require.NoError(t, err)
		assert.Equal(t, "actor-1", got.ActorID)
		assert.Equal(t, PhaseRunning, got.Phase)
	})

	// Callers must not be able to corrupt stored state through a returned pointer.
	t.Run("get returns a copy", func(t *testing.T) {
		store := NewInMemoryMetadataStore()
		store.Put(&Metadata{SandboxID: "sbx-1", Phase: PhaseRunning})

		got, err := store.Get("sbx-1")
		require.NoError(t, err)
		got.Phase = PhasePaused

		again, err := store.Get("sbx-1")
		require.NoError(t, err)
		assert.Equal(t, PhaseRunning, again.Phase)
	})

	t.Run("put stores a copy of its argument", func(t *testing.T) {
		store := NewInMemoryMetadataStore()
		meta := &Metadata{SandboxID: "sbx-1", Phase: PhaseRunning}
		store.Put(meta)
		meta.Phase = PhaseCrashed

		got, err := store.Get("sbx-1")
		require.NoError(t, err)
		assert.Equal(t, PhaseRunning, got.Phase)
	})

	t.Run("put is a nil-safe no-op", func(t *testing.T) {
		store := NewInMemoryMetadataStore()
		store.Put(nil)
		assert.Empty(t, store.List(ListOptions{}))
	})

	t.Run("delete removes the record", func(t *testing.T) {
		store := NewInMemoryMetadataStore()
		store.Put(&Metadata{SandboxID: "sbx-1"})
		store.Delete("sbx-1")

		_, err := store.Get("sbx-1")
		assert.True(t, errors.Is(err, ErrMetadataNotFound))
	})

	t.Run("delete on an unknown sandbox is a no-op", func(t *testing.T) {
		store := NewInMemoryMetadataStore()
		store.Delete("missing")
	})

	t.Run("update mutates in place and reports the hit", func(t *testing.T) {
		store := NewInMemoryMetadataStore()
		store.Put(&Metadata{SandboxID: "sbx-1", Phase: PhaseRunning})

		found := store.Update("sbx-1", func(m *Metadata) { m.Phase = PhasePaused })
		assert.True(t, found)

		got, err := store.Get("sbx-1")
		require.NoError(t, err)
		assert.Equal(t, PhasePaused, got.Phase)
	})

	t.Run("update on an unknown sandbox reports the miss", func(t *testing.T) {
		store := NewInMemoryMetadataStore()
		called := false
		found := store.Update("missing", func(*Metadata) { called = true })
		assert.False(t, found)
		assert.False(t, called, "mutate must not run when the sandbox is absent")
	})
}

func TestInMemoryMetadataStoreList(t *testing.T) {
	newStore := func() *InMemoryMetadataStore {
		store := NewInMemoryMetadataStore()
		store.Put(&Metadata{SandboxID: "a", Owner: "alice", Namespace: "team-a"})
		store.Put(&Metadata{SandboxID: "b", Owner: "bob", Namespace: "team-a"})
		store.Put(&Metadata{SandboxID: "c", Owner: "alice", Namespace: "team-b"})
		return store
	}

	tests := []struct {
		name string
		opts ListOptions
		want []string
	}{
		{name: "zero value matches everything", opts: ListOptions{}, want: []string{"a", "b", "c"}},
		{name: "filter by owner", opts: ListOptions{Owner: "alice"}, want: []string{"a", "c"}},
		{name: "filter by namespace", opts: ListOptions{Namespace: "team-a"}, want: []string{"a", "b"}},
		{name: "filters are ANDed", opts: ListOptions{Owner: "alice", Namespace: "team-a"}, want: []string{"a"}},
		{name: "no match yields empty", opts: ListOptions{Owner: "carol"}, want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newStore().List(tt.opts)
			ids := make([]string, 0, len(got))
			for _, meta := range got {
				ids = append(ids, meta.SandboxID)
			}
			assert.ElementsMatch(t, tt.want, ids)
		})
	}
}

func TestKeyedLocker(t *testing.T) {
	t.Run("the same key serializes its holders", func(t *testing.T) {
		locker := NewKeyedLocker()
		var mu sync.Mutex
		var order []int
		var wg sync.WaitGroup

		unlockFirst := locker.Lock("actor-1")

		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock := locker.Lock("actor-1")
			defer unlock()
			mu.Lock()
			order = append(order, 2)
			mu.Unlock()
		}()

		// Give the goroutine time to block on the held lock.
		time.Sleep(50 * time.Millisecond)
		mu.Lock()
		order = append(order, 1)
		mu.Unlock()
		unlockFirst()

		wg.Wait()
		assert.Equal(t, []int{1, 2}, order, "the second holder must wait for the first")
	})

	t.Run("different keys do not block each other", func(t *testing.T) {
		locker := NewKeyedLocker()
		unlockA := locker.Lock("actor-a")
		defer unlockA()

		done := make(chan struct{})
		go func() {
			unlockB := locker.Lock("actor-b")
			unlockB()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("a lock on another key must not block")
		}
	})

	// A process that never drops entries would leak one mutex per actor it ever
	// touched, so releasing the last holder must remove the key.
	t.Run("the last release drops the entry", func(t *testing.T) {
		locker := NewKeyedLocker()
		unlock := locker.Lock("actor-1")
		assert.Equal(t, 1, locker.Len())
		unlock()
		assert.Equal(t, 0, locker.Len())
	})

	t.Run("an entry survives while another holder waits", func(t *testing.T) {
		locker := NewKeyedLocker()
		unlockFirst := locker.Lock("actor-1")

		waiting := make(chan func(), 1)
		go func() { waiting <- locker.Lock("actor-1") }()

		// The waiter has registered its reference, so the entry must stay.
		time.Sleep(50 * time.Millisecond)
		assert.Equal(t, 1, locker.Len())

		unlockFirst()
		unlockSecond := <-waiting
		assert.Equal(t, 1, locker.Len())
		unlockSecond()
		assert.Equal(t, 0, locker.Len())
	})

	t.Run("concurrent holders of one key leave no entry behind", func(t *testing.T) {
		locker := NewKeyedLocker()
		var wg sync.WaitGroup
		counter := 0
		for range 50 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				unlock := locker.Lock("actor-1")
				defer unlock()
				counter++
			}()
		}
		wg.Wait()
		assert.Equal(t, 50, counter, "the lock must make the increments atomic")
		assert.Equal(t, 0, locker.Len())
	})
}
