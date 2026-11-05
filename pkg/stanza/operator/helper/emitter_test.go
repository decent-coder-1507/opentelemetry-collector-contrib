// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package helper

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/entry"
)

func TestBatchingLogEmitter(t *testing.T) {
	rwMtx := &sync.RWMutex{}
	var receivedEntries []*entry.Entry
	emitter := NewBatchingLogEmitter(
		componenttest.NewNopTelemetrySettings(),
		func(_ context.Context, entries []*entry.Entry) {
			rwMtx.Lock()
			defer rwMtx.Unlock()
			receivedEntries = entries
		},
	)

	require.NoError(t, emitter.Start(nil))

	defer func() {
		require.NoError(t, emitter.Stop())
	}()

	in := entry.New()

	assert.NoError(t, emitter.Process(t.Context(), in))

	require.Eventually(t, func() bool {
		rwMtx.RLock()
		defer rwMtx.RUnlock()
		return receivedEntries != nil
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, in, receivedEntries[0])
}

func TestBatchingLogEmitterEmitsOnMaxBatchSize(t *testing.T) {
	const (
		maxBatchSize = 100
		timeout      = time.Second
	)
	rwMtx := &sync.RWMutex{}
	var receivedEntries []*entry.Entry
	emitter := NewBatchingLogEmitter(
		componenttest.NewNopTelemetrySettings(),
		func(_ context.Context, entries []*entry.Entry) {
			rwMtx.Lock()
			defer rwMtx.Unlock()
			receivedEntries = entries
		},
	)

	require.NoError(t, emitter.Start(nil))
	defer func() {
		require.NoError(t, emitter.Stop())
	}()

	entries := complexEntries(maxBatchSize)

	ctx := t.Context()
	for _, e := range entries {
		assert.NoError(t, emitter.Process(ctx, e))
	}

	require.Eventually(t, func() bool {
		rwMtx.RLock()
		defer rwMtx.RUnlock()
		return receivedEntries != nil
	}, timeout, 10*time.Millisecond)
	require.Len(t, receivedEntries, maxBatchSize)
}

func TestBatchingLogEmitterEmitsOnFlushInterval(t *testing.T) {
	const (
		flushInterval = 100 * time.Millisecond
		timeout       = time.Second
	)
	rwMtx := &sync.RWMutex{}
	var receivedEntries []*entry.Entry
	emitter := NewBatchingLogEmitter(
		componenttest.NewNopTelemetrySettings(),
		func(_ context.Context, entries []*entry.Entry) {
			rwMtx.Lock()
			defer rwMtx.Unlock()
			receivedEntries = entries
		},
	)
	emitter.flushInterval = flushInterval

	require.NoError(t, emitter.Start(nil))
	defer func() {
		require.NoError(t, emitter.Stop())
	}()

	entry := complexEntry()

	ctx := t.Context()
	assert.NoError(t, emitter.Process(ctx, entry))

	require.Eventually(t, func() bool {
		rwMtx.RLock()
		defer rwMtx.RUnlock()
		return receivedEntries != nil
	}, timeout, 10*time.Millisecond)

	require.Len(t, receivedEntries, 1)
}

// newBatchRecorder returns an emitter that is not started (so no timer flushes happen)
// and a func returning the sizes of the batches flushed so far.
func newBatchRecorder(opts ...EmitterOption) (*BatchingLogEmitter, func() []int) {
	var mtx sync.Mutex
	var batchSizes []int
	emitter := NewBatchingLogEmitter(
		componenttest.NewNopTelemetrySettings(),
		func(_ context.Context, entries []*entry.Entry) {
			mtx.Lock()
			defer mtx.Unlock()
			batchSizes = append(batchSizes, len(entries))
		},
		opts...,
	)
	return emitter, func() []int {
		mtx.Lock()
		defer mtx.Unlock()
		return append([]int(nil), batchSizes...)
	}
}

func entryWithBody(body any) *entry.Entry {
	e := entry.New()
	e.Body = body
	return e
}

func TestBatchingLogEmitterEmitsOnMaxBatchBytes(t *testing.T) {
	emitter, flushed := newBatchRecorder(WithMaxBatchBytes(1000))
	ctx := t.Context()

	body := string(make([]byte, 400))
	require.NoError(t, emitter.Process(ctx, entryWithBody(body)))
	require.NoError(t, emitter.Process(ctx, entryWithBody([]byte(body))))
	require.Empty(t, flushed(), "800 bytes is below the limit")

	require.NoError(t, emitter.Process(ctx, entryWithBody(body)))
	require.Equal(t, []int{3}, flushed(), "1200 bytes reaches the limit")

	// The byte count starts over after a flush.
	require.NoError(t, emitter.Process(ctx, entryWithBody(body)))
	require.NoError(t, emitter.Process(ctx, entryWithBody(body)))
	require.Equal(t, []int{3}, flushed())

	// Non string/[]byte bodies are not counted.
	require.NoError(t, emitter.Process(ctx, entryWithBody(map[string]any{"k": body})))
	require.Equal(t, []int{3}, flushed())

	require.NoError(t, emitter.ProcessBatch(ctx, []*entry.Entry{entryWithBody(body)}))
	require.Equal(t, []int{3, 4}, flushed(), "ProcessBatch counts bytes too")
}

func TestBatchingLogEmitterDefaultMaxBatchBytes(t *testing.T) {
	emitter, flushed := newBatchRecorder()
	ctx := t.Context()

	// Small entries are still batched by count.
	for range 10 {
		require.NoError(t, emitter.Process(ctx, entryWithBody("small log line")))
	}
	require.Empty(t, flushed())

	// A large entry (e.g. a recombined container log) flushes the batch instead of being held.
	require.NoError(t, emitter.Process(ctx, entryWithBody(string(make([]byte, defaultMaxBatchBytes)))))
	require.Equal(t, []int{11}, flushed())
}

func TestBatchingLogEmitterMaxBatchBytesDisabled(t *testing.T) {
	emitter, flushed := newBatchRecorder(WithMaxBatchBytes(0))
	ctx := t.Context()

	for range 5 {
		require.NoError(t, emitter.Process(ctx, entryWithBody(string(make([]byte, defaultMaxBatchBytes)))))
	}
	require.Empty(t, flushed())
}

func TestSynchronousLogEmitter(t *testing.T) {
	rwMtx := &sync.RWMutex{}
	var receivedEntries []*entry.Entry
	emitter := NewSynchronousLogEmitter(
		componenttest.NewNopTelemetrySettings(),
		func(_ context.Context, entries []*entry.Entry) {
			rwMtx.Lock()
			defer rwMtx.Unlock()
			receivedEntries = entries
		},
	)

	require.NoError(t, emitter.Start(nil))

	defer func() {
		require.NoError(t, emitter.Stop())
	}()

	in := entry.New()

	assert.NoError(t, emitter.Process(t.Context(), in))

	require.Eventually(t, func() bool {
		rwMtx.RLock()
		defer rwMtx.RUnlock()
		return receivedEntries != nil
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, in, receivedEntries[0])
}

func complexEntries(count int) []*entry.Entry {
	return complexEntriesForNDifferentHosts(count, 1)
}

func complexEntry() *entry.Entry {
	e := entry.New()
	e.Severity = entry.Error
	e.Resource = map[string]any{
		"bool":   true,
		"int":    123,
		"double": 12.34,
		"string": "hello",
		"object": map[string]any{
			"bool":   true,
			"int":    123,
			"double": 12.34,
			"string": "hello",
		},
	}
	e.Attributes = map[string]any{
		"bool":   true,
		"int":    123,
		"double": 12.34,
		"string": "hello",
		"object": map[string]any{
			"bool":   true,
			"int":    123,
			"double": 12.34,
			"string": "hello",
		},
	}
	e.Body = map[string]any{
		"bool":   true,
		"int":    123,
		"double": 12.34,
		"string": "hello",
		// "bytes":  []byte("asdf"),
		"object": map[string]any{
			"bool":   true,
			"int":    123,
			"double": 12.34,
			"string": "hello",
			// "bytes":  []byte("asdf"),
			"object": map[string]any{
				"bool": true,
				"int":  123,
				// "double": 12.34,
				"string": "hello",
				// "bytes":  []byte("asdf"),
			},
		},
	}
	return e
}

func complexEntriesForNDifferentHosts(count, n int) []*entry.Entry {
	ret := make([]*entry.Entry, count)
	for i := range count {
		e := entry.New()
		e.Severity = entry.Error
		e.Resource = map[string]any{
			"host":   fmt.Sprintf("host-%d", i%n),
			"bool":   true,
			"int":    123,
			"double": 12.34,
			"string": "hello",
			"object": map[string]any{
				"bool":   true,
				"int":    123,
				"double": 12.34,
				"string": "hello",
			},
		}
		e.Body = map[string]any{
			"bool":   true,
			"int":    123,
			"double": 12.34,
			"string": "hello",
			"bytes":  []byte("asdf"),
			"object": map[string]any{
				"bool":   true,
				"int":    123,
				"double": 12.34,
				"string": "hello",
				"bytes":  []byte("asdf"),
				"object": map[string]any{
					"bool":   true,
					"int":    123,
					"double": 12.34,
					"string": "hello",
					"bytes":  []byte("asdf"),
				},
			},
		}
		ret[i] = e
	}
	return ret
}
