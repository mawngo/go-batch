package batch

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

var (
	_ ILoader[any, any] = (*Loader[any, any])(nil)
	_ IFuture[any]      = (*Future[any])(nil)
)

// IFuture is a future that can be used to get the result of a task.
type IFuture[T any] interface {
	// Get wait until the result is available.
	// The context can be used to cancel the wait (not the task).
	Get(ctx context.Context) (T, error)
}

// ILoader provides common methods of a [Loader].
type ILoader[K comparable, T any] interface {
	// Get registers a key to be loaded and wait for it to be loaded.
	//
	// Context can be used to provide a deadline for this method.
	// Cancel the context may stop the loader from loading the key.
	Get(ctx context.Context, key K) (T, error)

	// GetAll registers keys to be loaded and wait for all of them to be loaded.
	// Context can be used to provide a deadline for this method.
	//
	// In the case of context timed out, keys that are loaded will be returned along with an error.
	// Cancel the context may stop the loader from loading the key.
	GetAll(ctx context.Context, keys []K) (map[K]T, error)

	// Load registers a key to be loaded and return a [Future] for waiting for the result.
	//
	// Context can be used to provide a deadline for this method.
	// Cancel the context may stop the loader from loading the key.
	Load(ctx context.Context, key K) *Future[T]

	// LoadAll registers keys to be loaded and return a [Future] for waiting for the combined result.
	//
	// Context can be used to provide a deadline for this method.
	// Cancel the context may stop the loader from loading the key.
	LoadAll(ctx context.Context, keys []K) map[K]*Future[T]

	// Close stop the loader.
	// This method may load the left-over batch on the caller thread.
	// Context can be used to provide a deadline for this method.
	Close(ctx context.Context) error
	// Stop the loader.
	// This method does not load the leftover batch.
	Stop(ctx context.Context) error
	// Flush force load the current batch.
	// This method may load the batch on the caller thread.
	// Context can be used to provide a deadline for this method.
	Flush(ctx context.Context) error
}

// LoadKeys is a batch of keys to be loaded.
type LoadKeys[K any] struct {
	Ctx  context.Context
	Keys []K

	// counter is the number of pending load request of this batch before processing.
	counter int64
	// id is the unique id of this batch.
	id *byte
}

// LoadBatchFn function to load a batch of keys.
// Return a map of keys to values and an error, if both are non-nil, the error will be set for all missing keys.
// If the result does not contain all keys and the error is nil, the error of missing keys
// will be set to [ErrLoadMissingResult] or any configured error.
//
// The [LoadBatchFn] should not modify the key content.
type LoadBatchFn[K comparable, T any] func(batch LoadKeys[K], count int64) (map[K]T, error)

// ErrLoadMissingResult is the default error for keys that are missing in the result.
// Can be configured by [LoaderSetup.WithMissingResultError].
var ErrLoadMissingResult = errors.New("empty missing result for key")

// NewLoader create a LoaderSetup using specified functions.
// See [LoaderSetup.Configure] and [Option] for available configuration.
// The result [LoaderSetup] is in the setup state.
//
// Call [LoaderSetup.Run] with a handler to create a [Loader] that can load item.
// By default, the processor operates with the following configuration:
//   - WithMaxConcurrency: Unset (unlimited)
//   - WithMaxItem: 1000 (count keys)
//   - WithMaxWait: 16ms.
func NewLoader[K comparable, T any]() LoaderSetup[K, T] {
	s := LoaderSetup[K, T]{
		missingResultError: ErrLoadMissingResult,
	}
	s.setup = NewProcessor[K, LoadKeys[K]](
		func(i int64) LoadKeys[K] {
			var id byte
			b := LoadKeys[K]{
				Keys: InitSlice[K](i),
				Ctx:  nil,
				id:   &id,
			}
			return b
		},
		func(_ LoadKeys[K], _ K) LoadKeys[K] {
			// Disable Put* usage.
			panic("use Merge and MergeAll instead of Put")
		}).
		Configure(
			WithMaxConcurrency(Unset),
			WithMaxWait(16*time.Millisecond),
			WithMaxItem(1000),
		)
	return s
}

// LoaderSetup batch loader that is in setup phase (not running)
// You cannot load any item using this loader yet, use [LoaderSetup.Run] to create a [Loader] that can load item.
// See [Option] for available options.
type LoaderSetup[K comparable, T any] struct {
	setup              ProcessorSetup[K, LoadKeys[K]]
	missingResultError error
}

// Loader [ILoader] that is running and can load item.
type Loader[K comparable, T any] struct {
	processor *Processor[K, LoadKeys[K]]

	loadFn             LoadBatchFn[K, T]
	missingResultError error

	lock    sync.RWMutex
	loading map[K]*Future[T]
}

// Configure applies [Option] to this loader setup.
func (p LoaderSetup[K, T]) Configure(options ...Option) LoaderSetup[K, T] {
	p.setup = p.setup.Configure(options...)
	return p
}

// WithMissingResultError set the default error for keys that are missing in the result.
func (p LoaderSetup[K, T]) WithMissingResultError(err error) LoaderSetup[K, T] {
	p.missingResultError = err
	return p
}

// Run create a [Loader] that can accept item.
// Accept a [LoadBatchFn] and a list of [RunOption] of type [LoadKeys].
func (p LoaderSetup[K, T]) Run(loadFn LoadBatchFn[K, T], options ...RunOption[LoadKeys[K]]) *Loader[K, T] {
	loader := Loader[K, T]{
		loading:            make(map[K]*Future[T]),
		missingResultError: p.missingResultError,
		loadFn:             loadFn,
	}
	// By default, using the batch counter that counts the total pending request of that batch.
	countFn := func(batch LoadKeys[K]) int64 { return batch.counter }
	options = append([]RunOption[LoadKeys[K]]{WithBatchCounter(countFn)}, options...)
	loader.processor = p.setup.Run(loader.processLoad, options...)
	return &loader
}

// processLoad is a processor callback that loads a batch of keys.
func (l *Loader[K, T]) processLoad(batch LoadKeys[K], count int64) error {
	if batch.Ctx == nil {
		batch.Ctx = context.Background()
	}

	results, err := l.loadFn(batch, count)
	if err == nil {
		err = l.missingResultError
	}

	if len(batch.Keys) == 0 {
		return nil
	}

	l.lock.Lock()
	defer l.lock.Unlock()
	for _, key := range batch.Keys {
		future, ok := l.loading[key]
		if !ok {
			slog.Error("key removed before load", slog.Any("key", key))
			continue
		}
		delete(l.loading, key)
		if v, ok := results[key]; ok {
			future.result = v
		} else {
			future.err = err
		}
		close(future.ch)
		future.ch = nil
	}
	return nil
}

// Get registers a key to be loaded and wait for it to be loaded.
//
// Context can be used to provide a deadline for this method.
// Cancel the context may stop the loader from loading the key.
func (l *Loader[K, T]) Get(ctx context.Context, key K) (T, error) {
	return l.Load(ctx, key).Get(ctx)
}

// Load registers a key to be loaded and return a [Future] for waiting for the result.
//
// Context can be used to provide a deadline for this method.
// Cancel the context may stop the loader from loading the key.
func (l *Loader[K, T]) Load(ctx context.Context, key K) *Future[T] {
	var res *Future[T]

	l.processor.Merge(ctx, key, func(batch LoadKeys[K], _ K) LoadKeys[K] {
		if ctx.Err() == nil {
			batch.Ctx = ctx
		} else {
			res = &Future[T]{
				err: ctx.Err(),
			}
			return batch
		}

		// Check if the key is already in the batch.
		l.lock.RLock()
		if future, ok := l.loading[key]; ok {
			l.lock.RUnlock()
			res = future
			if future.batchID == batch.id {
				batch.counter++
			}
			return batch
		}
		l.lock.RUnlock()

		// Does not need to lock here, as we already ensure that a batch only contains existing keys, and this function
		// is already locked, so no thread will touch the new key.
		future := &Future[T]{
			ch:      make(chan struct{}, 1),
			batchID: batch.id,
		}
		batch.Keys = append(batch.Keys, key)
		batch.counter++
		l.loading[key] = future
		res = future
		return batch
	})
	return res
}

// GetAll registers keys to be loaded and wait for all of them to be loaded.
// Context can be used to provide a deadline for this method.
//
// In the case of context timed out, keys that are loaded will be returned along with error.
// Cancel the context may stop the loader from loading the key.
// The result map may not contain all keys if the context is canceled.
func (l *Loader[K, T]) GetAll(ctx context.Context, keys []K) (map[K]T, error) {
	futures := l.LoadAll(ctx, keys)

	errs := make([]error, 0, 10)
	result := make(map[K]T, len(keys))
	for k, future := range futures {
		v, err := future.Get(ctx)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		result[k] = v
	}
	return result, errors.Join(errs...)
}

// LoadAll registers keys to be loaded and return a [IFuture] for waiting for the combined result.
//
// Context can be used to provide a deadline for this method.
// Cancel the context may stop the loader from loading the key.
// The result map may not contain all keys if the context is canceled.
func (l *Loader[K, T]) LoadAll(ctx context.Context, keys []K) map[K]*Future[T] {
	futures := make(map[K]*Future[T], len(keys))

	l.processor.MergeAll(ctx, keys, func(batch LoadKeys[K], key K) LoadKeys[K] {
		if ctx.Err() == nil {
			batch.Ctx = ctx
		} else {
			futures[key] = &Future[T]{
				err: ctx.Err(),
			}
			return batch
		}

		// Check if the key is already in the batch.
		l.lock.RLock()
		if future, ok := l.loading[key]; ok {
			l.lock.RUnlock()
			futures[key] = future
			if future.batchID == batch.id {
				batch.counter++
			}
			return batch
		}
		l.lock.RUnlock()

		// Does not need to lock here, as we already ensure that a batch only contains existing keys, and this function
		// is already locked, so no thread will touch the new key.
		future := &Future[T]{
			ch:      make(chan struct{}, 1),
			batchID: batch.id,
		}
		batch.Keys = append(batch.Keys, key)
		batch.counter++
		l.loading[key] = future
		futures[key] = future
		return batch
	})

	return futures
}

// Close stop the loader.
// This method will load the left-over batch on the caller thread.
//
// Context can be used to provide a deadline for this method,
// Context does not affect already in processing batch.
func (l *Loader[K, T]) Close(ctx context.Context) error {
	return l.processor.Close(ctx)
}

// Stop the loader.
// This method does not load the leftover batch.
func (l *Loader[K, T]) Stop(ctx context.Context) error {
	err := l.processor.Stop(ctx)
	l.lock.Lock()
	defer l.lock.Unlock()
	for _, future := range l.loading {
		future.err = context.Canceled
		close(future.ch)
		future.ch = nil
	}
	clear(l.loading)
	return err
}

// Flush force load the current batch.
// This method may load the pending batch on the caller thread, depending on concurrent and block settings.
// Context can be used to provide a deadline for this method.
func (l *Loader[K, T]) Flush(ctx context.Context) error {
	return l.processor.Flush(ctx)
}

// Future implements [IFuture].
// Can be used to get the result of a load request.
type Future[T any] struct {
	// ch is used to notify that the future is completed.
	ch chan struct{}
	// batch id is used to identify the id of the batch that loading this future.
	batchID *byte

	result T
	err    error
}

// Get wait until the result is available and return the result.
// The context can be used to cancel the wait (not the load request).
func (r *Future[T]) Get(ctx context.Context) (T, error) {
	if r.ch == nil {
		return r.result, r.err
	}

	select {
	case <-r.ch:
		// Don't care, the result is already set.
	case <-ctx.Done():
		// Return the context error.
		return r.result, ctx.Err()
	}

	return r.result, r.err
}

// IsDone return whether the future is completed.
func (r *Future[T]) IsDone() bool {
	if r.ch == nil {
		return true
	}

	select {
	case <-r.ch:
		return true
	default:
		return false
	}
}
