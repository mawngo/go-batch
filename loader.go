package batch

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

var (
	_ ILoader[any, any] = (*Loader[any, any])(nil)
	_ IFuture[any]      = (*Future[any])(nil)
)

// ILoader provides common methods of a [Loader].
type ILoader[K comparable, T any] interface {
	// Get registers a key to be loaded and wait for it to be loaded.
	// This method can block until the loader is available for loading new batch.
	// It is recommended to use [ILoader.GetContext] instead.
	Get(key K) (T, error)
	// GetContext registers a key to be loaded and wait for it to be loaded.
	//
	// Context can be used to provide deadline for this method.
	// Cancel the context may stop the loader from loading the key.
	GetContext(ctx context.Context, key K) (T, error)

	// GetAll registers keys to be loaded and wait for all of them to be loaded.
	// This method can block until the loader is available for loading new batch,
	// and may block indefinitely.
	// It is recommended to use [ILoader.GetAllContext] instead.
	GetAll(keys []K) (map[K]T, error)
	// GetAllContext registers keys to be loaded and wait for all of them to be loaded.
	// Context can be used to provide deadline for this method.
	//
	// In the case of context timed out, keys that are loaded will be returned along with error.
	// Cancel the context may stop the loader from loading the key.
	GetAllContext(ctx context.Context, keys []K) (map[K]T, error)

	// Load registers a key to be loaded and return a [Future] for waiting for the result.
	// This method can block until the loader is available for loading new batch.
	// It is recommended to use [ILoader.LoadContext] instead.
	Load(key K) *Future[T]
	// LoadContext registers a key to be loaded and return a [Future] for waiting for the result.
	//
	// Context can be used to provide deadline for this method.
	// Cancel the context may stop the loader from loading the key.
	LoadContext(ctx context.Context, key K) *Future[T]

	// LoadAll registers keys to be loaded and return a [IFuture] for waiting for the combined result.
	// This method can block until the processor is available for processing new batch,
	// and may block indefinitely.
	// It is recommended to use [ILoader.LoadAllContext] instead.
	LoadAll(keys []K) map[K]*Future[T]
	// LoadAllContext registers keys to be loaded and return a [Future] for waiting for the combined result.
	//
	// Context can be used to provide deadline for this method.
	// Cancel the context may stop the loader from loading the key.
	LoadAllContext(ctx context.Context, keys []K) map[K]*Future[T]

	// Close stop the loader.
	// This method may process the leftover branch on caller thread.
	// The implementation of this method may vary, but it must never wait indefinitely.
	Close() error
	// CloseContext stop the loader.
	// This method may load the left-over batch on caller thread.
	// Context can be used to provide deadline for this method.
	CloseContext(ctx context.Context) error
	// StopContext stop the loader.
	// This method does not load leftover batch.
	StopContext(ctx context.Context) error
	// FlushContext force load the current batch.
	// This method may load the batch on caller thread.
	// Context can be used to provide deadline for this method.
	FlushContext(ctx context.Context) error
	// Flush force load the current batch.
	// This method may load the batch on caller thread.
	// It is recommended to use [ILoader.FlushContext] instead.
	Flush()
}

// LoadKeys is a batch of keys to be loaded.
type LoadKeys[K any] struct {
	Ctx  context.Context
	Keys []K
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
// The result [LoaderSetup] is in setup state.
//
// Call [LoaderSetup.Run] with a handler to create a [Loader] that can load item.
// By default, the processor operates with the following configuration:
// - WithMaxConcurrency: Unset (unlimited)
// - WithMaxItem: 1000 (count keys)
// - WithMaxWait: 16ms.
func NewLoader[K comparable, T any]() LoaderSetup[K, T] {
	s := LoaderSetup[K, T]{
		missingResultError: ErrLoadMissingResult,
	}
	s.setup = NewProcessor[K, LoadKeys[K]](
		func(i int64) LoadKeys[K] {
			return LoadKeys[K]{
				Keys: InitSlice[K](i),
				Ctx:  nil,
			}
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
// You cannot load item using this loader yet, use [LoaderSetup.Run] to create a [Loader] that can load item.
// See [Option] for available options.
type LoaderSetup[K comparable, T any] struct {
	setup              ProcessorSetup[K, LoadKeys[K]]
	missingResultError error
}

// Loader [ILoader] that is running and can load item.
type Loader[K comparable, T any] struct {
	processor *Processor[K, LoadKeys[K]]

	load               LoadBatchFn[K, T]
	missingResultError error

	lock    sync.RWMutex
	loading map[K]*Future[T]
	I       int32
}

// Configure apply [Option] to this loader setup.
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
// Accept a [LoadBatchFn]  and a list of [RunOption] of type [LoadKeys].
func (p LoaderSetup[K, T]) Run(loadFn LoadBatchFn[K, T], options ...RunOption[LoadKeys[K]]) *Loader[K, T] {
	loader := Loader[K, T]{
		loading:            make(map[K]*Future[T]),
		missingResultError: p.missingResultError,
		load:               loadFn,
	}
	options = append([]RunOption[LoadKeys[K]]{WithBatchCounter(func(b LoadKeys[K], _ int64) int64 {
		return int64(len(b.Keys))
	})}, options...)
	loader.processor = p.setup.Run(loader.processLoad, options...)
	return &loader
}

// processLoad is a processor callback that loads a batch of keys.
func (l *Loader[K, T]) processLoad(batch LoadKeys[K], count int64) error {
	if batch.Ctx == nil {
		batch.Ctx = context.Background()
	}

	results, err := l.load(batch, count)
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
// This method can block until the loader is available for loading new batch.
// It is recommended to use [Loader.GetContext] instead.
func (l *Loader[K, T]) Get(key K) (T, error) {
	//nolint:staticcheck
	return l.LoadContext(nil, key).Get()
}

// GetContext registers a key to be loaded and wait for it to be loaded.
//
// Context can be used to provide deadline for this method.
// Cancel the context may stop the loader from loading the key.
func (l *Loader[K, T]) GetContext(ctx context.Context, key K) (T, error) {
	return l.LoadContext(ctx, key).GetContext(ctx)
}

// Load registers a key to be loaded and return a [IFuture] for waiting for the result.
// This method can block until the loader is available for loading new batch.
// It is recommended to use [Loader.LoadContext] instead.
func (l *Loader[K, T]) Load(key K) *Future[T] {
	//nolint:staticcheck
	return l.LoadContext(nil, key)
}

// LoadContext registers a key to be loaded and return a [Future] for waiting for the result.
//
// Context can be used to provide deadline for this method.
// Cancel the context may stop the loader from loading the key.
func (l *Loader[K, T]) LoadContext(ctx context.Context, key K) *Future[T] {
	var res *Future[T]

	l.processor.MergeContext(ctx, key, func(batch LoadKeys[K], _ K) LoadKeys[K] {
		if ctx != nil {
			if ctx.Err() == nil {
				batch.Ctx = ctx
			} else {
				res = &Future[T]{
					err: ctx.Err(),
				}
				return batch
			}
		}

		// Check if the key is already in the batch.
		l.lock.RLock()
		if future, ok := l.loading[key]; ok {
			l.lock.RUnlock()
			res = future
			return batch
		}
		l.lock.RUnlock()

		// Does not need to lock here, as we already ensure that a batch only contains existing keys, and this function
		// is already locked, so no thread will touch the new key.
		future := &Future[T]{
			ch: make(chan struct{}, 1),
		}
		atomic.AddInt32(&l.I, 1)
		batch.Keys = append(batch.Keys, key)
		l.loading[key] = future
		res = future
		return batch
	})
	return res
}

// GetAll registers keys to be loaded and wait for all of them to be loaded.
// This method can block until the loader is available for loading new batch,
// and may block indefinitely.
// It is recommended to use [Loader.GetAllContext] instead.
func (l *Loader[K, T]) GetAll(keys []K) (map[K]T, error) {
	//nolint:staticcheck
	return l.GetAllContext(nil, keys)
}

// GetAllContext registers keys to be loaded and wait for all of them to be loaded.
// Context can be used to provide deadline for this method.
//
// In the case of context timed out, keys that are loaded will be returned along with error.
// Cancel the context may stop the loader from loading the key.
func (l *Loader[K, T]) GetAllContext(ctx context.Context, keys []K) (map[K]T, error) {
	futures := l.LoadAllContext(ctx, keys)

	errs := make([]error, 0, 10)
	result := make(map[K]T, len(keys))
	for k, future := range futures {
		v, err := future.GetContext(ctx)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		result[k] = v
	}
	return result, errors.Join(errs...)
}

// LoadAll registers keys to be loaded and return a [IFuture] for waiting for the combined result.
// This method can block until the processor is available for processing new batch,
// and may block indefinitely.
// It is recommended to use [Loader.LoadAllContext] instead.
func (l *Loader[K, T]) LoadAll(keys []K) map[K]*Future[T] {
	//nolint:staticcheck
	return l.LoadAllContext(nil, keys)
}

// LoadAllContext registers keys to be loaded and return a [IFuture] for waiting for the combined result.
//
// Context can be used to provide deadline for this method.
// Cancel the context may stop the loader from loading the key.
func (l *Loader[K, T]) LoadAllContext(ctx context.Context, keys []K) map[K]*Future[T] {
	futures := make(map[K]*Future[T], len(keys))

	l.processor.MergeAllContext(ctx, keys, func(batch LoadKeys[K], key K) LoadKeys[K] {
		if ctx != nil {
			if ctx.Err() == nil {
				batch.Ctx = ctx
				futures[key] = &Future[T]{
					err: ctx.Err(),
				}
			} else {
				return batch
			}
		}

		// Check if the key is already in the batch.
		l.lock.RLock()
		if future, ok := l.loading[key]; ok {
			l.lock.RUnlock()
			futures[key] = future
			return batch
		}
		l.lock.RUnlock()

		// Does not need to lock here, as we already ensure that a batch only contains existing keys, and this function
		// is already locked, so no thread will touch the new key.
		future := &Future[T]{
			ch: make(chan struct{}, 1),
		}
		batch.Keys = append(batch.Keys, key)
		l.loading[key] = future
		futures[key] = future
		return batch
	})

	return futures
}

// Close stop the loader.
// This method will process the leftover branch on caller thread.
func (l *Loader[K, T]) Close() error {
	return l.processor.Close()
}

// CloseContext stop the loader.
// This method may load the left-over batch on caller thread.
// Context can be used to provide deadline for this method.
func (l *Loader[K, T]) CloseContext(ctx context.Context) error {
	return l.processor.CloseContext(ctx)
}

// StopContext stop the loader.
// This method does not load leftover batch.
func (l *Loader[K, T]) StopContext(ctx context.Context) error {
	err := l.processor.StopContext(ctx)
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
// This method may load the batch on caller thread.
// It is recommended to use [Loader.FlushContext] instead.
func (l *Loader[K, T]) Flush() {
	l.processor.Flush()
}

// FlushContext force load the current batch.
// This method may load the batch on caller thread.
// Context can be used to provide deadline for this method.
func (l *Loader[K, T]) FlushContext(ctx context.Context) error {
	return l.processor.FlushContext(ctx)
}

// Future implements [IFuture].
// Can be used to get the result of a load request.
type Future[T any] struct {
	// ch is used to notify that the future is completed.
	ch chan struct{}

	result T
	err    error
}

// Get wait until the result is available and return the result.
// This method can block indefinitely.
// It is recommended to use [Future.GetContext] instead.
func (r *Future[T]) Get() (T, error) {
	//nolint:staticcheck
	return r.GetContext(nil)
}

// GetContext wait until the result is available and return the result.
// The context can be used to cancel the wait (not the load request).
func (r *Future[T]) GetContext(ctx context.Context) (T, error) {
	if r.ch == nil {
		return r.result, r.err
	}

	if ctx != nil {
		select {
		case <-r.ch:
			// Don't care, the result is already set.
		case <-ctx.Done():
			// Return the context error.
			return r.result, ctx.Err()
		}
	} else {
		<-r.ch
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
