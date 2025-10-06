# Go Batch

batch processing utilities for go projects.

This library provides a general batch processor that can apply to various use cases like bulk insert to the database,
bulk enqueue, precompute reports, ...

## Installation

Require go 1.24+

```shell
go get -u github.com/mawngo/go-batch/v2
```

## Usage

### Example

```go
package main

import (
	"github.com/mawngo/go-batch/v2"
	"sync/atomic"
	"time"
)

func main() {
	sum := int32(0)
	// First create a batch.ProcessorSetup by specifying 
	// the batch initializer and merger.
	//
	// Initializer will be called to create a new batch, 
	// here the batch.InitSlice[int] will create a slice.
	// Merger will be called to add item to a batch, 
	// here the batch.AddToSlice[int] will add item to the slice.
	//
	// A batch can be anything: slice, map, struct, channel, ...
	// The library already defined some built-in 
	// initializers and mergers for common data types,
	// but you can always define your own initializer and merger.
	//
	// This equals to: batch.NewSliceProcessor().
	setup := batch.NewProcessor(batch.InitSlice[int], batch.AddToSlice[int]).
		// Configure the processor.
		// The batch will be processed when the max item is reached 
		// or the max wait is reached.
		Configure(batch.WithMaxConcurrency(5),
			batch.WithMaxItem(10), batch.WithMaxWait(30*time.Second))

	// Start the processor by specifying a handler to process the batch,
	// and optionally additional run configuration.
	// This will create a *batch.Processor that can accept item.
	processor := setup.Run(summing(&sum))

	for i := 0; i < 1_000_000; i++ {
		// Add item to the processor.
		processor.Put(1)
	}
	// Remember to close the processor before your application stopped.
	// Closing will force the processor to process the left-over item, 
	// any item added after closing is not guarantee to be processed.
	if err := processor.Close(); err != nil {
		panic(err)
	}
	if sum != 1_000_000 {
		panic("sum is not 1_000_000")
	}
}

func summing(p *int32) batch.ProcessBatchFn[[]int] {
	return func(ints []int, _ int64) error {
		for _, num := range ints {
			atomic.AddInt32(p, int32(num))
		}
		return nil
	}
}
```

There are also built-in shortcuts for common processor `NewSliceProcessor`, `NewMapProcessor`, `NewSelfMapProcessor`.
For simple use cases, you can use those shortcuts to avoid boilerplate code, also those functions are unlikely to be
changed in the future major version.

More usage can be found in [test](batch_test.go) and [examples](examples)

### Context and Cancellation

This library provides both non-context `XXX` and context `XXXContext` variants.
However, it is recommended to use context variants, as non-context variants can block indefinitely (except for `Close`).
Non-context variants will be removed in the next major version of this library.

Cancelling the context only affects the item that is waiting to be added to the batch (for example, when the waiting
batch is full and all batch processing threads are busy), there is no way to cancel the item that is already added to
the batch.

You can implement your own logic to cancel the batch using the item context by creating custom batch and item struct
as demonstrated in [custom context control example](examples/ctxctrl/main.go).

### Waiting for an item to be processed

The processor does not provide a method to wait for or get the result of processing an item, however,
you can use the `batch.IFuture[T]` with custom batch to implement your own waiting logic.

See [future example](examples/future/main.go) or [loader implementation](loader.go).

## Loader

Provide batch loading like Facebook's DataLoader.

```go
package main

import (
	"github.com/mawngo/go-batch/v2"
	"strconv"
	"sync/atomic"
	"time"
)

func main() {
	loadedCount := int32(0)
	// First create a batch.LoaderSetup
	loader := batch.NewLoader[int, string]().
		// Configure the loader.
		// All pending load requests will be processed when one of the 
		// following limits is reached.
		Configure(batch.WithMaxItem(100), batch.WithMaxWait(1*time.Second)).
		// Like when using the processor,
		// start the loader by providing a LoadBatchFn,
		// and optionally additional run configuration.
		// This will create a *batch.Loader that can accept item.
		Run(load(&loadedCount))

	for i := 0; i < 100_000; i++ {
		k := i % 10
		go func() {
			// Use the loader.
			// Alternately, you can use the Load method
			// future := loader.Load(k)
			// ...
			// v, err := future.Get()
			v, err := loader.Get(k)

			if err != nil {
				panic(err)
			}
			if v != strconv.Itoa(k) {
				panic("key mismatch")
			}
		}()
	}
	// Remember to close the running load before your application stopped.
	// Closing will force the loader to load the left-over request,
	// any load request after the loader is closed is not guarantee 
	// to be processed, and may block forever.
	if err := loader.Close(); err != nil {
		panic(err)
	}
	// If you do not want to load left over request, then use StopContext instead.
	// if err := loader.StopContext(context.Background()); err != nil {
	//     panic(err)
	// }
	if loadedCount > 1 {
		panic("loaded too many time")
	}
}

func load(p *int32) batch.LoadBatchFn[int, string] {
	return func(batch batch.LoadKeys[int], count int64) (map[int]string, error) {
		atomic.AddInt32(p, 1)
		if len(batch.Keys) == 0 {
			// This could happen if you provide an alternate counting method.
			return nil, nil
		}

		res := make(map[int]string, len(batch.Keys))
		for _, k := range batch.Keys {
			res[k] = strconv.Itoa(k)
		}
		return res, nil
	}
}
```

The `batch.Loader` use `batch.Processor` for handling batching, so they share the same configuration and options.

However, the default configuration of the Loader is different:

- Default max item is `1000`
- Default wait time is `16ms`
- Default concurrency is unlimited.

Since version 2.5, the loader counts the number of pending load requests instead of pending keys for limit. To restore
old behavior, pass `WithLoaderCountKeys()` to run config.

### Caching

This library does not provide caching.
You can implement caching by simply checking the cache before `Load` and add item
to the cache in the `LoadBatchFn`

All `Load` request before and during load of the same key will share the same `Future`.
Multiple `LoadBatchFn` can be run concurrently, but they will never share the same keys sets.

See [loader cache example](examples/loadercache/main.go).

### Cancellation

This library provides both non-context `XXX` and context `XXXContext` variants.
However, it is recommended to use context variants, as non-context variants can block indefinitely (except for `Close`).
Non-context variants will be removed in the next major version of this library.

Cancelling the context may* only affect the request that is waiting to be loaded.

The last context provided to `Load` or `LoadAll` before the batch load is started will be used as the batch context.

---
There is a [java version of this library](https://github.com/mawngo/batch4j).
