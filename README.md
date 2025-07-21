# Go Batch

batch processing utilities for go projects.

This library provides a general batch processor that can apply to various use cases like bulk insert to the database,
bulk enqueue, precompute reports, ...

## Installation

Require go 1.24+

```shell
go get github.com/mawngo/go-batch/v2
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
	// First create a *batch.Processor by specifying the batch initializer and merger.
	//
	// Initializer will be called to create a new batch, 
	// here the batch.InitSlice[int] will create a slice.
	// Merger will be called to add item to a batch, 
	// here the batch.AddToSlice[int] will add item to the slice.
	//
	// A batch can be anything: slice, map, struct, channel, ...
	// The library already defined some built initializers and mergers for common data types,
	// but you can always define your own initializer and merger.
	processor := batch.NewProcessor(batch.InitSlice[int], batch.AddToSlice[int]).
		// Configure the processor.
		// The batch will be processed when the max item is reached 
		// or the max wait is reached.
		Configure(batch.WithMaxConcurrency(5), batch.WithMaxItem(10),
			batch.WithMaxWait(30*time.Second))

	// Start the processor by specifying a handler to process the batch, 
	// and optionally error handlers.
	// This will create a batch.Running processor that can accept item.
	runningProcessor := processor.Run(summing(&sum))

	for i := 0; i < 1_000_000; i++ {
		// Add item to the processor.
		runningProcessor.Put(1)
	}
	// Remember to close the running processor before your application stopped.
	// Closing will force the processor to process the left-over item, 
	// any item added after closing is not guarantee to be processed.
	if err := runningProcessor.Close(); err != nil {
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

More usage can be found in [test](batch_test.go) and [examples](examples)

### Context and Cancellation

This library provides both non-context `XXX` and context `XXXContext` variants.
However, it is recommended to use context variants, as non-context variants can block indefinitely (except for `Close`)

Cancelling the context only affects the item that is waiting to be added to the batch (for example, when the waiting
batch is full and all batch processing threads are busy), there is no way to cancel the item that is already added to
the batch.

You can implement your own logic to cancel the batch using the item context by creating custom batch and item struct
as demonstrated in [custom context control example](examples/ctxctrl/main.go).

### Waiting for an item to be processed

The processor does not provide a method to wait for or get the result of processing an item, however,
you can use the `batch.Future` with custom batch to implement your own waiting logic.

See [future example](examples/future/main.go)

---
There is a [java version of this library](https://github.com/mawngo/batch4j).
