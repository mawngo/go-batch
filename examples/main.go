package main

import (
	"context"
	"github.com/mawngo/go-batch/v3"
	"sync/atomic"
	"time"
)

func main() {
	sum := int32(0)
	// First create a batch.ProcessorSetup by specifying the batch initializer and merger.
	//
	// Initializer will be called to create a new batch, here the batch.InitSlice[int] will create a slice.
	// Merger will be called to add item to a batch, here the addToSlice[int] will add item to the slice.
	//
	// A batch can be anything: slice, map, struct, channel, ...
	// The library already defined some built initializers and mergers for common data types,
	// but you can always define your own initializer and merger.
	//
	// This equals to: batch.NewSliceProcessor().
	setup := batch.NewProcessor(batch.InitSlice[int], addToSlice[int]).
		// Configure the processor.
		// The batch will be processed when the max item is reached or the max wait is reached.
		Configure(batch.WithMaxConcurrency(5), batch.WithMaxItem(10), batch.WithMaxWait(30*time.Second))

	// Start the processor by specifying a handler to process the batch, and optionally additional run configuration.
	// This will create a *batch.Processor that can accept item.
	processor := setup.Run(summing(&sum))

	ctx := context.Background()
	for i := 0; i < 1_000_000; i++ {
		// Add item to the processor.
		processor.Put(ctx, 1)
	}
	// Remember to close the running processor before your application stopped.
	// Closing will force the processor to process the left-over item,
	// any item added after closing is not guarantee to be processed.
	if err := processor.Close(ctx); err != nil {
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

// addToSlice is [batch.MergeToBatchFn] that add item to a slice.
// It is recommended to use [batch.NewSliceProcessor], this is just for example.
func addToSlice[T any](b []T, item T) []T {
	return append(b, item)
}
