package main

import (
	"context"
	"github.com/mawngo/go-batch/v2"
	"sync/atomic"
)

func main() {
	sum := int32(0)
	processor := batch.NewSliceProcessor[int]().
		Configure(batch.WithMaxConcurrency(5), batch.WithMaxItem(10)).
		Run(summing(&sum))

	ctx := context.Background()
	for i := 0; i < 1_000_000; i++ {
		processor.Put(ctx, 1)
	}
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
