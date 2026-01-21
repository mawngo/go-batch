package main

import (
	"context"
	"github.com/mawngo/go-batch/v3"
	"sync/atomic"
)

func main() {
	sum := int32(0)
	processor := batch.NewProcessor(
		func(i int64) *batchWithContext {
			return &batchWithContext{
				lastCtx: context.Background(),
				batch:   batch.InitSlice[int](i),
			}
		},
		func(b *batchWithContext, t itemWithContext) *batchWithContext {
			// Set the batch context to the context of the last item.
			b.lastCtx = t.ctx
			b.batch = append(b.batch, t.item)
			return b
		},
	).
		Configure(
			batch.WithMaxWait(batch.Unset),
			batch.WithMaxConcurrency(5),
			batch.WithMaxItem(10)).
		Run(summing(&sum))
	for range 1_000_000 {
		processor.Put(context.Background(), itemWithContext{
			// This context will be passed to the batch process function and used to cancel the processing.
			ctx:  context.Background(),
			item: 1,
		})
	}
	if err := processor.Close(context.Background()); err != nil {
		panic(err)
	}
	if sum != 1_000_000 {
		panic("sum is not 1_000_000")
	}
}

func summing(p *int32) batch.ProcessBatchFn[*batchWithContext] {
	return func(b *batchWithContext, _ int64) error {
		// do something with context.
		if b.lastCtx.Err() != nil {
			return b.lastCtx.Err()
		}

		for _, num := range b.batch {
			atomic.AddInt32(p, int32(num))
		}
		return nil
	}
}

type batchWithContext struct {
	lastCtx context.Context
	batch   []int
}

type itemWithContext struct {
	ctx  context.Context
	item int
}
