package main

import (
	"context"
	"github.com/mawngo/go-batch"
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
		Configure(batch.WithMaxConcurrency(5), batch.WithMaxItem(10)).
		Run(summing(&sum))
	for i := 0; i < 1_000_000; i++ {
		processor.Put(itemWithContext{
			// Pass your context in here.
			ctx:  context.Background(),
			item: 1,
		})
	}
	if err := processor.Close(); err != nil {
		panic(err)
	}
	if sum != 1_000_000 {
		panic("sum is not 1_000_000")
	}
}

func summing(p *int32) batch.ProcessBatchFn[*batchWithContext] {
	return func(b *batchWithContext, _ int64) error {
		// do something with context.
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
