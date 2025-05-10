package batch

import (
	"context"
	"testing"
	"time"
)

func BenchmarkPutFree(b *testing.B) {
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(Unlimited), WithBlockWhileProcessing(), WithDisabledDefaultProcessErrorLog(), WithMaxWait(100*time.Hour)).
		Run(func(ints []int, _ int64) error {
			return nil
		})
	b.Run("put", func(b *testing.B) {
		for b.Loop() {
			processor.Put(1)
		}
	})
	processor.MustClose()
}

func BenchmarkPutContextFree(b *testing.B) {
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(Unlimited), WithBlockWhileProcessing(), WithDisabledDefaultProcessErrorLog(), WithMaxWait(100*time.Hour)).
		Run(func(ints []int, _ int64) error {
			return nil
		})
	ctx := context.Background()
	b.Run("put", func(b *testing.B) {
		for b.Loop() {
			processor.PutContext(ctx, 1)
		}
	})
	processor.MustClose()
}
