package batch

import (
	"context"
	"testing"
	"time"
)

func BenchmarkPut1Free(b *testing.B) {
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(Unlimited), WithBlockWhileProcessing(), WithDisabledDefaultProcessErrorLog(), WithMaxWait(100*time.Hour)).
		Run(func(_ []int, _ int64) error {
			return nil
		})
	b.Run("put", func(b *testing.B) {
		for b.Loop() {
			processor.Put(1)
		}
	})
	processor.MustClose()
}

func BenchmarkPutContext1Free(b *testing.B) {
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(Unlimited), WithBlockWhileProcessing(), WithDisabledDefaultProcessErrorLog(), WithMaxWait(100*time.Hour)).
		Run(func(_ []int, _ int64) error {
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

func BenchmarkPut3Free(b *testing.B) {
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(Unlimited), WithBlockWhileProcessing(), WithDisabledDefaultProcessErrorLog(), WithMaxWait(100*time.Hour)).
		Run(func(_ []int, _ int64) error {
			return nil
		})
	b.Run("put", func(b *testing.B) {
		for b.Loop() {
			processor.Put(1)
			processor.Put(1)
			processor.Put(1)
		}
	})
	processor.MustClose()
}

func BenchmarkPutAll3Free(b *testing.B) {
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(Unlimited), WithBlockWhileProcessing(), WithDisabledDefaultProcessErrorLog(), WithMaxWait(100*time.Hour)).
		Run(func(_ []int, _ int64) error {
			return nil
		})
	b.Run("put", func(b *testing.B) {
		for b.Loop() {
			processor.PutAll([]int{1, 1, 1})
		}
	})
	processor.MustClose()
}

func BenchmarkPutContext3Free(b *testing.B) {
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(Unlimited), WithBlockWhileProcessing(), WithDisabledDefaultProcessErrorLog(), WithMaxWait(100*time.Hour)).
		Run(func(_ []int, _ int64) error {
			return nil
		})
	ctx := context.Background()
	b.Run("put", func(b *testing.B) {
		for b.Loop() {
			processor.PutContext(ctx, 1)
			processor.PutContext(ctx, 1)
			processor.PutContext(ctx, 1)
		}
	})
	processor.MustClose()
}

func BenchmarkPutAllContext3Free(b *testing.B) {
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(Unlimited), WithBlockWhileProcessing(), WithDisabledDefaultProcessErrorLog(), WithMaxWait(100*time.Hour)).
		Run(func(_ []int, _ int64) error {
			return nil
		})
	ctx := context.Background()
	b.Run("put", func(b *testing.B) {
		for b.Loop() {
			processor.PutAllContext(ctx, []int{1, 1, 1})
		}
	})
	processor.MustClose()
}
