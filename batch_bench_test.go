package batch

import (
	"context"
	"testing"
	"time"
)

func BenchmarkPut(b *testing.B) {
	b.Run("Put free1", func(b *testing.B) {
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(Unset), WithBlockWhileProcessing(), WithDisabledDefaultProcessErrorLog(), WithMaxWait(100*time.Hour)).
			Run(func(_ []int, _ int64) error {
				return nil
			})
		ctx := context.Background()
		for b.Loop() {
			processor.Put(ctx, 1)
		}
		if err := processor.Close(ctx); err != nil {
			b.Fatalf("error closing processor: %v", err)
		}
	})

	b.Run("Put free3", func(b *testing.B) {
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(Unset), WithBlockWhileProcessing(), WithDisabledDefaultProcessErrorLog(), WithMaxWait(100*time.Hour)).
			Run(func(_ []int, _ int64) error {
				return nil
			})
		ctx := context.Background()
		for b.Loop() {
			processor.Put(ctx, 1)
			processor.Put(ctx, 1)
			processor.Put(ctx, 1)
		}
		if err := processor.Close(ctx); err != nil {
			b.Fatalf("error closing processor: %v", err)
		}
	})

	b.Run("Put full1", func(b *testing.B) {
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(1), WithMaxConcurrency(Unset), WithDisabledDefaultProcessErrorLog(), WithMaxWait(100*time.Hour)).
			Run(func(_ []int, _ int64) error {
				return nil
			})
		ctx := context.Background()
		for b.Loop() {
			processor.Put(ctx, 1)
		}
		if err := processor.Close(ctx); err != nil {
			b.Fatalf("error closing processor: %v", err)
		}
	})

	b.Run("Put full3", func(b *testing.B) {
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(1), WithMaxConcurrency(Unset), WithDisabledDefaultProcessErrorLog(), WithMaxWait(100*time.Hour)).
			Run(func(_ []int, _ int64) error {
				return nil
			})
		ctx := context.Background()
		for b.Loop() {
			processor.Put(ctx, 1)
			processor.Put(ctx, 1)
			processor.Put(ctx, 1)
		}
		if err := processor.Close(ctx); err != nil {
			b.Fatalf("error closing processor: %v", err)
		}
	})
}

func BenchmarkPutAll(b *testing.B) {
	b.Run("PutAll free3", func(b *testing.B) {
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(Unset), WithBlockWhileProcessing(), WithDisabledDefaultProcessErrorLog(), WithMaxWait(100*time.Hour)).
			Run(func(_ []int, _ int64) error {
				return nil
			})
		ctx := context.Background()
		for b.Loop() {
			processor.PutAll(ctx, []int{1, 1, 1})
		}
		if err := processor.Close(ctx); err != nil {
			b.Fatalf("error closing processor: %v", err)
		}
	})

	b.Run("PutAll full3", func(b *testing.B) {
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(1), WithMaxConcurrency(Unset), WithDisabledDefaultProcessErrorLog(), WithMaxWait(100*time.Hour)).
			Run(func(_ []int, _ int64) error {
				return nil
			})
		ctx := context.Background()
		for b.Loop() {
			processor.PutAll(ctx, []int{1, 1, 1})
		}
		if err := processor.Close(ctx); err != nil {
			b.Fatalf("error closing processor: %v", err)
		}
	})
}

func BenchmarkPutUnderLoad(b *testing.B) {
	b.Run("Put 1000x100x4", func(b *testing.B) {
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(100), WithBlockWhileProcessing(),
				WithMaxConcurrency(4),
				WithMaxWait(Unset),
				WithDisabledDefaultProcessErrorLog()).
			Run(func(_ []int, _ int64) error {
				return nil
			})
		ctx := context.Background()
		for b.Loop() {
			for i := 0; i < 10_000; i++ {
				processor.Put(ctx, 1)
			}
		}
		if err := processor.Close(ctx); err != nil {
			b.Fatalf("error closing processor: %v", err)
		}
	})

	b.Run("Put 1000x100", func(b *testing.B) {
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(100), WithBlockWhileProcessing(),
				WithMaxWait(Unset),
				WithDisabledDefaultProcessErrorLog()).
			Run(func(_ []int, _ int64) error {
				return nil
			})
		ctx := context.Background()
		for b.Loop() {
			for i := 0; i < 10_000; i++ {
				processor.Put(ctx, 1)
			}
		}
		if err := processor.Close(ctx); err != nil {
			b.Fatalf("error closing processor: %v", err)
		}
	})
}
