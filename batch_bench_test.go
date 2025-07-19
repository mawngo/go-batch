package batch

import (
	"context"
	"testing"
	"time"
)

func BenchmarkPut(b *testing.B) {
	b.Run("PutContext free1", func(b *testing.B) {
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(Unset), WithBlockWhileProcessing(), WithDisabledDefaultProcessErrorLog(), WithMaxWait(100*time.Hour)).
			Run(func(_ []int, _ int64) error {
				return nil
			})
		ctx := context.Background()
		for b.Loop() {
			processor.PutContext(ctx, 1)
		}
		processor.MustClose()
	})

	b.Run("PutContext free3", func(b *testing.B) {
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(Unset), WithBlockWhileProcessing(), WithDisabledDefaultProcessErrorLog(), WithMaxWait(100*time.Hour)).
			Run(func(_ []int, _ int64) error {
				return nil
			})
		ctx := context.Background()
		for b.Loop() {
			processor.PutContext(ctx, 1)
			processor.PutContext(ctx, 1)
			processor.PutContext(ctx, 1)
		}
		processor.MustClose()
	})

	b.Run("Put free1", func(b *testing.B) {
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(Unset), WithBlockWhileProcessing(), WithDisabledDefaultProcessErrorLog(), WithMaxWait(100*time.Hour)).
			Run(func(_ []int, _ int64) error {
				return nil
			})
		for b.Loop() {
			processor.Put(1)
		}
		processor.MustClose()
	})

	b.Run("Put free3", func(b *testing.B) {
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(Unset), WithBlockWhileProcessing(), WithDisabledDefaultProcessErrorLog(), WithMaxWait(100*time.Hour)).
			Run(func(_ []int, _ int64) error {
				return nil
			})
		for b.Loop() {
			processor.Put(1)
			processor.Put(1)
			processor.Put(1)
		}
		processor.MustClose()
	})

	b.Run("PutContext full1", func(b *testing.B) {
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(1), WithMaxConcurrency(Unset), WithDisabledDefaultProcessErrorLog(), WithMaxWait(100*time.Hour)).
			Run(func(_ []int, _ int64) error {
				return nil
			})
		ctx := context.Background()
		for b.Loop() {
			processor.PutContext(ctx, 1)
		}
		processor.MustClose()
	})

	b.Run("PutContext full3", func(b *testing.B) {
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(1), WithMaxConcurrency(Unset), WithDisabledDefaultProcessErrorLog(), WithMaxWait(100*time.Hour)).
			Run(func(_ []int, _ int64) error {
				return nil
			})
		ctx := context.Background()
		for b.Loop() {
			processor.PutContext(ctx, 1)
			processor.PutContext(ctx, 1)
			processor.PutContext(ctx, 1)
		}
		processor.MustClose()
	})

	b.Run("Put full1", func(b *testing.B) {
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(1), WithMaxConcurrency(Unset), WithDisabledDefaultProcessErrorLog(), WithMaxWait(100*time.Hour)).
			Run(func(_ []int, _ int64) error {
				return nil
			})
		for b.Loop() {
			processor.Put(1)
		}
		processor.MustClose()
	})

	b.Run("Put full3", func(b *testing.B) {
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(1), WithMaxConcurrency(Unset), WithDisabledDefaultProcessErrorLog(), WithMaxWait(100*time.Hour)).
			Run(func(_ []int, _ int64) error {
				return nil
			})
		for b.Loop() {
			processor.Put(1)
			processor.Put(1)
			processor.Put(1)
		}
		processor.MustClose()
	})
}

func BenchmarkPutAll(b *testing.B) {
	b.Run("PutAll free3", func(b *testing.B) {
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(Unset), WithBlockWhileProcessing(), WithDisabledDefaultProcessErrorLog(), WithMaxWait(100*time.Hour)).
			Run(func(_ []int, _ int64) error {
				return nil
			})
		for b.Loop() {
			processor.PutAll([]int{1, 1, 1})
		}
		processor.MustClose()
	})

	b.Run("PutAllContext free3", func(b *testing.B) {
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(Unset), WithBlockWhileProcessing(), WithDisabledDefaultProcessErrorLog(), WithMaxWait(100*time.Hour)).
			Run(func(_ []int, _ int64) error {
				return nil
			})
		ctx := context.Background()
		for b.Loop() {
			processor.PutAllContext(ctx, []int{1, 1, 1})
		}
		processor.MustClose()
	})

	b.Run("PutAllContext full3", func(b *testing.B) {
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(1), WithMaxConcurrency(Unset), WithDisabledDefaultProcessErrorLog(), WithMaxWait(100*time.Hour)).
			Run(func(_ []int, _ int64) error {
				return nil
			})
		ctx := context.Background()
		for b.Loop() {
			processor.PutAllContext(ctx, []int{1, 1, 1})
		}
		processor.MustClose()
	})
}

func BenchmarkPutUnderLoad(b *testing.B) {
	b.Run("PutContext 1000x100x4", func(b *testing.B) {
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
				processor.PutContext(ctx, 1)
			}
		}
		processor.MustClose()
	})

	b.Run("PutContext 1000x100", func(b *testing.B) {
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
				processor.PutContext(ctx, 1)
			}
		}
		processor.MustClose()
	})

	b.Run("Put 1000x100x4", func(b *testing.B) {
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(100), WithBlockWhileProcessing(),
				WithMaxConcurrency(4),
				WithMaxWait(Unset),
				WithDisabledDefaultProcessErrorLog()).
			Run(func(_ []int, _ int64) error {
				return nil
			})
		for b.Loop() {
			for i := 0; i < 10_000; i++ {
				processor.Put(1)
			}
		}
		processor.MustClose()
	})

	b.Run("Put 1000x100", func(b *testing.B) {
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(100), WithBlockWhileProcessing(),
				WithMaxWait(Unset),
				WithDisabledDefaultProcessErrorLog()).
			Run(func(_ []int, _ int64) error {
				return nil
			})
		for b.Loop() {
			for i := 0; i < 10_000; i++ {
				processor.Put(1)
			}
		}
		processor.MustClose()
	})
}
