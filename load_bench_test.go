package batch

import (
	"context"
	"testing"
)

func BenchmarkLoad(b *testing.B) {
	b.Run("LoadContext default", func(b *testing.B) {
		touched := int32(0)
		loader := NewLoader[int, int]().
			Run(loadMapInt1(&touched), WithBatchLoaderCountInput[int]())
		for b.Loop() {
			b.StopTimer()
			ctx := context.Background()
			waits := make([]*LoadFuture[int], 0, 100_000)
			sum := 0
			b.StartTimer()

			for i := 0; i < 100_000; i++ {
				waits = append(waits, loader.LoadContext(ctx, 1))
			}
			for _, future := range waits {
				v, _ := future.GetContext(ctx)
				sum += v
			}
			if sum != 100_000 {
				b.Fatalf("sum is %d != 100_000", sum)
			}
		}
		if err := loader.Close(); err != nil {
			panic(err)
		}
		println(touched)
	})
}
