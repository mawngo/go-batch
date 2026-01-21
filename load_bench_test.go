package batch

import (
	"context"
	"testing"
)

func BenchmarkLoad(b *testing.B) {
	b.Run("Load default", func(b *testing.B) {
		touched := int32(0)
		loader := NewLoader[int, int]().
			Run(loadMapInt1(&touched))
		for b.Loop() {
			b.StopTimer()
			ctx := context.Background()
			waits := make([]*Future[int], 0, 100_000)
			sum := 0
			b.StartTimer()

			for range 100_000 {
				waits = append(waits, loader.Load(ctx, 1))
			}
			for _, future := range waits {
				v, _ := future.Get(ctx)
				sum += v
			}
			if sum != 100_000 {
				b.Fatalf("sum is %d != 100_000", sum)
			}
		}
		if err := loader.Close(context.Background()); err != nil {
			panic(err)
		}
		println(touched)
	})
}
