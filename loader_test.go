package batch

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBatchedLoad(t *testing.T) {
	defer VerifyNoGoroutineLeak(t)

	testLoad := func(t *testing.T, loader ILoader[int, int], touched *int32) {
		ctx := context.Background()
		loadings := make([]*Future[int], 0, 55_000)
		sum := 0

		for range 50_000 {
			loadings = append(loadings, loader.Load(ctx, 1))
		}

		for range 1_000 {
			go func() {
				loader.Load(ctx, 1)
			}()
		}

		for _, loading := range loadings {
			v, _ := loading.Get(ctx)
			sum += v
		}

		if err := loader.Close(ctx); err != nil {
			panic(err)
		}
		if sum != 50_000 {
			t.Fatalf("sum is %d != 50_000", touched)
		}
		if *touched > 1 {
			t.Fatalf("touched too many time %d > 1", touched)
		}
	}

	t.Run("Load", func(t *testing.T) {
		touched := int32(0)
		loader := NewLoader[int, int]().
			Configure(WithMaxItem(50_000+1000+1), WithMaxWait(1*time.Second)).
			Run(loadMapInt1(&touched))
		testLoad(t, loader, &touched)
	})

	t.Run("Load (Keys)", func(t *testing.T) {
		touched := int32(0)
		loader := NewLoader[int, int]().
			Configure(WithMaxItem(10), WithMaxWait(1*time.Second)).
			Run(loadMapInt1(&touched), WithLoaderCountKeys[int]())
		testLoad(t, loader, &touched)
	})

	testLoadAll := func(t *testing.T, loader ILoader[int, int], touched *int32) {
		ctx := context.Background()
		loadings := make([]map[int]*Future[int], 0, 55_000)
		sum := 0

		for range 50_000 {
			loadings = append(loadings, loader.LoadAll(ctx, []int{1, 2, 3}))
		}

		for range 1_000 {
			go func() {
				loader.LoadAll(ctx, []int{1, 2, 3})
			}()
		}

		for _, m := range loadings {
			for _, loading := range m {
				v, _ := loading.Get(ctx)
				sum += v
			}
		}

		if err := loader.Close(ctx); err != nil {
			panic(err)
		}
		if sum != 150_000 {
			t.Fatalf("sum is %d != 150_000", touched)
		}
		if *touched > 1 {
			t.Fatalf("touched too many time %d > 1", touched)
		}
	}
	t.Run("LoadAll", func(t *testing.T) {
		touched := int32(0)
		loader := NewLoader[int, int]().
			Configure(WithMaxItem(50_000*3+1000*3+1), WithMaxWait(1*time.Second)).
			Run(loadMapInt1(&touched))
		testLoadAll(t, loader, &touched)
	})

	t.Run("LoadAll (Keys)", func(t *testing.T) {
		touched := int32(0)
		loader := NewLoader[int, int]().
			Configure(WithMaxItem(10), WithMaxWait(1*time.Second)).
			Run(loadMapInt1(&touched), WithLoaderCountKeys[int]())
		testLoadAll(t, loader, &touched)
	})
}

func TestBatchedLoadCancel(t *testing.T) {
	defer VerifyNoGoroutineLeak(t)

	loader := NewLoader[int, int]().
		Configure(WithMaxItem(100), WithMaxWait(Unset)).
		Run(func(batch LoadKeys[int], _ int64) (map[int]int, error) {
			<-batch.Ctx.Done()
			return nil, batch.Ctx.Err()
		}, WithLoaderCountKeys[int]())

	ctx, cancel := context.WithCancel(context.Background())
	loadings := make([]*Future[int], 0, 15_000)

	for range 10_000 {
		loadings = append(loadings, loader.Load(ctx, 1))
	}

	for range 1_000 {
		m := loader.LoadAll(ctx, []int{1, 2})
		for _, loading := range m {
			loadings = append(loadings, loading)
		}
	}
	cancel()
	err := loader.Flush(context.Background())
	if err != nil {
		t.Fatalf("error flushing")
	}

	for _, loading := range loadings {
		_, err := loading.Get(context.Background())
		if err == nil {
			t.Fatalf("missing error when cancelled")
		}
	}
	if err := loader.Close(context.Background()); err != nil {
		panic(err)
	}
}

func TestBatchedLoadReuse(t *testing.T) {
	defer VerifyNoGoroutineLeak(t)

	touched := int32(0)
	loader := NewLoader[int, int]().
		Configure(WithMaxItem(100), WithMaxWait(Unset)).
		Run(func(_ LoadKeys[int], _ int64) (map[int]int, error) {
			atomic.AddInt32(&touched, 1)
			return nil, nil
		}, WithLoaderCountKeys[int]())

	ctx := context.Background()
	loading1 := loader.Load(ctx, 1)
	loading2 := loader.Load(ctx, 2)

	if loader.Load(ctx, 1) != loading1 {
		t.Fatalf("loading not reuse pending future")
	}

	if loader.Load(ctx, 2) != loading2 {
		t.Fatalf("loading not reuse pending future")
	}

	for range 1_000 {
		m := loader.LoadAll(ctx, []int{1, 2})
		if m[1] != loading1 {
			t.Fatalf("loading not reuse pending future")
		}
		if m[2] != loading2 {
			t.Fatalf("loading not reuse pending future")
		}
	}

	wg := sync.WaitGroup{}
	for range 1_000 {
		wg.Go(func() {
			if loader.Load(ctx, 1) != loading1 {
				panic("loading not reuse pending future " + strconv.Itoa(int(touched)))
			}

			if loader.Load(ctx, 2) != loading2 {
				panic("loading not reuse pending future " + strconv.Itoa(int(touched)))
			}
		})
	}
	wg.Wait()

	for range 1_000 {
		wg.Go(func() {
			m := loader.LoadAll(ctx, []int{1, 2})
			if m[1] != loading1 {
				panic("loading not reuse pending future " + strconv.Itoa(int(touched)))
			}
			if m[2] != loading2 {
				panic("loading not reuse pending future" + strconv.Itoa(int(touched)))
			}
		})
	}
	wg.Wait()
	if err := loader.Close(ctx); err != nil {
		panic(err)
	}
	if touched != 1 {
		t.Fatalf("touched too many time %d > 1", touched)
	}
}

func TestBatchedLoadDisabled(t *testing.T) {
	defer VerifyNoGoroutineLeak(t)

	touched := int32(0)
	loader := NewLoader[int, int]().
		Configure(WithMaxItem(0), WithMaxWait(Unset)).
		Run(loadMapInt1(&touched), WithLoaderCountKeys[int]())

	ctx := context.Background()
	sum := 0

	for range 10_000 {
		future := loader.Load(ctx, 1)
		if !future.IsDone() {
			t.Fatalf("async load when disabled")
		}
		v, _ := future.Get(ctx)
		sum += v
	}

	for range 5_000 {
		futures := loader.LoadAll(ctx, []int{1, 2})
		for _, future := range futures {
			if !future.IsDone() {
				t.Fatalf("async load when disabled")
			}
			v, _ := future.Get(ctx)
			sum += v
		}
	}

	if err := loader.Close(ctx); err != nil {
		panic(err)
	}
	if touched != 15_000 {
		t.Fatalf("touched not match %d != 15_000", touched)
	}
	if sum != 20_000 {
		t.Fatalf("sum is %d != 20_000", touched)
	}
}

func TestStopContext(t *testing.T) {
	defer VerifyNoGoroutineLeak(t)

	touched := int32(0)
	loader := NewLoader[int, int]().
		Configure(WithMaxItem(10), WithMaxWait(Unset)).
		Run(loadMapInt1(&touched), WithLoaderCountKeys[int]())

	ctx := context.Background()
	loadings := make([]*Future[int], 0, 10_000)
	for range 10_000 {
		loadings = append(loadings, loader.Load(ctx, 1))
	}
	if touched > 0 {
		t.Fatalf("touched when limit not reached")
	}
	if err := loader.Stop(ctx); err != nil {
		panic(err)
	}
	for _, loading := range loadings {
		if !loading.IsDone() {
			t.Fatalf("not complete after stopped")
		}
		_, err := loading.Get(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("invalid error when stop %v", err)
		}
	}
	if touched > 0 {
		t.Fatalf("touched when calling stop")
	}
}

func loadMapInt1(cnt *int32) LoadBatchFn[int, int] {
	return func(batch LoadKeys[int], _ int64) (map[int]int, error) {
		atomic.AddInt32(cnt, 1)
		res := make(map[int]int, len(batch.Keys))
		for _, k := range batch.Keys {
			res[k] = 1
		}
		return res, nil
	}
}

// TestLoaderConcurrentMapWrite previously, we have a concurrent map writing race bug,
// so all those tests below are for testing the bug fix.
func TestLoaderConcurrentMapWrite(t *testing.T) {
	t.Run("Race", func(t *testing.T) {
		loader := NewLoader[int, int]().
			Configure(WithMaxItem(1000), WithMaxWait(Unset)).
			Run(func(batch LoadKeys[int], _ int64) (map[int]int, error) {
				res := make(map[int]int, len(batch.Keys))
				for _, k := range batch.Keys {
					res[k] = k
				}
				return res, nil
			})

		ctx := context.Background()
		var key atomic.Int64
		var wg sync.WaitGroup
		for range 16 {
			wg.Go(func() {
				for range 100_000 {
					_ = loader.Load(ctx, int(key.Add(1)))
				}
			})
		}
		wg.Wait()
		_ = loader.Close(ctx)
	})

	t.Run("Panic", func(t *testing.T) {
		const n = 500_000
		loader := NewLoader[int, int]().
			Configure(WithMaxItem(n), WithMaxWait(Unset)).
			Run(func(LoadKeys[int], int64) (map[int]int, error) {
				return nil, nil // fast loadFn -> delete loop is the dominant window
			})

		ctx := context.Background()

		stop := make(chan struct{})
		producerDone := make(chan struct{})
		var key atomic.Int64
		go func() {
			defer close(producerDone)
			for {
				select {
				case <-stop:
					return
				default:
					_ = loader.Load(ctx, int(key.Add(1))+n) // fresh keys, never collide with the batch
				}
			}
		}()

		keys := make([]int, n)
		for i := range keys {
			keys[i] = i
		}
		loader.LoadAll(ctx, keys) // fills the 500k batch -> dispatch -> long delete loop

		close(stop)
		<-producerDone
		_ = loader.Close(ctx)
	})
}
