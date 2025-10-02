package batch

import (
	"context"
	"errors"
	"go.uber.org/goleak"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// This test will leak as the processor is forced to close before tasks completed, which was an expected behavior.
		goleak.IgnoreAnyFunction("github.com/mawngo/go-batch/v2.TestCloseCancelContext.func1"),
		goleak.IgnoreAnyFunction("github.com/mawngo/go-batch/v2.TestClusterCloseContext.func2"),
	)
}

func new1x10Slice() []int {
	return []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
}

func TestBatched(t *testing.T) {
	t.Run("Batched Default", func(t *testing.T) {
		sum := int32(0)
		processor := NewSliceProcessor[int]().
			Run(summing(&sum))
		runSumPutTest(t, processor, &sum)
	})

	t.Run("Batched Simple", func(t *testing.T) {
		sum := int32(0)
		processor := NewSliceProcessor[int]().
			Configure(WithMaxItem(10)).
			Run(summing(&sum))
		runSumPutTest(t, processor, &sum)
	})

	t.Run("Batched (Disabled)", func(t *testing.T) {
		sum := int32(0)
		processor := NewSliceProcessor[int]().
			Configure(WithMaxItem(0)).
			Run(summing(&sum))
		runSumPutTest(t, processor, &sum)
	})

	t.Run("Batched (Disabled) (NoWait)", func(t *testing.T) {
		sum := int32(0)
		processor := NewSliceProcessor[int]().
			Configure(WithMaxItem(0), WithMaxWait(Unset)).
			Run(summing(&sum))
		runSumPutTest(t, processor, &sum)
	})

	t.Run("Batched (Disabled) (NoWait) (Concurrent)", func(t *testing.T) {
		sum := int32(0)
		processor := NewSliceProcessor[int]().
			Configure(WithMaxItem(0), WithMaxWait(Unset), WithMaxConcurrency(10)).
			Run(summing(&sum))
		runSumPutTest(t, processor, &sum)
	})

	t.Run("Batched (Aggressive)", func(t *testing.T) {
		sum := int32(0)
		processor := NewSliceProcessor[int]().
			Configure(WithMaxItem(10), WithAggressiveMode()).
			Run(summing(&sum))
		runSumPutTest(t, processor, &sum)
	})

	t.Run("Batched (Aggressive) (Concurrent)", func(t *testing.T) {
		sum := int32(0)
		processor := NewSliceProcessor[int]().
			Configure(WithMaxItem(10), WithAggressiveMode(), WithMaxConcurrency(10)).
			Run(summing(&sum))
		runSumPutTest(t, processor, &sum)
	})

	t.Run("Batched (BlockWhileProcessing)", func(t *testing.T) {
		sum := int32(0)
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithBlockWhileProcessing(), WithMaxItem(10)).
			Run(summing(&sum))
		runSumPutTest(t, processor, &sum)
	})

	t.Run("Batched (BlockWhileProcessing) (Concurrent)", func(t *testing.T) {
		sum := int32(0)
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithBlockWhileProcessing(), WithMaxItem(10), WithMaxConcurrency(10)).
			Run(summing(&sum))
		runSumPutTest(t, processor, &sum)
	})

	t.Run("Batched (Concurrent)", func(t *testing.T) {
		sum := int32(0)
		processor := NewSliceProcessor[int]().
			Configure(WithMaxConcurrency(10), WithMaxItem(10)).
			Run(summing(&sum))
		runSumPutTest(t, processor, &sum)
	})

	t.Run("Batched (NoWait)", func(t *testing.T) {
		sum := int32(0)
		processor := NewSliceProcessor[int]().
			Configure(WithMaxItem(10), WithMaxWait(0)).
			Run(summing(&sum))
		runSumPutTest(t, processor, &sum)
	})

	t.Run("Batched (NoWait) (Concurrent)", func(t *testing.T) {
		sum := int32(0)
		processor := NewSliceProcessor[int]().
			Configure(WithMaxConcurrency(10), WithMaxItem(10), WithMaxWait(0)).
			Run(summing(&sum))
		runSumPutTest(t, processor, &sum)
	})
}

func runSumPutTest(t *testing.T, processor IProcessor[int, []int], sum *int32) {
	ctx := context.Background()
	for i := 0; i < 500_000; i++ {
		processor.Put(ctx, 1)
	}
	for i := 0; i < 50_000; i++ {
		processor.PutAll(ctx, new1x10Slice())
	}
	if err := processor.Close(ctx); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
	if *sum != 1_000_000 {
		t.Fatalf("sum is %d != 1_000_000", sum)
	}
}

func TestBatchedMultiGoroutine(t *testing.T) {
	sum := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(10)).
		Run(summing(&sum))

	ctx := context.Background()
	wg := sync.WaitGroup{}
	for i := 0; i < 500_000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			processor.Put(ctx, 1)
		}()
	}
	for i := 0; i < 50_000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			processor.PutAll(ctx, new1x10Slice())
		}()
	}
	if err := processor.Close(ctx); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
	wg.Wait()
	if sum != 1_000_000 {
		t.Fatalf("sum is %d != 2_000_000", sum)
	}
}

func TestBatchedSplit(t *testing.T) {
	maxes := []int{1, 2, 5, 8, 10, 18, 20, 111}
	for _, m := range maxes {
		sum := int32(0)
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(10)).
			Run(summing(&sum), WithBatchSplitter(SplitSliceEqually[int](m)))

		ctx := context.Background()
		for i := 0; i < 50_000; i++ {
			processor.Put(ctx, 1)
		}
		for i := 0; i < 5_000; i++ {
			processor.PutAll(ctx, new1x10Slice())
		}
		if err := processor.Close(ctx); err != nil {
			t.Fatalf("error closing processor: %v", err)
		}
		if sum != 100_000 {
			t.Fatalf("sum is %d != 100_000", sum)
		}
	}
}

func TestCloseCancelContext(t *testing.T) {
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(5), WithMaxConcurrency(Unset)).
		Run(func(_ []int, _ int64) error {
			time.Sleep(100 * time.Second)
			return nil
		})

	for i := 0; i < 10; i++ {
		processor.Put(context.Background(), 1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := processor.Close(ctx)
	if err == nil {
		t.Fatalf("processor is closed before complete processing!")
	}
}

func TestDrain(t *testing.T) {
	sum := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(11), WithBlockWhileProcessing(), WithMaxWait(Unset)).
		Run(summing(&sum))

	for i := 0; i < 10; i++ {
		processor.Put(context.Background(), 1)
	}
	if sum != 0 {
		t.Fatalf("item processed while limit not reached")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := processor.Drain(ctx)
	if err != nil {
		t.Fatalf("drain result in an error %v", err)
	}
	if sum != 10 {
		t.Fatalf("drain not processing remaining item")
	}
	if err := processor.Close(context.Background()); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
}

func TestFlush(t *testing.T) {
	sum := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(11), WithBlockWhileProcessing(), WithMaxWait(Unset)).
		Run(summing(&sum))

	for i := 0; i < 10; i++ {
		processor.Put(context.Background(), 1)
	}
	if sum != 0 {
		t.Fatalf("item processed while limit not reached")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := processor.Flush(ctx)
	if err != nil {
		t.Fatalf("flush result in an error %v", err)
	}
	if sum != 10 {
		t.Fatalf("flush not processing remaining item")
	}
	if err := processor.Close(context.Background()); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
}

func TestBatchedWait(t *testing.T) {
	t.Run("Wait", func(t *testing.T) {
		sum := int32(0)
		processor := NewSliceProcessor[int]().
			Configure(WithMaxItem(10), WithMaxWait(500*time.Millisecond)).
			Run(summing(&sum))
		ctx := context.Background()
		for i := 0; i < 9; i++ {
			processor.Put(ctx, 1)
		}
		time.Sleep(1 * time.Second)
		if cnt, ok := processor.ItemCount(ctx); ok && cnt != 0 {
			t.Fatalf("item is not processed after timeout passed")
		}
		if sum != 9 {
			t.Fatalf("sum is %d != 9", sum)
		}
		if err := processor.Close(context.Background()); err != nil {
			t.Fatalf("error closing processor: %v", err)
		}
	})

	t.Run("SoftWait", func(t *testing.T) {
		sum := int32(0)
		processor := NewSliceProcessor[int]().
			Configure(WithMaxWait(500 * time.Millisecond)).
			Run(func(ints []int, _ int64) error {
				if len(ints) > 0 {
					t.Fatalf("TestBatchedSoftWait must not put any item to processor")
				}
				atomic.AddInt32(&sum, 1)
				return nil
			})
		time.Sleep(1 * time.Second)
		if sum != 0 {
			t.Fatalf("soft wait process still run even when the batch is empty")
		}
		if err := processor.Close(context.Background()); err != nil {
			t.Fatalf("error closing processor: %v", err)
		}
	})

	t.Run("HardWait", func(t *testing.T) {
		sum := int32(0)
		processor := NewSliceProcessor[int]().
			Configure(WithHardMaxWait(500 * time.Millisecond)).
			Run(func(ints []int, _ int64) error {
				if len(ints) > 0 {
					t.Fatalf("TestBatchedHardWait must not put any item to processor")
				}
				atomic.AddInt32(&sum, 1)
				return nil
			})
		time.Sleep(1 * time.Second)
		if sum == 0 {
			t.Fatalf("hard wait process not run when the batch is empty")
		}
		if err := processor.Close(context.Background()); err != nil {
			t.Fatalf("error closing processor: %v", err)
		}
	})
}

func summing(p *int32) ProcessBatchFn[[]int] {
	return func(ints []int, _ int64) error {
		for _, num := range ints {
			atomic.AddInt32(p, int32(num))
		}
		return nil
	}
}

func TestErrorHandling(t *testing.T) {
	t.Run("BasicError", func(t *testing.T) {
		sum := int32(0)
		errCnt := int32(0)
		processor := NewSliceProcessor[int]().
			Configure(WithMaxItem(10), WithBlockWhileProcessing(), WithDisabledDefaultProcessErrorLog(), WithMaxWait(Unset)).
			Run(summingErr(&sum), WithBatchErrorHandlers(func(ints []int, i int64, _ error) error {
				slog.Info("receive error of", slog.Any("ints", ints))
				atomic.AddInt32(&errCnt, int32(i))
				return nil
			}))
		ctx := context.Background()
		for i := 0; i < 1_000_000; i++ {
			processor.Put(ctx, 1)
		}
		if err := processor.Close(ctx); err != nil {
			t.Fatalf("error closing processor: %v", err)
		}
		if sum+errCnt != 1_000_000 {
			t.Fatalf("sum is %d != 1_000_000", sum+errCnt)
		}
	})

	t.Run("BasicError Chain", func(t *testing.T) {
		sum := int32(0)
		errCnt := int32(0)
		processor := NewSliceProcessor[int]().
			Configure(WithMaxItem(10), WithBlockWhileProcessing(), WithDisabledDefaultProcessErrorLog(), WithMaxWait(Unset)).
			Run(summingErr(&sum), WithBatchErrorHandlers(
				func(ints []int, i int64, _ error) error {
					slog.Info("receive error of", slog.Any("ints", ints))
					atomic.AddInt32(&errCnt, int32(i))
					return errors.New("continue")
				},
				func(ints []int, count int64, _ error) error {
					slog.Info("receive error of", slog.Any("ints", ints))
					for i := int64(0); i < count; i++ {
						atomic.AddInt32(&errCnt, int32(ints[i]))
					}
					return nil
				},
				func(_ []int, _ int64, _ error) error {
					t.Fatalf("should stopped")
					return nil
				},
			))

		ctx := context.Background()
		for i := 0; i < 1_000_000; i++ {
			processor.Put(ctx, 1)
		}
		if err := processor.Close(ctx); err != nil {
			t.Fatalf("error closing processor: %v", err)
		}
		if sum+(errCnt/2) != 1_000_000 {
			t.Fatalf("sum is %d != 1_000_000", sum+(errCnt/2))
		}
	})

	t.Run("RemainError", func(t *testing.T) {
		sum := int32(0)
		errCnt := int32(0)
		processor := NewSliceProcessor[int]().
			Configure(WithMaxItem(10), WithBlockWhileProcessing(), WithDisabledDefaultProcessErrorLog(), WithMaxWait(Unset)).
			Run(summingErrHalf(&sum), WithBatchErrorHandlers(func(ints []int, i int64, _ error) error {
				slog.Info("receive error of", slog.Any("ints", ints))
				atomic.AddInt32(&errCnt, int32(i))
				return nil
			}))
		ctx := context.Background()
		for i := 0; i < 1_000_000; i++ {
			processor.Put(ctx, 1)
		}
		if err := processor.Close(ctx); err != nil {
			t.Fatalf("error closing processor: %v", err)
		}
		if sum+(errCnt*2) != 1_000_000 {
			t.Fatalf("sum is %d != 1_000_000", sum+(errCnt*2))
		}
	})

	t.Run("RemainError (NoMaxWait)", func(t *testing.T) {
		sum := int32(0)
		errCnt := int32(0)
		processor := NewSliceProcessor[int]().
			Configure(WithMaxItem(100), WithDisabledDefaultProcessErrorLog()).
			Run(summingErrHalfSleep(&sum, 200*time.Millisecond), WithBatchErrorHandlers(func(ints []int, i int64, _ error) error {
				slog.Info("receive error of", slog.Any("ints", ints))
				atomic.AddInt32(&errCnt, int32(i))
				return nil
			}))
		ctx := context.Background()
		for i := 0; i < 1_000; i++ {
			processor.Put(ctx, 1)
		}
		if err := processor.Close(ctx); err != nil {
			t.Fatalf("error closing processor: %v", err)
		}
		if sum+(errCnt*2) != 1_000 {
			t.Fatalf("sum is %d != 1_000", sum+(errCnt*2))
		}
	})

	t.Run("RemainError Chain", func(t *testing.T) {
		sum := int32(0)
		errCnt := int32(0)
		processor := NewSliceProcessor[int]().
			Configure(WithMaxItem(10), WithBlockWhileProcessing(), WithDisabledDefaultProcessErrorLog(), WithMaxWait(Unset)).
			Run(summingErr(&sum), WithBatchErrorHandlers(
				func(ints []int, i int64, _ error) error {
					slog.Info("receive error of", slog.Any("ints", ints))
					half := i / 2
					atomic.AddInt32(&errCnt, int32(half))
					return NewErrorWithRemaining(errors.New("continue"), ints[0:half], half)
				},
				func(ints []int, count int64, _ error) error {
					slog.Info("receive error of", slog.Any("ints", ints))
					for i := int64(0); i < count; i++ {
						atomic.AddInt32(&errCnt, int32(ints[i]))
					}
					return nil
				},
				func(_ []int, _ int64, _ error) error {
					t.Fatalf("should stopped")
					return nil
				},
			))
		ctx := context.Background()
		for i := 0; i < 1_000_000; i++ {
			processor.Put(ctx, 1)
		}
		if err := processor.Close(ctx); err != nil {
			t.Fatalf("error closing processor: %v", err)
		}
		if sum+errCnt != 1_000_000 {
			t.Fatalf("sum is %d != 1_000_000", sum+errCnt)
		}
	})
}

func summingErrHalf(p *int32) ProcessBatchFn[[]int] {
	return func(ints []int, i int64) error {
		if rand.Intn(10) > 6 {
			half := i / 2
			return NewErrorWithRemaining(errors.New("random error"), ints[0:half], half)
		}
		for _, num := range ints {
			atomic.AddInt32(p, int32(num))
		}
		return nil
	}
}

func summingErrHalfSleep(p *int32, sleep time.Duration) ProcessBatchFn[[]int] {
	return func(ints []int, i int64) error {
		time.Sleep(sleep)
		if rand.Intn(10) > 6 {
			half := i / 2
			return NewErrorWithRemaining(errors.New("random error"), ints[0:half], half)
		}
		for _, num := range ints {
			atomic.AddInt32(p, int32(num))
		}
		return nil
	}
}

func summingErr(p *int32) ProcessBatchFn[[]int] {
	return func(ints []int, _ int64) error {
		if rand.Intn(10) > 6 {
			return errors.New("random error")
		}
		for _, num := range ints {
			atomic.AddInt32(p, int32(num))
		}
		return nil
	}
}

func TestCancelContext(t *testing.T) {
	t.Run("Put", func(t *testing.T) {
		sum := int32(0)
		processor := NewSliceProcessor[int]().
			Configure(WithMaxItem(5), WithMaxConcurrency(Unset)).
			Run(summing(&sum))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		ok := processor.Put(ctx, 1)
		if ok || sum > 0 {
			t.Fatalf("Cancelled context added to processor")
		}
		if err := processor.Close(context.Background()); err != nil {
			t.Fatalf("error closing processor: %v", err)
		}
	})

	t.Run("PutAll", func(t *testing.T) {
		sum := int32(0)
		processor := NewSliceProcessor[int]().
			Configure(WithMaxItem(5), WithMaxConcurrency(Unset)).
			Run(summing(&sum))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		ok := processor.PutAll(ctx, []int{1, 2, 3})
		if ok > 0 || sum > 0 {
			t.Fatalf("Cancelled context added to processor")
		}
		if err := processor.Close(context.Background()); err != nil {
			t.Fatalf("error closing processor: %v", err)
		}
	})

	t.Run("Merge", func(t *testing.T) {
		sum := int32(0)
		merger := AddToSlice[int]
		processor := NewProcessor(InitSlice[int], func(_ []int, _ int) []int { return nil }).
			Configure(WithMaxItem(5), WithMaxConcurrency(Unset)).
			Run(summing(&sum))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		ok := processor.Merge(ctx, 1, merger)
		if ok || sum > 0 {
			t.Fatalf("Cancelled context added to processor")
		}
		if err := processor.Close(context.Background()); err != nil {
			t.Fatalf("error closing processor: %v", err)
		}
	})

	t.Run("MergeAll", func(t *testing.T) {
		sum := int32(0)
		merger := AddToSlice[int]
		processor := NewProcessor(InitSlice[int], func(_ []int, _ int) []int { return nil }).
			Configure(WithMaxItem(5), WithMaxConcurrency(Unset)).
			Run(summing(&sum))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		ok := processor.MergeAll(ctx, []int{1, 2, 3}, merger)
		if ok > 0 || sum > 0 {
			t.Fatalf("Cancelled context added to processor")
		}
		if err := processor.Close(context.Background()); err != nil {
			t.Fatalf("error closing processor: %v", err)
		}
	})

	t.Run("Peek", func(t *testing.T) {
		sum := int32(0)
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(5), WithMaxConcurrency(Unset)).
			Run(summing(&sum))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := processor.Peek(ctx, func(ints []int, i int64) error {
			panic("Should not be called")
		})
		if err == nil {
			t.Fatalf("Cancelled peek should return error")
		}
		if err := processor.Close(context.Background()); err != nil {
			t.Fatalf("error closing processor: %v", err)
		}
	})
}

func TestPeek(t *testing.T) {
	sum := int32(0)
	processor := NewSliceProcessor[int]().
		Configure(WithMaxItem(10), WithMaxWait(Unset)).
		Run(summing(&sum))

	ctx := context.Background()
	for i := 0; i < 9; i++ {
		processor.Put(ctx, 1)
	}

	peeked := false
	err := processor.Peek(ctx, func(ints []int, _ int64) error {
		if len(ints) != 9 {
			t.Fatal("Peek return inconsistent item count")
		}
		peeked = true
		return nil
	})
	if err != nil {
		t.Fatal(err.Error())
	}
	if cnt, ok := processor.ItemCount(ctx); ok && cnt != 9 {
		t.Fatal("Peek trigger processing")
	}
	if !peeked {
		t.Fatalf("Peek not called")
	}
	if err := processor.Close(ctx); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
	if sum != 9 {
		t.Fatalf("sum is %d != 9", sum)
	}
}

func TestCustomCounter(t *testing.T) {
	touched := int32(0)

	processor := NewSelfMapProcessor[int, int](func(i int) int {
		return i
	}, nil).
		Configure(WithMaxItem(11), WithMaxWait(Unset)).
		Run(func(_ map[int]int, i int64) error {
			atomic.AddInt32(&touched, 1)
			if i != 110 {
				t.Fatal("Input counter is not correct")
			}
			return nil
		}, WithBatchCounter(CountMapKeys[int, int]()))

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		processor.Put(ctx, 1)
	}

	if cnt, ok := processor.ItemCount(ctx); ok && cnt != 1 {
		t.Fatal("Counter is not correct")
	}

	if atomic.LoadInt32(&touched) > 0 {
		t.Fatal("Touched when counter is not reached limit")
	}

	for i := 0; i < 10; i++ {
		processor.Put(ctx, i+1)
	}

	if cnt, ok := processor.ItemCount(ctx); ok && cnt != 10 {
		t.Fatal("Counter is not correct")
	}

	if err := processor.Close(ctx); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
	if atomic.LoadInt32(&touched) != 1 {
		t.Fatal("Batch not being processed")
	}
}
