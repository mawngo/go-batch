package batch

import (
	"context"
	"errors"
	"go.uber.org/goleak"
	"log/slog"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// This test will leak as the processor is forced to close before tasks completed, which was an expected behavior.
		goleak.IgnoreAnyFunction("github.com/mawngo/go-batch/v2.TestCloseContext.func1"),
		goleak.IgnoreAnyFunction("github.com/mawngo/go-batch/v2.TestClusterCloseContext.func2"),
	)
}

func new1x10Slice() []int {
	return []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
}

func TestBatched(t *testing.T) {
	sum := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(10)).
		Run(summing(&sum))

	for i := 0; i < 500_000; i++ {
		processor.Put(1)
	}
	for i := 0; i < 500_000; i++ {
		processor.PutContext(context.Background(), 1)
	}
	for i := 0; i < 50_000; i++ {
		processor.PutAll(new1x10Slice())
	}
	for i := 0; i < 50_000; i++ {
		processor.PutAllContext(context.Background(), new1x10Slice())
	}
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
	if sum != 2_000_000 {
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

		for i := 0; i < 50_000; i++ {
			processor.Put(1)
		}
		for i := 0; i < 5_000; i++ {
			processor.PutAll(new1x10Slice())
		}
		for i := 0; i < 50_000; i++ {
			processor.PutContext(context.Background(), 1)
		}
		for i := 0; i < 5_000; i++ {
			processor.PutAllContext(context.Background(), new1x10Slice())
		}
		if err := processor.Close(); err != nil {
			t.Fatalf("error closing processor: %v", err)
		}
		if sum != 200_000 {
			t.Fatalf("sum is %d != 200_000", sum)
		}
	}
}

func TestBatchedAllDisabled(t *testing.T) {
	sum := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(Unset), WithMaxWait(Unset)).
		Run(summing(&sum))

	for i := 0; i < 500_000; i++ {
		processor.Put(1)
	}
	for i := 0; i < 500_000; i++ {
		processor.PutContext(context.Background(), 1)
	}
	for i := 0; i < 50_000; i++ {
		processor.PutAll(new1x10Slice())
	}
	for i := 0; i < 50_000; i++ {
		processor.PutAllContext(context.Background(), new1x10Slice())
	}
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
	if sum != 2_000_000 {
		t.Fatalf("sum is %d != 2_000_000", sum)
	}
}

func TestConcurrentBatchedAllDisabled(t *testing.T) {
	sum := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(Unset), WithMaxWait(Unset), WithMaxConcurrency(10)).
		Run(summing(&sum))

	for i := 0; i < 500_000; i++ {
		processor.Put(1)
	}
	for i := 0; i < 500_000; i++ {
		processor.PutContext(context.Background(), 1)
	}
	for i := 0; i < 50_000; i++ {
		processor.PutAll(new1x10Slice())
	}
	for i := 0; i < 50_000; i++ {
		processor.PutAllContext(context.Background(), new1x10Slice())
	}
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
	if sum != 2_000_000 {
		t.Fatalf("sum is %d != 2_000_000", sum)
	}
}

func TestBatchedAggressive(t *testing.T) {
	sum := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(10), WithAggressiveMode()).
		Run(summing(&sum))

	for i := 0; i < 500_000; i++ {
		processor.Put(1)
	}
	for i := 0; i < 500_000; i++ {
		processor.PutContext(context.Background(), 1)
	}
	for i := 0; i < 50_000; i++ {
		processor.PutAll(new1x10Slice())
	}
	for i := 0; i < 50_000; i++ {
		processor.PutAllContext(context.Background(), new1x10Slice())
	}
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
	if sum != 2_000_000 {
		t.Fatalf("sum is %d != 2_000_000", sum)
	}
}

func TestConcurrentBatchedAggressive(t *testing.T) {
	sum := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(10), WithAggressiveMode(), WithMaxConcurrency(10)).
		Run(summing(&sum))

	for i := 0; i < 500_000; i++ {
		processor.Put(1)
	}
	for i := 0; i < 500_000; i++ {
		processor.PutContext(context.Background(), 1)
	}
	for i := 0; i < 50_000; i++ {
		processor.PutAll(new1x10Slice())
	}
	for i := 0; i < 50_000; i++ {
		processor.PutAllContext(context.Background(), new1x10Slice())
	}
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
	if sum != 2_000_000 {
		t.Fatalf("sum is %d != 2_000_000", sum)
	}
}

func TestDisabled(t *testing.T) {
	sum := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(0)).
		Run(summing(&sum))

	for i := 0; i < 500_000; i++ {
		processor.Put(1)
	}
	for i := 0; i < 500_000; i++ {
		processor.PutContext(context.Background(), 1)
	}
	for i := 0; i < 50_000; i++ {
		processor.PutAll(new1x10Slice())
	}
	for i := 0; i < 50_000; i++ {
		processor.PutAllContext(context.Background(), new1x10Slice())
	}
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
	if sum != 2_000_000 {
		t.Fatalf("sum is %d != 2_000_000", sum)
	}
}

func TestCloseContext(t *testing.T) {
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(5), WithMaxConcurrency(Unset)).
		Run(func(_ []int, _ int64) error {
			time.Sleep(100 * time.Second)
			return nil
		})

	for i := 0; i < 10; i++ {
		processor.Put(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := processor.CloseContext(ctx)
	if err == nil {
		t.Fatalf("processor is closed before complete processing!")
	}
}

func TestDrainContext(t *testing.T) {
	sum := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(11), WithBlockWhileProcessing(), WithMaxWait(Unset)).
		Run(summing(&sum))

	for i := 0; i < 10; i++ {
		processor.Put(1)
	}
	if sum != 0 {
		t.Fatalf("item processed while limit not reached")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := processor.DrainContext(ctx)
	if err != nil {
		t.Fatalf("drain result in an error %v", err)
	}
	if sum != 10 {
		t.Fatalf("drain not processing remaining item")
	}
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
}

func TestFlushContext(t *testing.T) {
	sum := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(11), WithBlockWhileProcessing(), WithMaxWait(Unset)).
		Run(summing(&sum))

	for i := 0; i < 10; i++ {
		processor.Put(1)
	}
	if sum != 0 {
		t.Fatalf("item processed while limit not reached")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := processor.FlushContext(ctx)
	if err != nil {
		t.Fatalf("flush result in an error %v", err)
	}
	if sum != 10 {
		t.Fatalf("flush not processing remaining item")
	}
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
}

func TestBatchedDefault(t *testing.T) {
	sum := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Run(summing(&sum))

	for i := 0; i < 500_000; i++ {
		processor.Put(1)
	}
	for i := 0; i < 500_000; i++ {
		processor.PutContext(context.Background(), 1)
	}
	for i := 0; i < 50_000; i++ {
		processor.PutAll(new1x10Slice())
	}
	for i := 0; i < 50_000; i++ {
		processor.PutAllContext(context.Background(), new1x10Slice())
	}
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
	if sum != 2_000_000 {
		t.Fatalf("sum is %d != 2_000_000", sum)
	}
}

func TestBatchedBlockWhileProcessing(t *testing.T) {
	sum := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithBlockWhileProcessing(), WithMaxItem(10)).
		Run(summing(&sum))

	for i := 0; i < 500_000; i++ {
		processor.Put(1)
	}
	for i := 0; i < 500_000; i++ {
		processor.PutContext(context.Background(), 1)
	}
	for i := 0; i < 50_000; i++ {
		processor.PutAll(new1x10Slice())
	}
	for i := 0; i < 50_000; i++ {
		processor.PutAllContext(context.Background(), new1x10Slice())
	}
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
	if sum != 2_000_000 {
		t.Fatalf("sum is %d != 2_000_000", sum)
	}
}

func TestBatchedConcurrentBlockWhileProcessing(t *testing.T) {
	sum := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithBlockWhileProcessing(), WithMaxItem(10), WithMaxConcurrency(10)).
		Run(summing(&sum))

	for i := 0; i < 500_000; i++ {
		processor.Put(1)
	}
	for i := 0; i < 500_000; i++ {
		processor.PutContext(context.Background(), 1)
	}
	for i := 0; i < 50_000; i++ {
		processor.PutAll(new1x10Slice())
	}
	for i := 0; i < 50_000; i++ {
		processor.PutAllContext(context.Background(), new1x10Slice())
	}
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
	if sum != 2_000_000 {
		t.Fatalf("sum is %d != 2_000_000", sum)
	}
}

func TestBatchedConcurrent(t *testing.T) {
	sum := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxConcurrency(10), WithMaxItem(10)).
		Run(summing(&sum))

	for i := 0; i < 500_000; i++ {
		processor.Put(1)
	}
	for i := 0; i < 500_000; i++ {
		processor.PutContext(context.Background(), 1)
	}
	for i := 0; i < 50_000; i++ {
		processor.PutAll(new1x10Slice())
	}
	for i := 0; i < 50_000; i++ {
		processor.PutAllContext(context.Background(), new1x10Slice())
	}
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
	if sum != 2_000_000 {
		t.Fatalf("sum is %d != 2_000_000", sum)
	}
}

func TestBatchedNoWait(t *testing.T) {
	sum := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(10), WithMaxWait(0)).
		Run(summing(&sum))
	for i := 0; i < 500_000; i++ {
		processor.Put(1)
	}
	for i := 0; i < 500_000; i++ {
		processor.PutContext(context.Background(), 1)
	}
	for i := 0; i < 50_000; i++ {
		processor.PutAll(new1x10Slice())
	}
	for i := 0; i < 50_000; i++ {
		processor.PutAllContext(context.Background(), new1x10Slice())
	}
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
	if sum != 2_000_000 {
		t.Fatalf("sum is %d != 2_000_000", sum)
	}
}

func TestBatchedWait(t *testing.T) {
	sum := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(10), WithMaxWait(500*time.Millisecond)).
		Run(summing(&sum))
	for i := 0; i < 9; i++ {
		processor.Put(1)
	}
	time.Sleep(1 * time.Second)
	if processor.ItemCount() != 0 {
		t.Fatalf("item is not processed after timeout passed")
	}
	if sum != 9 {
		t.Fatalf("sum is %d != 9", sum)
	}
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
}

func TestBatchedSoftWait(t *testing.T) {
	sum := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
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
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
}

func TestBatchedHardWait(t *testing.T) {
	sum := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
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
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
}

func TestBatchedConcurrentNoWait(t *testing.T) {
	sum := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxConcurrency(10), WithMaxItem(10), WithMaxWait(0)).
		Run(summing(&sum))
	for i := 0; i < 500_000; i++ {
		processor.Put(1)
	}
	for i := 0; i < 500_000; i++ {
		processor.PutContext(context.Background(), 1)
	}
	for i := 0; i < 50_000; i++ {
		processor.PutAll(new1x10Slice())
	}
	for i := 0; i < 50_000; i++ {
		processor.PutAllContext(context.Background(), new1x10Slice())
	}
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
	if sum != 2_000_000 {
		t.Fatalf("sum is %d != 2_000_000", sum)
	}
}

func summing(p *int32) ProcessBatchFn[[]int] {
	return func(ints []int, _ int64) error {
		for _, num := range ints {
			atomic.AddInt32(p, int32(num))
		}
		return nil
	}
}

func TestBasicError(t *testing.T) {
	sum := int32(0)
	errCnt := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(10), WithBlockWhileProcessing(), WithDisabledDefaultProcessErrorLog(), WithMaxWait(Unset)).
		Run(summingErr(&sum), WithBatchErrorHandlers(func(ints []int, i int64, _ error) error {
			slog.Info("receive error of", slog.Any("ints", ints))
			atomic.AddInt32(&errCnt, int32(i))
			return nil
		}))

	for i := 0; i < 1_000_000; i++ {
		processor.Put(1)
	}
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
	if sum+errCnt != 1_000_000 {
		t.Fatalf("sum is %d != 1_000_000", sum+errCnt)
	}
}

func TestRemainError(t *testing.T) {
	sum := int32(0)
	errCnt := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(10), WithBlockWhileProcessing(), WithDisabledDefaultProcessErrorLog(), WithMaxWait(Unset)).
		Run(summingErrHalf(&sum), WithBatchErrorHandlers(func(ints []int, i int64, _ error) error {
			slog.Info("receive error of", slog.Any("ints", ints))
			atomic.AddInt32(&errCnt, int32(i))
			return nil
		}))

	for i := 0; i < 1_000_000; i++ {
		processor.Put(1)
	}
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
	if sum+(errCnt*2) != 1_000_000 {
		t.Fatalf("sum is %d != 1_000_000", sum+(errCnt*2))
	}
}

func TestRemainErrorNoMaxWait(t *testing.T) {
	sum := int32(0)
	errCnt := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(100), WithDisabledDefaultProcessErrorLog()).
		Run(summingErrHalfSleep(&sum, 200*time.Millisecond), WithBatchErrorHandlers(func(ints []int, i int64, _ error) error {
			slog.Info("receive error of", slog.Any("ints", ints))
			atomic.AddInt32(&errCnt, int32(i))
			return nil
		}))

	for i := 0; i < 1_000; i++ {
		processor.Put(1)
	}
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
	if sum+(errCnt*2) != 1_000 {
		t.Fatalf("sum is %d != 1_000", sum+(errCnt*2))
	}
}

func TestRemainErrorChain(t *testing.T) {
	sum := int32(0)
	errCnt := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
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

	for i := 0; i < 1_000_000; i++ {
		processor.Put(1)
	}
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
	if sum+errCnt != 1_000_000 {
		t.Fatalf("sum is %d != 1_000_000", sum+errCnt)
	}
}

func TestBasicErrorChain(t *testing.T) {
	sum := int32(0)
	errCnt := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
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

	for i := 0; i < 1_000_000; i++ {
		processor.Put(1)
	}
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
	if sum+(errCnt/2) != 1_000_000 {
		t.Fatalf("sum is %d != 1_000_000", sum+(errCnt/2))
	}
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

func TestPutContext(t *testing.T) {
	sum := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(5), WithMaxConcurrency(Unset)).
		Run(summing(&sum))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ok := processor.PutContext(ctx, 1)
	if ok || sum > 0 {
		t.Fatalf("Cancelled context added to processor")
	}
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
}

func TestPutAllContext(t *testing.T) {
	sum := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(5), WithMaxConcurrency(Unset)).
		Run(summing(&sum))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ok := processor.PutAllContext(ctx, []int{1, 2, 3})
	if ok > 0 || sum > 0 {
		t.Fatalf("Cancelled context added to processor")
	}
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
}

func TestMergeContext(t *testing.T) {
	sum := int32(0)
	merger := AddToSlice[int]
	processor := NewProcessor(InitSlice[int], func(_ []int, _ int) []int { return nil }).
		Configure(WithMaxItem(5), WithMaxConcurrency(Unset)).
		Run(summing(&sum))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ok := processor.MergeContext(ctx, 1, merger)
	if ok || sum > 0 {
		t.Fatalf("Cancelled context added to processor")
	}
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
}

func TestMergeAllContext(t *testing.T) {
	sum := int32(0)
	merger := AddToSlice[int]
	processor := NewProcessor(InitSlice[int], func(_ []int, _ int) []int { return nil }).
		Configure(WithMaxItem(5), WithMaxConcurrency(Unset)).
		Run(summing(&sum))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ok := processor.MergeAllContext(ctx, []int{1, 2, 3}, merger)
	if ok > 0 || sum > 0 {
		t.Fatalf("Cancelled context added to processor")
	}
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
}

func TestPeek(t *testing.T) {
	sum := int32(0)
	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(10), WithMaxWait(Unset)).
		Run(summing(&sum))
	for i := 0; i < 9; i++ {
		processor.Put(1)
	}

	err := processor.Peek(func(ints []int, _ int64) error {
		if len(ints) != 9 {
			t.Fatal("Peek return inconsistent item count")
		}
		// Modify current batch.
		// This should work as the function is synchronized.
		ints[0] = 2
		return nil
	})
	if err != nil {
		t.Fatal(err.Error())
	}
	if processor.ItemCount() != 9 {
		t.Fatal("Peek trigger processes")
	}
	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
	if sum != 10 {
		t.Fatalf("sum is %d != 10", sum)
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

	for i := 0; i < 100; i++ {
		processor.Put(1)
	}

	if processor.ItemCount() != 1 {
		t.Fatal("Counter is not correct")
	}

	if atomic.LoadInt32(&touched) > 0 {
		t.Fatal("Touched when counter is not reached limit")
	}

	for i := 0; i < 10; i++ {
		processor.Put(i + 1)
	}

	if processor.ItemCount() != 10 {
		t.Fatal("Counter is not correct")
	}

	if err := processor.Close(); err != nil {
		t.Fatalf("error closing processor: %v", err)
	}
	if atomic.LoadInt32(&touched) != 1 {
		t.Fatal("Batch not being processed")
	}
}
