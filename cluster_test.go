package batch

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestCluster(t *testing.T) {
	maxes := []int{1, 2, 5, 8, 10, 18, 20, 111}
	for _, m := range maxes {
		cnt := int64(0)
		pFn := func(_ int, _ int64) uint64 {
			res := int(cnt) % m
			atomic.AddInt64(&cnt, 1)
			return uint64(res)
		}

		sum := int32(0)
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(10)).
			ToCluster(m, FnPartitioner(pFn)).
			Run(summing(&sum))

		for i := 0; i < 50_000; i++ {
			processor.Put(1)
		}
		for i := 0; i < 50_000; i++ {
			processor.PutContext(context.Background(), 1)
		}
		for i := 0; i < 5_000; i++ {
			processor.PutAllContext(context.Background(), new1x10Slice())
		}
		for i := 0; i < 5_000; i++ {
			processor.PutAll(new1x10Slice())
		}
		processor.MustClose()
		if sum != 200_000 {
			t.Fatalf("sum is %d != 200_000", sum)
		}
	}
}

func TestClusterCloseContext(t *testing.T) {
	maxes := []int{1, 2, 5, 8, 10, 18}
	for _, m := range maxes {
		cnt := int64(0)
		pFn := func(_ int, _ int64) uint64 {
			res := int(cnt) % m
			atomic.AddInt64(&cnt, 1)
			return uint64(res)
		}

		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(5), WithMaxConcurrency(Unset)).
			ToCluster(m, FnPartitioner(pFn)).
			Run(func(_ []int, _ int64) error {
				time.Sleep(100 * time.Second)
				return nil
			})

		for i := 0; i < 10; i++ {
			processor.Put(1)
		}
		var err error
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err = processor.CloseContext(ctx)
		}()
		if err == nil {
			t.Fatalf("processor is closed before complete processing!")
		}
	}
}

func TestClusterDrainContext(t *testing.T) {
	maxes := []int{1, 2, 5, 8, 10, 18}
	for _, m := range maxes {
		cnt := int64(0)
		pFn := func(_ int, _ int64) uint64 {
			res := int(cnt) % m
			atomic.AddInt64(&cnt, 1)
			return uint64(res)
		}
		sum := int32(0)

		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(11), WithBlockWhileProcessing(), WithMaxWait(Unset)).
			ToCluster(m, FnPartitioner(pFn)).
			Run(summing(&sum))

		for i := 0; i < 10; i++ {
			processor.Put(1)
		}
		if sum != 0 {
			t.Fatalf("item processed while limit not reached")
		}
		var err error
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err = processor.DrainContext(ctx)
		}()

		if err != nil {
			t.Fatalf("drain result in an error %v", err)
		}
		if sum != 10 {
			t.Fatalf("drain not processing remaining item")
		}
		processor.MustClose()
	}
}

func TestClusterFlushContext(t *testing.T) {
	maxes := []int{1, 2, 5, 8, 10, 18}
	for _, m := range maxes {
		cnt := int64(0)
		pFn := func(_ int, _ int64) uint64 {
			res := int(cnt) % m
			atomic.AddInt64(&cnt, 1)
			return uint64(res)
		}

		sum := int32(0)
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(11), WithBlockWhileProcessing(), WithMaxWait(Unset)).
			ToCluster(m, FnPartitioner(pFn)).
			Run(summing(&sum))

		for i := 0; i < 10; i++ {
			processor.Put(1)
		}
		if sum != 0 {
			t.Fatalf("item processed while limit not reached")
		}
		var err error
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err = processor.FlushContext(ctx)
		}()

		if err != nil {
			t.Fatalf("flush result in an error %v", err)
		}
		if sum != 10 {
			t.Fatalf("flush not processing remaining item")
		}
		processor.MustClose()
	}
}

func TestClusterSequentiallyFlushContext(t *testing.T) {
	maxes := []int{1, 2, 5, 8, 10, 18}
	for _, m := range maxes {
		cnt := int64(0)
		pFn := func(_ int, _ int64) uint64 {
			res := int(cnt) % m
			atomic.AddInt64(&cnt, 1)
			return uint64(res)
		}

		sum := int32(0)
		processor := NewProcessor(InitSlice[int], AddToSlice[int]).
			Configure(WithMaxItem(11), WithBlockWhileProcessing(), WithMaxWait(Unset)).
			ToCluster(m, FnPartitioner(pFn)).
			Sequentially().
			Run(summing(&sum))

		for i := 0; i < 10; i++ {
			processor.Put(1)
		}
		if sum != 0 {
			t.Fatalf("item processed while limit not reached")
		}
		var err error
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err = processor.FlushContext(ctx)
		}()

		if err != nil {
			t.Fatalf("flush result in an error %v", err)
		}
		if sum != 10 {
			t.Fatalf("flush not processing remaining item")
		}
		processor.MustClose()
	}
}

func TestClusterPutContext(t *testing.T) {
	pFn := func(i int, _ int64) uint64 {
		return uint64(i % 2)
	}

	processor := NewProcessor(InitSlice[int], AddToSlice[int]).
		Configure(WithMaxItem(5), WithMaxConcurrency(Unset)).
		ToCluster(2, FnPartitioner(pFn)).
		Sequentially().
		Run(func(_ []int, _ int64) error {
			return nil
		})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ok := processor.PutContext(ctx, 1)
	if ok {
		t.Fatalf("Cancelled context added to processor")
	}
	processor.MustClose()
}
