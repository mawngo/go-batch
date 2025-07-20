package main

import (
	"github.com/mawngo/go-batch/v2"
	"sync"
)

var mu sync.Mutex

func main() {
	count := make(map[int]int)
	processor := batch.NewIdentityMapProcessor[int, int](mod2, nil).
		Configure(batch.WithMaxConcurrency(5), batch.WithMaxItem(10)).
		Run(summingValByKey(count))
	for i := 0; i < 1_000_000; i++ {
		processor.Put(i)
	}
	if err := processor.Close(); err != nil {
		panic(err)
	}
	// batch size is 10 so total batch is 100_000.
	if count[0] != 100_000 {
		panic("count is not 100_000")
	}
	if count[1] != 100_000 {
		panic("count is not 100_000")
	}
}

func summingValByKey(p map[int]int) batch.ProcessBatchFn[map[int]int] {
	return func(ints map[int]int, _ int64) error {
		for k := range ints {
			func() {
				mu.Lock()
				defer mu.Unlock()
				if current, ok := p[k]; ok {
					p[k] = current + 1
					return
				}
				p[k] = 1
			}()
		}
		return nil
	}
}

func mod2(i int) int {
	return i % 2
}
