package main

import (
	"context"
	"github.com/mawngo/go-batch"
)

func main() {
	processor := batch.NewProcessor(
		batch.InitSlice[*WaitedItem],
		func(b []*WaitedItem, item *WaitedItem) []*WaitedItem {
			item.ch = make(chan struct{}, 1)
			b = append(b, item)
			return b
		},
	).Configure(batch.WithMaxConcurrency(5), batch.WithMaxItem(10)).
		Run(func(items []*WaitedItem, _ int64) error {
			// Do stuff...
			// Then notify the future.
			for _, item := range items {
				item.done = true
				close(item.ch)
			}
			return nil
		})

	waiting := make([]*WaitedItem, 0, 1_000_000)

	for i := 0; i < 1_000_000; i++ {
		item := &WaitedItem{item: 1}
		processor.Put(item)
		waiting = append(waiting, item)
	}

	sum := 0
	for _, item := range waiting {
		v, _ := item.Get()
		sum += v
	}

	processor.MustClose()
	if sum != 1_000_000 {
		panic("sum is not 1_000_000")
	}
}

var (
	_ batch.Future[int] = (*WaitedItem)(nil)
)

type WaitedItem struct {
	ch chan struct{}

	done bool
	item int
	err  error
}

func (w *WaitedItem) Get() (int, error) {
	return w.GetContext(context.Background())
}

func (w *WaitedItem) GetContext(ctx context.Context) (int, error) {
	if w.done {
		return w.item, w.err
	}
	select {
	case <-ctx.Done():
	case _, _ = <-w.ch:
	}
	return w.item, w.err
}
