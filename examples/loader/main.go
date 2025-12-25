package main

import (
	"github.com/mawngo/go-batch/v2"
	"strconv"
	"sync/atomic"
	"time"
)

func main() {
	loadedCount := int32(0)
	// First create a batch.LoaderSetup
	loader := batch.NewLoader[int, string]().
		// Configure the loader.
		// All pending load requests will be processed when one of the following limits is reached.
		Configure(batch.WithMaxItem(100), batch.WithMaxWait(1*time.Second)).
		// Like when using the processor,
		// start the loader by providing a LoadBatchFn,
		// and optionally additional run configuration.
		// This will create a *batch.Loader that can accept item.
		Run(load(&loadedCount))

	for i := 0; i < 100_000; i++ {
		k := i % 10
		go func() {
			// Use the loader.
			// Alternately, you can use the Load method
			// future := loader.Load(k)
			// ...
			// v, err := future.Get()
			v, err := loader.Get(k)

			if err != nil {
				panic(err)
			}
			if v != strconv.Itoa(k) {
				panic("key mismatch")
			}
		}()
	}
	// Remember to close the running load before your application stopped.
	// Closing will force the loader to load the left-over request,
	// any load request after the loader is closed is not guaranteed to be processed
	// and may block forever.
	if err := loader.Close(); err != nil {
		panic(err)
	}
	// If you do not want to load left over request, then use StopContext instead.
	// if err := loader.StopContext(context.Background()); err != nil {
	//     panic(err)
	// }
	if loadedCount > 1 {
		panic("loaded too many time")
	}
}

func load(p *int32) batch.LoadBatchFn[int, string] {
	return func(batch batch.LoadKeys[int], _ int64) (map[int]string, error) {
		atomic.AddInt32(p, 1)
		if len(batch.Keys) == 0 {
			// This could happen if you provide an alternate counting method.
			return nil, nil
		}
		time.Sleep(2 * time.Second)

		res := make(map[int]string, len(batch.Keys))
		for _, k := range batch.Keys {
			res[k] = strconv.Itoa(k)
		}
		return res, nil
	}
}
