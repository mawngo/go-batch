package main

import (
	"github.com/mawngo/go-batch/v2"
	"strconv"
	"sync"
)

func main() {
	cache := myCache{
		data: make(map[int]string),
	}
	// First create a batch.LoaderSetup
	loader := batch.NewLoader[int, string]().
		Run(func(batch batch.LoadKeys[int], _ int64) (map[int]string, error) {
			// Simulate getting the value from somewhere.
			res := make(map[int]string, len(batch.Keys))
			for _, k := range batch.Keys {
				res[k] = strconv.Itoa(k)
			}
			cache.AddAll(res)
			return res, nil
		})

	// Example use the cache.
	go func() {
		if v, ok := cache.Get(1); ok {
			println(v + " cached")
		}

		v, _ := loader.Get(1)
		println(v + " fresh")
	}()
}

type myCache struct {
	lock sync.RWMutex
	data map[int]string
}

func (c *myCache) Get(k int) (string, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if v, ok := c.data[k]; ok {
		return v, true
	}
	return "", false
}

func (c *myCache) AddAll(m map[int]string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	for k, v := range m {
		c.data[k] = v
	}
}
