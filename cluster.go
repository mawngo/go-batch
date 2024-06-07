package batch

import (
	"context"
	"errors"
	"sync"
)

var _ Runner[any, any] = (*RunningCluster[any, any])(nil)
var _ Partitioner[any] = (*fnPartitioner[any])(nil)

// ToCluster convert this processor into a cluster of processor.
func (p ProcessorSetup[T, B]) ToCluster(maxPartition int, partitioner Partitioner[T]) ClusterSetup[T, B] {
	return ClusterSetup[T, B]{
		processor:    p,
		partition:    partitioner,
		maxPartition: int64(maxPartition),
	}
}

// NewCluster create cluster using configuration.
func NewCluster[T any, B any, I ~int | ~int32 | ~int64](p ProcessorSetup[T, B], maxPartition I, partitioner Partitioner[T]) ClusterSetup[T, B] {
	return ClusterSetup[T, B]{
		processor:    p,
		partition:    partitioner,
		maxPartition: int64(maxPartition),
	}
}

// ClusterSetup cluster processor that is in setup phase (not running).
// Call [ClusterSetup.Run] with a handler to create a [RunningCluster] that can accept item.
// A Cluster is a group of processors that share the same configuration and running logic.
// Item passed to a cluster will be partitioned into one of the processors using [Partitioner].
type ClusterSetup[T any, B any] struct {
	processor    ProcessorSetup[T, B]
	partition    Partitioner[T]
	maxPartition int64
	sequential   bool
}

// Partitioner takes an item and returns the partition number.
// If the partition number is greater than maxPartition, then the partition will be recalculation.
type Partitioner[T any] interface {
	Partition(item T, maxPartition int64) uint64
}

// FnPartitioner create a [Partitioner] using the specified function.
func FnPartitioner[T any](fn func(T, int64) uint64) Partitioner[T] {
	return &fnPartitioner[T]{
		apply: fn,
	}
}

type fnPartitioner[T any] struct {
	apply func(T, int64) uint64
}

func (p *fnPartitioner[T]) Partition(item T, maxPartition int64) uint64 {
	return p.apply(item, maxPartition)
}

// Sequentially create a new [ClusterSetup] with sequential enabled.
// Operation on sequential cluster will be processed sequentially for each processor.
// Operation on non-sequential cluster will be processed at the same time for each processor.
func (c ClusterSetup[T, B]) Sequentially() ClusterSetup[T, B] {
	p := c
	p.sequential = true
	return p
}

// RunningCluster cluster of processor that is running and can process item.
type RunningCluster[T any, B any] struct {
	ClusterSetup[T, B]
	processors []*RunningProcessor[T, B]
}

// PutAll put all items to cluster
// It is recommended to use [RunningCluster.PutAllContext] instead.
func (r *RunningCluster[T, B]) PutAll(items []T) {
	r.PutAllContext(context.Background(), items)
}

// PutAllContext put all items to cluster
// Due to partitioning, the number of items processed cannot be used to determine the index of the last processed item.
func (r *RunningCluster[T, B]) PutAllContext(ctx context.Context, items []T) int {
	maxPartition := uint64(r.ClusterSetup.maxPartition)
	m := make(map[uint64][]T, maxPartition)
	for _, item := range items {
		p := r.ClusterSetup.partition.Partition(item, r.ClusterSetup.maxPartition)
		if p >= maxPartition {
			p %= maxPartition
		}
		list := m[p]
		if list == nil {
			list = make([]T, 0, int64(len(items))/r.ClusterSetup.maxPartition)
		}
		m[p] = append(list, item)
	}

	ok := 0
	for p, items := range m {
		ok += r.processors[p].PutAllContext(ctx, items)
	}
	return ok
}

// Put add item to the processor.
// It is recommended to use [RunningCluster.PutContext] instead.
func (r *RunningCluster[T, B]) Put(item T) {
	r.PutContext(context.Background(), item)
}

// PutContext add item to the processor.
func (r *RunningCluster[T, B]) PutContext(ctx context.Context, item T) bool {
	maxPartition := uint64(r.ClusterSetup.maxPartition)
	p := r.ClusterSetup.partition.Partition(item, r.ClusterSetup.maxPartition)
	if p >= maxPartition {
		p %= maxPartition
	}
	return r.processors[p].PutContext(ctx, item)
}

// ApproxItemCount return total number of current item in cluster, approximately.
func (r *RunningCluster[T, B]) ApproxItemCount() int64 {
	sum := int64(0)
	for i := range r.processors {
		sum += r.processors[i].ApproxItemCount()
	}
	return sum
}

// ItemCount return total number of current item in cluster.
func (r *RunningCluster[T, B]) ItemCount() int64 {
	sum := int64(0)
	for i := range r.processors {
		sum += r.processors[i].ItemCount()
	}
	return sum
}

// ItemCountContext return total number of current item in cluster.
func (r *RunningCluster[T, B]) ItemCountContext(ctx context.Context) (int64, bool) {
	sum := int64(0)
	ok := true
	for i := range r.processors {
		s, o := r.processors[i].ItemCountContext(ctx)
		sum += s
		if !o {
			ok = false
		}
	}
	return sum, ok
}

// Close stop the cluster.
// This method will process the leftover branch on caller thread.
// Return error if maxCloseWait passed. See getCloseMaxWait for detail.
func (r *RunningCluster[T, B]) Close() error {
	errs := make([]error, 0, r.ClusterSetup.maxPartition)
	if r.sequential {
		for i := range r.processors {
			err := r.processors[i].Close()
			if err != nil {
				errs = append(errs, err)
			}
		}
	} else {
		var wg sync.WaitGroup
		for i := range r.processors {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := r.processors[i].Close()
				if err != nil {
					errs = append(errs, err)
				}
			}()
		}
		wg.Wait()
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// CloseContext stop the cluster.
// This method will process the leftover branch on caller thread.
// Context can be used to provide deadline for this method.
func (r *RunningCluster[T, B]) CloseContext(ctx context.Context) error {
	errs := make([]error, 0, r.ClusterSetup.maxPartition)
	if r.sequential {
		for i := range r.processors {
			err := r.processors[i].CloseContext(ctx)
			if err != nil {
				errs = append(errs, err)
			}
		}
	} else {
		var wg sync.WaitGroup
		for i := range r.processors {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := r.processors[i].CloseContext(ctx)
				if err != nil {
					errs = append(errs, err)
				}
			}()
		}
		wg.Wait()
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// StopContext stop the cluster.
// This method does not handle leftover batch.
func (r *RunningCluster[T, B]) StopContext(ctx context.Context) error {
	errs := make([]error, 0, r.ClusterSetup.maxPartition)
	if r.sequential {
		for i := range r.processors {
			err := r.processors[i].StopContext(ctx)
			if err != nil {
				errs = append(errs, err)
			}
		}
	} else {
		var wg sync.WaitGroup
		for i := range r.processors {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := r.processors[i].StopContext(ctx)
				if err != nil {
					errs = append(errs, err)
				}
			}()
		}
		wg.Wait()
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// DrainContext force all processor in cluster to process the current batch util their batch is empty.
// This method always processes the batch on caller thread.
// Context can be used to provide deadline for this method.
func (r *RunningCluster[T, B]) DrainContext(ctx context.Context) error {
	errs := make([]error, 0, r.ClusterSetup.maxPartition)
	if r.sequential {
		for i := range r.processors {
			err := r.processors[i].DrainContext(ctx)
			if err != nil {
				errs = append(errs, err)
			}
		}
	} else {
		var wg sync.WaitGroup
		for i := range r.processors {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := r.processors[i].DrainContext(ctx)
				if err != nil {
					errs = append(errs, err)
				}
			}()
		}
		wg.Wait()
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// FlushContext force all processor in cluster to process the current batch.
// This method may process the batch on caller thread, depend on concurrent and block settings.
// Context can be used to provide deadline for this method.
func (r *RunningCluster[T, B]) FlushContext(ctx context.Context) error {
	errs := make([]error, 0, r.ClusterSetup.maxPartition)
	if r.sequential {
		for i := range r.processors {
			err := r.processors[i].FlushContext(ctx)
			if err != nil {
				errs = append(errs, err)
			}
		}
	} else {
		var wg sync.WaitGroup
		for i := range r.processors {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := r.processors[i].FlushContext(ctx)
				if err != nil {
					errs = append(errs, err)
				}
			}()
		}
		wg.Wait()
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// Flush force all processor in the cluster to process the current batch.
// This method may process the batch on caller thread, depend on concurrent and block settings.
func (r *RunningCluster[T, B]) Flush() {
	if r.sequential {
		for i := range r.processors {
			r.processors[i].Flush()
		}
		return
	}
	var wg sync.WaitGroup
	for i := range r.processors {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.processors[i].Flush()
		}()
	}
	wg.Wait()
}

// MustClose stop the cluster without deadline.
func (r *RunningCluster[T, B]) MustClose() {
	if r.sequential {
		for i := range r.processors {
			r.processors[i].MustClose()
		}
		return
	}
	var wg sync.WaitGroup
	for i := range r.processors {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.processors[i].MustClose()
		}()
	}
	wg.Wait()
}

// Run create a [*RunningCluster] that can accept item.
// Accept a [ProcessBatchFn] and a [RecoverBatchFn] chain to process on error.
func (c ClusterSetup[T, B]) Run(process ProcessBatchFn[B], errorHandlers ...RecoverBatchFn[B]) *RunningCluster[T, B] {
	processors := make([]*RunningProcessor[T, B], c.maxPartition)
	for i := 0; i < len(processors); i++ {
		processors[i] = c.processor.Run(process, errorHandlers...)
	}
	return &RunningCluster[T, B]{ClusterSetup: c, processors: processors}
}
