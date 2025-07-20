package batch

import "context"

// Future is a future that can be used to get the result of a task.
type Future[T any] interface {
	Get() (T, error)
	GetContext(ctx context.Context) (T, error)
}

// KeyVal is a tuple of key and value.
type KeyVal[K any, V any] struct {
	key K
	val V
}

// InitSlice is [InitBatchFn] that allocate a slice.
func InitSlice[T any](i int64) []T {
	if i < 0 {
		return make([]T, 0)
	}
	if i == 0 {
		return make([]T, 1)
	}
	return make([]T, 0, i)
}

// AddToSlice is [MergeToBatchFn] that add item to a slice.
func AddToSlice[T any](b []T, item T) []T {
	return append(b, item)
}

// InitMap is [InitBatchFn] that allocate a map.
// It uses the default size for map, as the size of item may be much larger than the size of map after merged.
// However, if you properly configured [WithBatchCounter] to count the size of map and [WithMaxItem] to a reasonable value,
// you may benefit from specifying the size of map using your own [InitBatchFn].
func InitMap[K comparable, V any](i int64) map[K]V {
	if i == 0 {
		return make(map[K]V, 1)
	}
	return make(map[K]V)
}

// ExtractFn is a function to extract value from item.
type ExtractFn[T any, V any] func(T) V

// CombineFn is a function to combine two values in to one.
type CombineFn[T any] func(T, T) T

// MergeToMapUsing create [MergeToBatchFn]
// that add item to map using [KeyVal] [ExtractFn] and apply [CombineFn] if key duplicated.
// The original value will be passed as 1st parameter to the [CombineFn].
// If [CombineFn] is nil, duplicated key will be replaced.
func MergeToMapUsing[T any, K comparable, V any](extractor ExtractFn[T, KeyVal[K, V]], combiner CombineFn[V]) MergeToBatchFn[map[K]V, T] {
	if combiner == nil {
		return func(m map[K]V, t T) map[K]V {
			kv := extractor(t)
			m[kv.key] = kv.val
			return m
		}
	}

	return func(m map[K]V, t T) map[K]V {
		kv := extractor(t)
		if v, ok := m[kv.key]; ok {
			m[kv.key] = combiner(v, kv.val)
			return m
		}
		m[kv.key] = kv.val
		return m
	}
}

// MergeSelfToMapUsing create a [MergeToBatchFn] that add self as item to map using key [ExtractFn]
// and apply [CombineFn] if key duplicated.
// The original value will be passed as 1st parameter to the [CombineFn].
// If [CombineFn] is nil, duplicated key will be replaced.
func MergeSelfToMapUsing[T any, K comparable](keyExtractor ExtractFn[T, K], combiner CombineFn[T]) MergeToBatchFn[map[K]T, T] {
	if combiner == nil {
		return func(m map[K]T, t T) map[K]T {
			key := keyExtractor(t)
			m[key] = t
			return m
		}
	}

	return func(m map[K]T, t T) map[K]T {
		key := keyExtractor(t)
		if v, ok := m[key]; ok {
			m[key] = combiner(v, t)
			return m
		}
		m[key] = t
		return m
	}
}

// InitType is an [InitBatchFn] that allocate a type T.
func InitType[T any](_ int64) T {
	t := new(T)
	return *t
}

// SplitSliceEqually create a [SplitBatchFn] that split a slice into multiple equal chuck.
func SplitSliceEqually[T any, I Size](numberOfChunk I) SplitBatchFn[[]T] {
	return func(b []T, _ int64) [][]T {
		batches := make([][]T, numberOfChunk)
		for i := 0; i < len(b); i++ {
			bucket := int64(i) % int64(numberOfChunk)
			batches[bucket] = append(batches[bucket], b[i])
		}
		return batches
	}
}

// SplitSliceSizeLimit create a [SplitBatchFn] that split a slice into multiple chuck of limited size.
func SplitSliceSizeLimit[T any, I Size](maxSizeOfChunk I) SplitBatchFn[[]T] {
	return func(b []T, i int64) [][]T {
		size := i / int64(maxSizeOfChunk)
		if i%int64(maxSizeOfChunk) != 0 {
			size++
		}
		batches := make([][]T, size)
		index := 0
		for batchI := 0; batchI < len(batches); batchI++ {
			batch := batches[batchI]
			for ; index < len(b); index++ {
				batch = append(batch, b[index])
			}
			batches[batchI] = batch
		}
		return batches
	}
}

// NewSliceProcessor prepare a processor that backed by a slice.
func NewSliceProcessor[T any]() ProcessorSetup[T, []T] {
	return NewProcessor(InitSlice[T], AddToSlice[T])
}

// NewMapProcessor prepare a processor that backed by a map.
// If [CombineFn] is nil, duplicated key will be replaced.
func NewMapProcessor[T any, K comparable, V any](extractor ExtractFn[T, KeyVal[K, V]], combiner CombineFn[V]) ProcessorSetup[T, map[K]V] {
	return NewProcessor(InitMap[K, V], MergeToMapUsing(extractor, combiner))
}

// NewIdentityMapProcessor prepare a processor that backed by a map, using item as value without extracting.
// If [CombineFn] is nil, duplicated key will be replaced.
func NewIdentityMapProcessor[T any, K comparable](keyExtractor ExtractFn[T, K], combiner CombineFn[T]) ProcessorSetup[T, map[K]T] {
	return NewProcessor(InitMap[K, T], MergeSelfToMapUsing(keyExtractor, combiner))
}
