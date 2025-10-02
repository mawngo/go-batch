package batch

import (
	"log/slog"
)

// KeyVal is a tuple of key and value.
type KeyVal[K any, V any] struct {
	key K
	val V
}

// InitBatchFn function to create empty batch.
// Accept max item limit, can be 1 (disabled), -1 (unlimited) or any positive number.
type InitBatchFn[B any] func(int64) B

// MergeToBatchFn function to add an item to batch.
type MergeToBatchFn[B any, T any] func(B, T) B

// SplitBatchFn function to split a batch into multiple smaller batches.
// Accept the current batch and the input count.
// The SplitBatchFn must never panic.
type SplitBatchFn[B any] func(B, int64) []B

// ProcessBatchFn function to process a batch.
// Accept the current batch and the input count.
type ProcessBatchFn[B any] func(B, int64) error

// RecoverBatchFn function to handle an error batch.
// Each RecoverBatchFn can further return error to enable the next RecoverBatchFn in the chain.
// The RecoverBatchFn must never panic.
//
// Accept the current batch and a counter with the previous error.
// The counter and batch can be controlled by returning an [Error],
// otherwise it will receive the same arguments of [ProcessBatchFn].
type RecoverBatchFn[B any] func(B, int64, error) error

// LoggingErrorHandler default error handler, always included in [RecoverBatchFn] chain unless disable.
func LoggingErrorHandler[B any](_ B, count int64, err error) error {
	slog.Error("error processing batch", slog.Any("count", count), slog.Any("err", err))
	return err
}

// InitSlice is [InitBatchFn] that allocate a slice.
// It is recommended to use [NewSliceProcessor] instead if possible.
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
// It is recommended to use [NewSliceProcessor] instead if possible.
func AddToSlice[T any](b []T, item T) []T {
	return append(b, item)
}

// ToSlice create [InitBatchFn] and [MergeToBatchFn] that allocate a slice and add item to it.
// A shortcut for [InitSlice] and [AddToSlice].
func ToSlice[T any]() (InitBatchFn[[]T], MergeToBatchFn[[]T, T]) {
	return InitSlice[T], AddToSlice[T]
}

// InitMap is [InitBatchFn] that allocate a map.
// It uses the default size for map, as the size of item may be much larger than the size of map after merged.
// However, if you properly configured [WithBatchCounter] to count the size of map and [WithMaxItem] to a reasonable value,
// you may benefit from specifying the size of map using your own [InitBatchFn].
// It is recommended to use [NewMapProcessor] instead if possible.
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

// AddToMapUsing create [MergeToBatchFn]
// that add item to map using [KeyVal] [ExtractFn] and apply [CombineFn] if key duplicated.
// The original value will be passed as 1st parameter to the [CombineFn].
// If [CombineFn] is nil, duplicated key will be replaced.
// It is recommended to use [NewMapProcessor] instead if possible.
func AddToMapUsing[T any, K comparable, V any](extractor ExtractFn[T, KeyVal[K, V]], combiner CombineFn[V]) MergeToBatchFn[map[K]V, T] {
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

// AddSelfToMapUsing create a [MergeToBatchFn] that add self as item to map using key [ExtractFn]
// and apply [CombineFn] if key duplicated.
// The original value will be passed as 1st parameter to the [CombineFn].
// If [CombineFn] is nil, duplicated key will be replaced.
// It is recommended to use [NewSelfMapProcessor]  instead if possible.
func AddSelfToMapUsing[T any, K comparable](keyExtractor ExtractFn[T, K], combiner CombineFn[T]) MergeToBatchFn[map[K]T, T] {
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
func SplitSliceEqually[T any, I size](numberOfChunk I) SplitBatchFn[[]T] {
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
func SplitSliceSizeLimit[T any, I size](maxSizeOfChunk I) SplitBatchFn[[]T] {
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

// CountMapKeys create a counter that count keys in map.
func CountMapKeys[V any, K comparable]() func(map[K]V, int64) int64 {
	return func(m map[K]V, _ int64) int64 {
		return int64(len(m))
	}
}

// NewSliceProcessor prepare a processor that backed by a slice.
func NewSliceProcessor[T any]() ProcessorSetup[T, []T] {
	return NewProcessor(InitSlice[T], AddToSlice[T])
}

// NewMapProcessor prepare a processor that backed by a map.
// If [CombineFn] is nil, duplicated key will be replaced.
func NewMapProcessor[T any, K comparable, V any](extractor ExtractFn[T, KeyVal[K, V]], combiner CombineFn[V]) ProcessorSetup[T, map[K]V] {
	return NewProcessor(InitMap[K, V], AddToMapUsing(extractor, combiner))
}

// NewSelfMapProcessor prepare a processor that backed by a map, using item as value without extracting.
// If [CombineFn] is nil, duplicated key will be replaced.
func NewSelfMapProcessor[T any, K comparable](keyExtractor ExtractFn[T, K], combiner CombineFn[T]) ProcessorSetup[T, map[K]T] {
	return NewProcessor(InitMap[K, T], AddSelfToMapUsing(keyExtractor, combiner))
}
