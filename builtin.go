package batch

import (
	"log/slog"
)

// KeyVal is a tuple of key and value.
type KeyVal[K any, V any] struct {
	key K
	val V
}

// InitBatchFn function to create an empty batch.
// Accept max item limit can be 1 (disabled), -1 (unlimited) or any positive number.
// The InitBatchFn must never panic.
type InitBatchFn[B any] func(int64) B

// MergeToBatchFn function to add an item to the batch.
// The MergeToBatchFn must never panic.
type MergeToBatchFn[B any, T any] func(B, T) B

// SplitBatchFn function to split a batch into multiple smaller batches.
// Accept the current batch and the input count.
// The SplitBatchFn must never panic.
type SplitBatchFn[B any] func(B, int64) []B

// CountBatchFn function to count the number of items in a batch.
// The CountBatchFn must never panic.
type CountBatchFn[B any] func(B) int64

// ProcessBatchFn function to process a batch.
// Accept the current batch and the input count.
type ProcessBatchFn[B any] func(B, int64) error

// RecoverBatchFn function to handle an error batch.
// Each RecoverBatchFn can further return an error to enable the next RecoverBatchFn in the chain.
// The RecoverBatchFn must never panic.
//
// Accept the current batch and a counter with the previous error.
// The counter and batch can be controlled by returning an [Error],
// otherwise it will receive the same arguments of [ProcessBatchFn].
type RecoverBatchFn[B any] func(B, int64, error) error

// LoggingErrorHandler default error handler,
// it is included in [RecoverBatchFn] chain if there is no other error handler.
//
// Can be disabled by using [WithBatchErrorHandlers] with an empty slice or [WithDisabledDefaultProcessErrorLog].
func LoggingErrorHandler[B any](_ B, count int64, err error) error {
	slog.Error("error processing batch", slog.Any("count", count), slog.Any("err", err))
	return err
}

// NewSliceProcessor prepare a processor that backed by a slice.
func NewSliceProcessor[T any]() ProcessorSetup[T, []T] {
	return NewProcessor(InitSlice[T], addToSlice[T])
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

// addToSlice is [MergeToBatchFn] that add item to a slice.
func addToSlice[T any](b []T, item T) []T {
	return append(b, item)
}

// NewMapProcessor prepare a processor that backed by a map.
// If [CombineFn] is nil, the duplicated key will be replaced.
func NewMapProcessor[T any, K comparable, V any](extractor ExtractFn[T, KeyVal[K, V]], combiner CombineFn[V]) ProcessorSetup[T, map[K]V] {
	return NewProcessor(InitMap[K, V], addToMapUsing(extractor, combiner))
}

// NewSelfMapProcessor prepare a processor that backed by a map, using item as value without extracting.
// If [CombineFn] is nil, the duplicated key will be replaced.
func NewSelfMapProcessor[T any, K comparable](keyExtractor ExtractFn[T, K], combiner CombineFn[T]) ProcessorSetup[T, map[K]T] {
	return NewProcessor(InitMap[K, T], addSelfToMapUsing(keyExtractor, combiner))
}

// InitMap is [InitBatchFn] that allocate a map.
// It uses the default size for the map, as the size of items may be much larger than the size of the map after merged.
// However, if you properly configured [WithBatchCounter] to count the size of the map
// and [WithMaxItem] to a reasonable value,
// you may benefit from specifying the size of the map using your own [InitBatchFn].
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

// addToMapUsing create [MergeToBatchFn]
// that add item to the map using [KeyVal] [ExtractFn] and apply [CombineFn] if key duplicated.
// The original value will be passed as the 1st parameter to the [CombineFn].
// If [CombineFn] is nil, any duplicated key will be replaced.
func addToMapUsing[T any, K comparable, V any](extractor ExtractFn[T, KeyVal[K, V]], combiner CombineFn[V]) MergeToBatchFn[map[K]V, T] {
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

// addSelfToMapUsing create a [MergeToBatchFn] that add self as item to map using key [ExtractFn]
// and apply [CombineFn] if key duplicated.
// The original value will be passed as the 1st parameter to the [CombineFn].
// If [CombineFn] is nil, any duplicated key will be replaced.
func addSelfToMapUsing[T any, K comparable](keyExtractor ExtractFn[T, K], combiner CombineFn[T]) MergeToBatchFn[map[K]T, T] {
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
