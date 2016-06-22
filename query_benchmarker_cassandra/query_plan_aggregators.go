package main

import "fmt"

// Type Aggregator merges QueryPlan results on the client in constant time.
// This is intended to match the aggregation that a CQLQuery performs on a
// Cassandra server.
//
// Note that the underlying functions should be commutative and associative.
type Aggregator interface {
	Put(float64)
	Get() float64
}

// AggregatorMax aggregates the maximum of a stream of values.
type AggregatorMax struct {
	value float64
	count int64
}

// Put puts a value for finding the maximum.
func (a *AggregatorMax) Put(n float64) {
	if n > a.value || a.count == 0 {
		a.value = n
	}
	a.count++
}

// Get computes the aggregated maximum.
func (a *AggregatorMax) Get() float64 {
	if a.count == 0 {
		return 0
	}
	return a.value
}

// AggregatorMax aggregates the minimum of a stream of values.
type AggregatorMin struct {
	value float64
	count int64
}

// Put puts a value for finding the minimum.
func (a *AggregatorMin) Put(n float64) {
	if n < a.value || a.count == 0 {
		a.value = n
	}
	a.count++
}

// Get computes the aggregated minimum.
func (a *AggregatorMin) Get() float64 {
	return a.value
}

// AggregatorMax aggregates the average of a stream of values.
type AggregatorAvg struct {
	value float64
	count int64
}

// Put puts a value for averaging.
func (a *AggregatorAvg) Put(n float64) {
	a.value += n
	a.count++
}

// Get computes the aggregated average.
func (a *AggregatorAvg) Get() float64 {
	if a.count == 0 {
		return 0
	}
	return a.value / float64(a.count)
}

// GetConstantSpaceAggr translates a label into a new ConstantSpaceAggr.
func GetAggregator(label string) (Aggregator, error) {
	// TODO(rw): fewer heap allocations here.
	switch label {
	case "min":
		return &AggregatorMin{}, nil
	case "max":
		return &AggregatorMax{}, nil
	case "avg":
		return &AggregatorAvg{}, nil
	default:
		return nil, fmt.Errorf("invalid aggregation specifier")
	}
}
