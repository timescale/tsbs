package main

import "fmt"

// Type AggrFunc is a function that merges QueryPlan results on the client.
// This is intended to match the aggregation that a CQLQuery performs on a
// Cassandra server.
//
// Note that these functions should be both commutative and associative.
type AggrFunc func([]float64) float64

// minAggr gets the minimum of a slice of float64s.
func minAggr(nn []float64) float64 {
	if len(nn) == 0 {
		return 0
	}

	x := nn[0]
	for _, n := range nn[1:] {
		if n < x {
			x = n
		}
	}
	return x
}

// maxAggr gets the maximum of a slice of float64s.
func maxAggr(nn []float64) float64 {
	if len(nn) == 0 {
		return 0
	}

	x := nn[0]
	for _, n := range nn[1:] {
		if n > x {
			x = n
		}
	}
	return x
}

// avgAggr gets the average of a slice of float64s.
func avgAggr(nn []float64) float64 {
	if len(nn) == 0 {
		return 0
	}
	var sum float64
	for _, n := range nn {
		sum += n
	}

	avg := sum / float64(len(nn))
	return avg
}

// GetAggrFunc translates a label into a valid AggrFunc by looking it up in a
// map.
func GetAggrFunc(label string) (AggrFunc, error) {
	m := map[string]AggrFunc{
		"min": minAggr,
		"max": maxAggr,
		"avg": avgAggr,
	}
	f, ok := m[string(label)]
	if !ok {
		return nil, fmt.Errorf("invalid aggregation specifier")
	}
	return f, nil
}
