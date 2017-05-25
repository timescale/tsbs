package main

import "time"

// TimescaleDBDevopsGroupby produces TimescaleDB-specific queries for the devops groupby case.
type TimescaleDBDevopsGroupby struct {
	TimescaleDBDevops
	numMetrics int
}

// NewTimescaleDBDevopsGroupBy produces a function that produces a new TimescaleDBDevops
func NewTimescaleDBDevopsGroupBy(numMetrics int) func(DatabaseConfig, time.Time, time.Time) QueryGenerator {
	return func(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
		underlying := newTimescaleDBDevopsCommon(dbConfig, start, end).(*TimescaleDBDevops)
		return &TimescaleDBDevopsGroupby{
			TimescaleDBDevops: *underlying,
			numMetrics:        numMetrics,
		}
	}
}

// Dispatch fills in the Query
func (d *TimescaleDBDevopsGroupby) Dispatch(i, scaleVar int) Query {
	q := NewTimescaleDBQuery() // from pool
	d.MeanCPUMetricsDayByHourAllHostsGroupbyHost(q, d.numMetrics)
	return q
}
