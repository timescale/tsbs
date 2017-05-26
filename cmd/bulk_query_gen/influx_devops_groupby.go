package main

import "time"

// InfluxDevopsGroupby produces Influx-specific queries for the devops groupby case.
type InfluxDevopsGroupby struct {
	InfluxDevops
	numMetrics int
}

// NewInfluxDevopsGroupBy produces a function that produces a new InfluxDevops
func NewInfluxDevopsGroupBy(numMetrics int) func(DatabaseConfig, time.Time, time.Time) QueryGenerator {
	return func(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
		underlying := newInfluxDevopsCommon(dbConfig, start, end).(*InfluxDevops)
		return &InfluxDevopsGroupby{
			InfluxDevops: *underlying,
			numMetrics:   numMetrics,
		}
	}
}

// Dispatch fills in the Query
func (d *InfluxDevopsGroupby) Dispatch(i, scaleVar int) Query {
	q := NewHTTPQuery() // from pool
	d.MeanCPUUsageDayByHourAllHostsGroupbyHost(q, d.numMetrics)
	return q
}
