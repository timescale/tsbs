package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// InfluxDevopsGroupby produces Influx-specific queries for the devops groupby case.
type InfluxDevopsGroupby struct {
	InfluxDevops
	numMetrics int
}

// NewInfluxDevopsGroupBy produces a function that produces a new InfluxDevopsGroupby for the given parameters
func NewInfluxDevopsGroupBy(numMetrics int) func(DatabaseConfig, time.Time, time.Time) QueryGenerator {
	return func(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
		underlying := newInfluxDevopsCommon(dbConfig, start, end).(*InfluxDevops)
		return &InfluxDevopsGroupby{
			InfluxDevops: *underlying,
			numMetrics:   numMetrics,
		}
	}
}

// Dispatch fills in the query.Query
func (d *InfluxDevopsGroupby) Dispatch(i, scaleVar int) query.Query {
	q := query.NewHTTP() // from pool
	d.MeanCPUUsageDayByHourAllHostsGroupbyHost(q, d.numMetrics)
	return q
}
