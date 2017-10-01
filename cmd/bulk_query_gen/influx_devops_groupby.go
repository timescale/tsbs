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
func NewInfluxDevopsGroupBy(numMetrics int) QueryGeneratorMaker {
	return func(start, end time.Time) QueryGenerator {
		underlying := newInfluxDevopsCommon(start, end)
		return &InfluxDevopsGroupby{
			InfluxDevops: *underlying,
			numMetrics:   numMetrics,
		}
	}
}

// Dispatch fills in the query.Query
func (d *InfluxDevopsGroupby) Dispatch(scaleVar int) query.Query {
	q := query.NewHTTP() // from pool
	d.MeanCPUMetricsDayByHourAllHostsGroupbyHost(q, d.numMetrics)
	return q
}
