package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// TimescaleDBDevops5Metrics contains info for TimescaleDB-devops test '5-metrics-*'
type TimescaleDBDevops5Metrics struct {
	TimescaleDBDevops
	hosts int
	hours int
}

// NewTimescaleDBDevops5Metrics produces a new function that produces a new TimescaleDBDevops5Metrics
func NewTimescaleDBDevops5Metrics(hosts, hours int) func(time.Time, time.Time) QueryGenerator {
	return func(start, end time.Time) QueryGenerator {
		underlying := newTimescaleDBDevopsCommon(start, end).(*TimescaleDBDevops)
		return &TimescaleDBDevops5Metrics{
			TimescaleDBDevops: *underlying,
			hosts:             hosts,
			hours:             hours,
		}
	}
}

// Dispatch fills in the query.Query
func (d *TimescaleDBDevops5Metrics) Dispatch(_, scaleVar int) query.Query {
	q := query.NewTimescaleDB() // from pool
	d.CPU5Metrics(q, scaleVar, d.hosts, time.Duration(int64(d.hours)*int64(time.Hour)))
	return q
}
