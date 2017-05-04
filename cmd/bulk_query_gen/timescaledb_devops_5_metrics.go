package main

import "time"

// TimescaleDBDevops5Metrics contains info for TimescaleDB-devops test '5-metrics-*'
type TimescaleDBDevops5Metrics struct {
	TimescaleDBDevops
	hosts int
	hours int
}

// NewTimescaleDBDevops5Metrics produces a new function that produces a new TimescaleDBDevops5Metrics
func NewTimescaleDBDevops5Metrics(hosts, hours int) func(DatabaseConfig, time.Time, time.Time) QueryGenerator {
	return func(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
		underlying := newTimescaleDBDevopsCommon(dbConfig, start, end).(*TimescaleDBDevops)
		return &TimescaleDBDevops5Metrics{
			TimescaleDBDevops: *underlying,
			hosts:             hosts,
			hours:             hours,
		}
	}
}

// Dispatch fills in the Query
func (d *TimescaleDBDevops5Metrics) Dispatch(_, scaleVar int) Query {
	q := NewTimescaleDBQuery() // from pool
	d.CPU5Metrics(q, scaleVar, d.hosts, time.Duration(int64(d.hours)*int64(time.Hour)))
	return q
}
