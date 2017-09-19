package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// TimescaleDBDevopsAllMaxCPU contains info for TimescaleDB-devops test 'cpu-max-all-*'
type TimescaleDBDevopsAllMaxCPU struct {
	TimescaleDBDevops
	hosts int
}

// NewTimescaleDBDevopsAllMaxCPU produces a new function that produces a new TimescaleDBDevopsAllMaxCPU
func NewTimescaleDBDevopsAllMaxCPU(hosts int) func(DatabaseConfig, time.Time, time.Time) QueryGenerator {
	return func(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
		underlying := newTimescaleDBDevopsCommon(start, end).(*TimescaleDBDevops)
		return &TimescaleDBDevopsAllMaxCPU{
			TimescaleDBDevops: *underlying,
			hosts:             hosts,
		}
	}
}

// Dispatch fills in the query.Query
func (d *TimescaleDBDevopsAllMaxCPU) Dispatch(_, scaleVar int) query.Query {
	q := query.NewTimescaleDB() // from pool
	d.MaxAllCPU(q, scaleVar, d.hosts)
	return q
}
