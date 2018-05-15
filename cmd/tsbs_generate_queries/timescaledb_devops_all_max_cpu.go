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
func NewTimescaleDBDevopsAllMaxCPU(hosts int) QueryGeneratorMaker {
	return func(start, end time.Time, scale int) QueryGenerator {
		underlying := newTimescaleDBDevopsCommon(start, end, scale)
		return &TimescaleDBDevopsAllMaxCPU{
			TimescaleDBDevops: *underlying,
			hosts:             hosts,
		}
	}
}

// Dispatch fills in the query.Query
func (d *TimescaleDBDevopsAllMaxCPU) Dispatch() query.Query {
	q := query.NewTimescaleDB() // from pool
	d.MaxAllCPU(q, d.hosts)
	return q
}
