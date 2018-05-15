package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// TimescaleDBDevopsHighCPU produces TimescaleDB-specific queries for the devops high-cpu cases
type TimescaleDBDevopsHighCPU struct {
	TimescaleDBDevops
	hosts int
}

// NewTimescaleDBDevopsHighCPU produces a new function that produces a new TimescaleDBDevopsHighCPU
func NewTimescaleDBDevopsHighCPU(hosts int) QueryGeneratorMaker {
	return func(start, end time.Time, scale int) QueryGenerator {
		underlying := newTimescaleDBDevopsCommon(start, end, scale)
		return &TimescaleDBDevopsHighCPU{
			TimescaleDBDevops: *underlying,
			hosts:             hosts,
		}
	}
}

// Dispatch fills in the query.Query
func (d *TimescaleDBDevopsHighCPU) Dispatch() query.Query {
	q := query.NewTimescaleDB() // from pool
	d.HighCPUForHosts(q, d.hosts)
	return q
}
