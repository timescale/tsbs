package main

import "time"

// TimescaleDBDevopsAllMaxCPU contains info for TimescaleDB-devops test 'cpu-max-all-*'
type TimescaleDBDevopsAllMaxCPU struct {
	TimescaleDBDevops
	hosts int
}

// NewTimescaleDBDevopsAllMaxCPU produces a new function that produces a new TimescaleDBDevopsAllMaxCPU
func NewTimescaleDBDevopsAllMaxCPU(hosts int) func(DatabaseConfig, time.Time, time.Time) QueryGenerator {
	return func(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
		underlying := newTimescaleDBDevopsCommon(dbConfig, start, end).(*TimescaleDBDevops)
		return &TimescaleDBDevopsAllMaxCPU{
			TimescaleDBDevops: *underlying,
			hosts:             hosts,
		}
	}
}

// Dispatch fills in the Query
func (d *TimescaleDBDevopsAllMaxCPU) Dispatch(_, scaleVar int) Query {
	q := NewTimescaleDBQuery() // from pool
	d.MaxAllCPU(q, scaleVar, d.hosts)
	return q
}
