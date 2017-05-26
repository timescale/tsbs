package main

import "time"

// InfluxDevopsHighCPU produces Influx-specific queries for the devops high-cpu cases
type InfluxDevopsHighCPU struct {
	InfluxDevops
	hosts int
}

// NewInfluxDevopsHighCPU produces a new function that produces a new InfluxDevopsHighCPU
func NewInfluxDevopsHighCPU(hosts int) func(DatabaseConfig, time.Time, time.Time) QueryGenerator {
	return func(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
		underlying := newInfluxDevopsCommon(dbConfig, start, end).(*InfluxDevops)
		return &InfluxDevopsHighCPU{
			InfluxDevops: *underlying,
			hosts:        hosts,
		}
	}
}

// Dispatch fills in the Query
func (d *InfluxDevopsHighCPU) Dispatch(_, scaleVar int) Query {
	q := NewHTTPQuery() // from pool
	d.HighCPUForHosts(q, scaleVar, d.hosts)
	return q
}
