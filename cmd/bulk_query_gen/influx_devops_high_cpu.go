package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// InfluxDevopsHighCPU produces Influx-specific queries for the devops high-cpu cases
type InfluxDevopsHighCPU struct {
	InfluxDevops
	hosts int
}

// NewInfluxDevopsHighCPU produces a new function that produces a new InfluxDevopsHighCPU
func NewInfluxDevopsHighCPU(hosts int) QueryGeneratorMaker {
	return func(start, end time.Time) QueryGenerator {
		underlying := newInfluxDevopsCommon(start, end)
		return &InfluxDevopsHighCPU{
			InfluxDevops: *underlying,
			hosts:        hosts,
		}
	}
}

// Dispatch fills in the query.Query
func (d *InfluxDevopsHighCPU) Dispatch(scaleVar int) query.Query {
	q := query.NewHTTP() // from pool
	d.HighCPUForHosts(q, scaleVar, d.hosts)
	return q
}
