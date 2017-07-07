package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// InfluxDevopsAllMaxCPU contains info for Influx-devops test 'cpu-max-all-*'
type InfluxDevopsAllMaxCPU struct {
	InfluxDevops
	hosts int
}

// NewInfluxDevopsAllMaxCPU produces a new function that produces a new InfluxDevopsAllMaxCPU
func NewInfluxDevopsAllMaxCPU(hosts int) func(DatabaseConfig, time.Time, time.Time) QueryGenerator {
	return func(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
		underlying := newInfluxDevopsCommon(dbConfig, start, end).(*InfluxDevops)
		return &InfluxDevopsAllMaxCPU{
			InfluxDevops: *underlying,
			hosts:        hosts,
		}
	}
}

// Dispatch fills in the query.Query
func (d *InfluxDevopsAllMaxCPU) Dispatch(_, scaleVar int) query.Query {
	q := query.NewHTTP() // from pool
	d.MaxAllCPU(q, scaleVar, d.hosts)
	return q
}
