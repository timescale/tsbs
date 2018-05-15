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
func NewInfluxDevopsAllMaxCPU(hosts int) QueryGeneratorMaker {
	return func(start, end time.Time, scale int) QueryGenerator {
		underlying := newInfluxDevopsCommon(start, end, scale)
		return &InfluxDevopsAllMaxCPU{
			InfluxDevops: *underlying,
			hosts:        hosts,
		}
	}
}

// Dispatch fills in the query.Query
func (d *InfluxDevopsAllMaxCPU) Dispatch() query.Query {
	q := query.NewHTTP() // from pool
	d.MaxAllCPU(q, d.hosts)
	return q
}
