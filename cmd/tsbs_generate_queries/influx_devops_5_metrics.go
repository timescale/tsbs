package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// InfluxDevops5Metrics contains info for Influx-devops test '5-metrics-*'
type InfluxDevops5Metrics struct {
	InfluxDevops
	hosts int
	hours int
}

// NewInfluxDevops5Metrics produces a new function that produces a new InfluxDevops5Metrics
func NewInfluxDevops5Metrics(hosts, hours int) QueryGeneratorMaker {
	return func(start, end time.Time) QueryGenerator {
		underlying := newInfluxDevopsCommon(start, end)
		return &InfluxDevops5Metrics{
			InfluxDevops: *underlying,
			hosts:        hosts,
			hours:        hours,
		}
	}
}

// Dispatch fills in the query.Query
func (d *InfluxDevops5Metrics) Dispatch(scaleVar int) query.Query {
	q := query.NewHTTP() // from pool
	d.MaxCPUMetricsByMinute(q, scaleVar, d.hosts, 5, time.Duration(int64(d.hours)*int64(time.Hour)))
	return q
}
