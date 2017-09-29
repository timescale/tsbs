package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// InfluxDevopsSingleMetric contains info for Influx-devops test '*-hosts-*-hrs'
type InfluxDevopsSingleMetric struct {
	InfluxDevops
	hosts int
	hours int
}

// NewInfluxDevopsSingleMetric produces a new function that produces a new InfluxDevopsSingleMetric
func NewInfluxDevopsSingleMetric(hosts, hours int) func(time.Time, time.Time) QueryGenerator {
	return func(start, end time.Time) QueryGenerator {
		underlying := newInfluxDevopsCommon(start, end).(*InfluxDevops)
		return &InfluxDevopsSingleMetric{
			InfluxDevops: *underlying,
			hosts:        hosts,
			hours:        hours,
		}
	}
}

// Dispatch fills in the query.Query
func (d *InfluxDevopsSingleMetric) Dispatch(_, scaleVar int) query.Query {
	q := query.NewHTTP() // from pool
	d.MaxCPUUsageHourByMinute(q, scaleVar, d.hosts, time.Duration(int64(d.hours)*int64(time.Hour)))
	return q
}
