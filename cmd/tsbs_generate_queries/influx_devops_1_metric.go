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
func NewInfluxDevopsSingleMetric(hosts, hours int) QueryGeneratorMaker {
	return func(start, end time.Time) QueryGenerator {
		underlying := newInfluxDevopsCommon(start, end)
		return &InfluxDevopsSingleMetric{
			InfluxDevops: *underlying,
			hosts:        hosts,
			hours:        hours,
		}
	}
}

// Dispatch fills in the query.Query
func (d *InfluxDevopsSingleMetric) Dispatch(scaleVar int) query.Query {
	q := query.NewHTTP() // from pool
	d.MaxCPUMetricsByMinute(q, scaleVar, d.hosts, 1, time.Duration(int64(d.hours)*int64(time.Hour)))
	return q
}
