package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// TimescaleDBDevopsSingleMetric contains info for TimescaleDB-devops test '*-hosts-*-hrs'
type TimescaleDBDevopsSingleMetric struct {
	TimescaleDBDevops
	hosts int
	hours int
}

// NewTimescaleDBDevopsSingleMetric produces a new function that produces a new TimescaleDBDevopsSingleMetric
func NewTimescaleDBDevopsSingleMetric(hosts, hours int) QueryGeneratorMaker {
	return func(start, end time.Time, scale int) QueryGenerator {
		underlying := newTimescaleDBDevopsCommon(start, end, scale)
		return &TimescaleDBDevopsSingleMetric{
			TimescaleDBDevops: *underlying,
			hosts:             hosts,
			hours:             hours,
		}
	}
}

// Dispatch fills in the query.Query
func (d *TimescaleDBDevopsSingleMetric) Dispatch() query.Query {
	q := query.NewTimescaleDB() // from pool
	d.MaxCPUMetricsByMinute(q, d.hosts, 1, time.Duration(int64(d.hours)*int64(time.Hour)))
	return q
}
