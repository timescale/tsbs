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
func NewTimescaleDBDevopsSingleMetric(hosts, hours int) func(DatabaseConfig, time.Time, time.Time) QueryGenerator {
	return func(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
		underlying := newTimescaleDBDevopsCommon(start, end).(*TimescaleDBDevops)
		return &TimescaleDBDevopsSingleMetric{
			TimescaleDBDevops: *underlying,
			hosts:             hosts,
			hours:             hours,
		}
	}
}

// Dispatch fills in the query.Query
func (d *TimescaleDBDevopsSingleMetric) Dispatch(_, scaleVar int) query.Query {
	q := query.NewTimescaleDB() // from pool
	d.MaxCPUUsageHourByMinute(q, scaleVar, d.hosts, time.Duration(int64(d.hours)*int64(time.Hour)))
	return q
}
