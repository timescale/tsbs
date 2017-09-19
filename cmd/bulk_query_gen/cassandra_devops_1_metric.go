package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// CassandraDevopsSingleMetric contains info for Cassandra-devops test '*-hosts-*-hrs'
type CassandraDevopsSingleMetric struct {
	CassandraDevops
	hosts int
	hours int
}

// NewCassandraDevopsSingleMetric produces a new function that produces a new CassandraDevopsSingleMetric
func NewCassandraDevopsSingleMetric(hosts, hours int) func(DatabaseConfig, time.Time, time.Time) QueryGenerator {
	return func(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
		underlying := newCassandraDevopsCommon(start, end).(*CassandraDevops)
		return &CassandraDevopsSingleMetric{
			CassandraDevops: *underlying,
			hosts:           hosts,
			hours:           hours,
		}
	}
}

// Dispatch fills in the query.Query
func (d *CassandraDevopsSingleMetric) Dispatch(_, scaleVar int) query.Query {
	q := query.NewCassandra() // from pool
	d.MaxCPUUsageHourByMinute(q, scaleVar, d.hosts, time.Duration(int64(d.hours)*int64(time.Hour)))
	return q
}
