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
func NewCassandraDevopsSingleMetric(hosts, hours int) QueryGeneratorMaker {
	return func(start, end time.Time) QueryGenerator {
		underlying := newCassandraDevopsCommon(start, end)
		return &CassandraDevopsSingleMetric{
			CassandraDevops: *underlying,
			hosts:           hosts,
			hours:           hours,
		}
	}
}

// Dispatch fills in the query.Query
func (d *CassandraDevopsSingleMetric) Dispatch(scaleVar int) query.Query {
	q := query.NewCassandra() // from pool
	d.MaxCPUMetricsByMinute(q, scaleVar, d.hosts, 1, time.Duration(int64(d.hours)*int64(time.Hour)))
	return q
}
