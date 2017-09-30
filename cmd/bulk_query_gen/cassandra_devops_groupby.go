package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// CassandraDevopsGroupby produces Cassandra-specific queries for the devops groupby case.
type CassandraDevopsGroupby struct {
	CassandraDevops
	numMetrics int
}

// NewCassandraDevopsGroupBy produces a function that produces a new CassandraDevopsGroupby for the given parameters
func NewCassandraDevopsGroupBy(numMetrics int) func(time.Time, time.Time) QueryGenerator {
	return func(start, end time.Time) QueryGenerator {
		underlying := newCassandraDevopsCommon(start, end).(*CassandraDevops)
		return &CassandraDevopsGroupby{
			CassandraDevops: *underlying,
			numMetrics:      numMetrics,
		}
	}
}

// Dispatch fills in the query.Query
func (d *CassandraDevopsGroupby) Dispatch(i, scaleVar int) query.Query {
	q := query.NewCassandra() // from pool
	d.MeanCPUUsageDayByHourAllHostsGroupbyHost(q, d.numMetrics)
	return q
}
