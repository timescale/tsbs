package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// CassandraDevopsGroupby produces Cassandra-specific queries for the devops groupby case.
type CassandraDevopsGroupby struct {
	CassandraDevops
}

// NewCassandraDevopsGroupBy produces a function that produces a new CassandraDevopsGroupby for the given parameters
func NewCassandraDevopsGroupBy(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newCassandraDevopsCommon(dbConfig, start, end).(*CassandraDevops)
	return &CassandraDevopsGroupby{
		CassandraDevops: *underlying,
	}

}

// Dispatch fills in the query.Query
func (d *CassandraDevopsGroupby) Dispatch(i, scaleVar int) query.Query {
	q := query.NewCassandra() // from pool
	d.MeanCPUUsageDayByHourAllHostsGroupbyHost(q, scaleVar)
	return q
}
