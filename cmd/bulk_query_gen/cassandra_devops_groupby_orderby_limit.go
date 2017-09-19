package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// CassandraDevopsGroupByOrderByLimit produces Cassandra-specific queries for the devops groupby-orderby-limit case.
type CassandraDevopsGroupByOrderByLimit struct {
	CassandraDevops
}

// NewCassandraDevopsGroupByOrderByLimit returns a new CassandraDevopsGroupByOrderByLimit for given paremeters
func NewCassandraDevopsGroupByOrderByLimit(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newCassandraDevopsCommon(start, end).(*CassandraDevops)
	return &CassandraDevopsGroupByOrderByLimit{
		CassandraDevops: *underlying,
	}
}

// Dispatch fills in the query.Query
func (d *CassandraDevopsGroupByOrderByLimit) Dispatch(i, scaleVar int) query.Query {
	q := query.NewCassandra() // from pool
	d.GroupByOrderByLimit(q, scaleVar)
	return q
}
