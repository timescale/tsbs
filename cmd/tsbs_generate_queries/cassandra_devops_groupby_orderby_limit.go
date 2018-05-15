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
func NewCassandraDevopsGroupByOrderByLimit(start, end time.Time, scale int) QueryGenerator {
	underlying := newCassandraDevopsCommon(start, end, scale)
	return &CassandraDevopsGroupByOrderByLimit{
		CassandraDevops: *underlying,
	}
}

// Dispatch fills in the query.Query
func (d *CassandraDevopsGroupByOrderByLimit) Dispatch() query.Query {
	q := query.NewCassandra() // from pool
	d.GroupByOrderByLimit(q)
	return q
}
