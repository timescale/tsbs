package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// CassandraDevopsLastPointPerHost produces Cassandra-specific queries for the devops lastpoint case
type CassandraDevopsLastPointPerHost struct {
	CassandraDevops
}

// NewCassandraDevopsLastPointPerHost returns a new CassandraDevopsLastPointPerHost for given paremeters
func NewCassandraDevopsLastPointPerHost(start, end time.Time, scale int) QueryGenerator {
	underlying := newCassandraDevopsCommon(start, end, scale)
	return &CassandraDevopsLastPointPerHost{
		CassandraDevops: *underlying,
	}

}

// Dispatch fills in the query.Query
func (d *CassandraDevopsLastPointPerHost) Dispatch() query.Query {
	q := query.NewCassandra() // from pool
	d.LastPointPerHost(q)
	return q
}
