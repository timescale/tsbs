package main

import "time"

// CassandraDevopsGroupby produces Cassandra-specific queries for the devops groupby case.
type CassandraDevopsLastPointPerHost struct {
	CassandraDevops
}

func NewCassandraDevopsLastPointPerHost(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newCassandraDevopsCommon(dbConfig, start, end).(*CassandraDevops)
	return &CassandraDevopsLastPointPerHost{
		CassandraDevops: *underlying,
	}

}

func (d *CassandraDevopsLastPointPerHost) Dispatch(i, scaleVar int) Query {
	q := NewCassandraQuery() // from pool
	d.LastPointPerHost(q, scaleVar)
	return q
}
