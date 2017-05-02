package main

import "time"

// CassandraDevopsSingleHost produces Cassandra-specific queries for the devops single-host case.
type CassandraDevopsGroupByOrderByLimit struct {
	CassandraDevops
}

func NewCassandraDevopsGroupByOrderByLimit(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newCassandraDevopsCommon(dbConfig, start, end).(*CassandraDevops)
	return &CassandraDevopsGroupByOrderByLimit{
		CassandraDevops: *underlying,
	}
}

func (d *CassandraDevopsGroupByOrderByLimit) Dispatch(i, scaleVar int) Query {
	q := NewCassandraQuery() // from pool
	d.GroupByOrderByLimit(q, scaleVar)
	return q
}
