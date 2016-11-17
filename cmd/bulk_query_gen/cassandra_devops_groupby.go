package main

import "time"

// CassandraDevopsGroupby produces Cassandra-specific queries for the devops groupby case.
type CassandraDevopsGroupby struct {
	CassandraDevops
}

func NewCassandraDevopsGroupBy(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newCassandraDevopsCommon(dbConfig, start, end).(*CassandraDevops)
	return &CassandraDevopsGroupby{
		CassandraDevops: *underlying,
	}

}

func (d *CassandraDevopsGroupby) Dispatch(i, scaleVar int) Query {
	q := NewCassandraQuery() // from pool
	d.MeanCPUUsageDayByHourAllHostsGroupbyHost(q, scaleVar)
	return q
}

