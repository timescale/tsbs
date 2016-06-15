package main

import "time"

// CassandraDevops8Hosts produces Cassandra-specific queries for the devops groupby case.
type CassandraDevops8Hosts struct {
	CassandraDevops
}

func NewCassandraDevops8Hosts(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newCassandraDevopsCommon(dbConfig, start, end).(*CassandraDevops)
	return &CassandraDevops8Hosts{
		CassandraDevops: *underlying,
	}
}

func (d *CassandraDevops8Hosts) Dispatch(_, scaleVar int) Query {
	q := NewCassandraQuery() // from pool
	d.MaxCPUUsageHourByMinuteEightHosts(q, scaleVar)
	return q
}
