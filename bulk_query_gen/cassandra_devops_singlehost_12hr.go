package main

import "time"

// CassandraDevopsSingleHost12hr produces Cassandra-specific queries for the devops single-host case.
type CassandraDevopsSingleHost12hr struct {
	CassandraDevops
}

func NewCassandraDevopsSingleHost12hr(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newCassandraDevopsCommon(dbConfig, start, end).(*CassandraDevops)
	return &CassandraDevopsSingleHost12hr{
		CassandraDevops: *underlying,
	}
}

func (d *CassandraDevopsSingleHost12hr) Dispatch(_, scaleVar int) Query {
	q := NewCassandraQuery() // from pool
	d.MaxCPUUsage12HoursByMinuteOneHost(q, scaleVar)
	return q
}
