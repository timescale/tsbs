package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// CassandraDevopsHighCPU produces Cassandra-specific queries for the devops high-cpu cases
type CassandraDevopsHighCPU struct {
	CassandraDevops
	hosts int
}

// NewCassandraDevopsHighCPU produces a new function that produces a new CassandraDevopsHighCPU
func NewCassandraDevopsHighCPU(hosts int) func(DatabaseConfig, time.Time, time.Time) QueryGenerator {
	return func(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
		underlying := newCassandraDevopsCommon(dbConfig, start, end).(*CassandraDevops)
		return &CassandraDevopsHighCPU{
			CassandraDevops: *underlying,
			hosts:           hosts,
		}
	}
}

// Dispatch fills in the query.Query
func (d *CassandraDevopsHighCPU) Dispatch(_, scaleVar int) query.Query {
	q := query.NewCassandra() // from pool
	d.HighCPUForHosts(q, scaleVar, d.hosts)
	return q
}
