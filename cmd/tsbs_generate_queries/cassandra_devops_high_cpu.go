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
func NewCassandraDevopsHighCPU(hosts int) QueryGeneratorMaker {
	return func(start, end time.Time, scale int) QueryGenerator {
		underlying := newCassandraDevopsCommon(start, end, scale)
		return &CassandraDevopsHighCPU{
			CassandraDevops: *underlying,
			hosts:           hosts,
		}
	}
}

// Dispatch fills in the query.Query
func (d *CassandraDevopsHighCPU) Dispatch() query.Query {
	q := query.NewCassandra() // from pool
	d.HighCPUForHosts(q, d.hosts)
	return q
}
