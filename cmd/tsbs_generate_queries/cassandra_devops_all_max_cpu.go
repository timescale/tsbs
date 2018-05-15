package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// CassandraDevopsAllMaxCPU contains info for Cassandra-devops test 'cpu-max-all-*'
type CassandraDevopsAllMaxCPU struct {
	CassandraDevops
	hosts int
}

// NewCassandraDevopsAllMaxCPU produces a new function that produces a new CassandraDevopsAllMaxCPU
func NewCassandraDevopsAllMaxCPU(hosts int) QueryGeneratorMaker {
	return func(start, end time.Time, scale int) QueryGenerator {
		underlying := newCassandraDevopsCommon(start, end, scale)
		return &CassandraDevopsAllMaxCPU{
			CassandraDevops: *underlying,
			hosts:           hosts,
		}
	}
}

// Dispatch fills in the query.Query
func (d *CassandraDevopsAllMaxCPU) Dispatch() query.Query {
	q := query.NewCassandra() // from pool
	d.MaxAllCPU(q, d.hosts)
	return q
}
