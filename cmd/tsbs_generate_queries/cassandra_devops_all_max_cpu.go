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
	return func(start, end time.Time) QueryGenerator {
		underlying := newCassandraDevopsCommon(start, end)
		return &CassandraDevopsAllMaxCPU{
			CassandraDevops: *underlying,
			hosts:           hosts,
		}
	}
}

// Dispatch fills in the query.Query
func (d *CassandraDevopsAllMaxCPU) Dispatch(scaleVar int) query.Query {
	q := query.NewCassandra() // from pool
	d.MaxAllCPU(q, scaleVar, d.hosts)
	return q
}
