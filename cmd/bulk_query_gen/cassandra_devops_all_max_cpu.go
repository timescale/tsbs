package main

import "time"

// CassandraDevopsAllMaxCPU contains info for Cassandra-devops test 'cpu-max-all-*'
type CassandraDevopsAllMaxCPU struct {
	CassandraDevops
	hosts int
}

// NewCassandraDevopsAllMaxCPU produces a new function that produces a new CassandraDevopsAllMaxCPU
func NewCassandraDevopsAllMaxCPU(hosts int) func(DatabaseConfig, time.Time, time.Time) QueryGenerator {
	return func(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
		underlying := newCassandraDevopsCommon(dbConfig, start, end).(*CassandraDevops)
		return &CassandraDevopsAllMaxCPU{
			CassandraDevops: *underlying,
			hosts:           hosts,
		}
	}
}

// Dispatch fills in the Query
func (d *CassandraDevopsAllMaxCPU) Dispatch(_, scaleVar int) Query {
	q := NewCassandraQuery() // from pool
	d.MaxAllCPU(q, scaleVar, d.hosts)
	return q
}
