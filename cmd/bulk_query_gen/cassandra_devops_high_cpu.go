package main

import "time"

// CassandraDevopsHighCPU produces Cassandra-specific queries for the devops "high CPU"
type CassandraDevopsHighCPU struct {
	CassandraDevops
}

func NewCassandraDevopsHighCPU(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newCassandraDevopsCommon(dbConfig, start, end).(*CassandraDevops)
	return &CassandraDevopsHighCPU{
		CassandraDevops: *underlying,
	}
}

func (d *CassandraDevopsHighCPU) Dispatch(i, scaleVar int) Query {
	q := NewCassandraQuery() // from pool
	d.HighCPU(q, scaleVar)
	return q
}

type CassandraDevopsHighCPUAndField struct {
	CassandraDevops
}

func NewCassandraDevopsHighCPUAndField(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newCassandraDevopsCommon(dbConfig, start, end).(*CassandraDevops)
	return &CassandraDevopsHighCPUAndField{
		CassandraDevops: *underlying,
	}
}

func (d *CassandraDevopsHighCPUAndField) Dispatch(i, scaleVar int) Query {
	q := NewCassandraQuery() // from pool
	d.HighCPUAndField(q, scaleVar)
	return q
}
