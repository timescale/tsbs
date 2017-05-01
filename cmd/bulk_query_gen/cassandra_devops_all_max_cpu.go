package main

import "time"

// NewCassandraDevopsAllMaxCPUHosts is a factory method for getting the constructor of a AllMaxCPU QueryGenerator
func NewCassandraDevopsAllMaxCPUHosts(hosts int) func(DatabaseConfig, time.Time, time.Time) QueryGenerator {
	if hosts == 1 {
		return newCassandraDevopsAllMaxCPUOneHost
	} else if hosts == 8 {
		return newCassandraDevopsAllMaxCPUEightHosts
	} else {
		panic("unknown number of hosts: " + string(hosts))
	}
}

// CassandraDevopsAllMaxCPUOneHost produces Cassandra-specific queries for the devops single-host case.
type CassandraDevopsAllMaxCPUOneHost struct {
	CassandraDevops
}

func newCassandraDevopsAllMaxCPUOneHost(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newCassandraDevopsCommon(dbConfig, start, end).(*CassandraDevops)
	return &CassandraDevopsAllMaxCPUOneHost{
		CassandraDevops: *underlying,
	}
}

func (d *CassandraDevopsAllMaxCPUOneHost) Dispatch(i, scaleVar int) Query {
	q := NewCassandraQuery() // from pool
	d.MaxAllCPUOneHost(q, scaleVar)
	return q
}

// CassandraDevopsAllMaxCPUEightHosts produces Cassandra-specific queries for the devops single-host case.
type CassandraDevopsAllMaxCPUEightHosts struct {
	CassandraDevops
}

func newCassandraDevopsAllMaxCPUEightHosts(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newCassandraDevopsCommon(dbConfig, start, end).(*CassandraDevops)
	return &CassandraDevopsAllMaxCPUEightHosts{
		CassandraDevops: *underlying,
	}
}

func (d *CassandraDevopsAllMaxCPUEightHosts) Dispatch(i, scaleVar int) Query {
	q := NewCassandraQuery() // from pool
	d.MaxAllCPUEightHosts(q, scaleVar)
	return q
}
