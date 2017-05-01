package main

import "time"

// CassandraDevops5Metrics1Host1Hr produces Cassandra-specific query for the devops 5-metrics, 1 host, 1 hr
type CassandraDevops5Metrics1Host1Hr struct {
	CassandraDevops
}

// NewCassandraDevops5Metrics1Host1Hr creates a new query for the given db and time
func NewCassandraDevops5Metrics1Host1Hr(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newCassandraDevopsCommon(dbConfig, start, end).(*CassandraDevops)
	return &CassandraDevops5Metrics1Host1Hr{
		CassandraDevops: *underlying,
	}
}

func (d *CassandraDevops5Metrics1Host1Hr) Dispatch(_, scaleVar int) Query {
	q := NewCassandraQuery() // from pool
	d.CPU5MetricsHourByMinuteOneHost(q, scaleVar)
	return q
}

// CassandraDevops5Metrics1Host12Hrs produces Cassandra-specific query for the devops 5-metrics, 1 host, 12 hrs
type CassandraDevops5Metrics1Host12Hrs struct {
	CassandraDevops
}

// NewCassandraDevops5Metrics1Host12Hrs creates a new query for the given db and time
func NewCassandraDevops5Metrics1Host12Hrs(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newCassandraDevopsCommon(dbConfig, start, end).(*CassandraDevops)
	return &CassandraDevops5Metrics1Host12Hrs{
		CassandraDevops: *underlying,
	}
}

func (d *CassandraDevops5Metrics1Host12Hrs) Dispatch(_, scaleVar int) Query {
	q := NewCassandraQuery() // from pool
	d.CPU5Metrics12HoursByMinuteOneHost(q, scaleVar)
	return q
}

// CassandraDevops5Metrics8Hosts1Hr produces Cassandra-specific query for the devops 5-metrics, 8 hosts, 1 hr
type CassandraDevops5Metrics8Hosts1Hr struct {
	CassandraDevops
}

// NewCassandraDevops5Metrics8Hosts1Hr creates a new query for the given db and time
func NewCassandraDevops5Metrics8Hosts1Hr(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newCassandraDevopsCommon(dbConfig, start, end).(*CassandraDevops)
	return &CassandraDevops5Metrics8Hosts1Hr{
		CassandraDevops: *underlying,
	}
}

func (d *CassandraDevops5Metrics8Hosts1Hr) Dispatch(_, scaleVar int) Query {
	q := NewCassandraQuery() // from pool
	d.CPU5MetricsHourByMinuteEightHosts(q, scaleVar)
	return q
}
