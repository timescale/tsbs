package main

import "time"

// TimescaleDBDevops5Metrics1Host1Hr produces TimescaleDB-specific query for the devops 5-metrics, 1 host, 1 hr
type TimescaleDBDevops5Metrics1Host1Hr struct {
	TimescaleDBDevops
}

// NewTimescaleDBDevops5Metrics1Host1Hr creates a new query for the given db and time
func NewTimescaleDBDevops5Metrics1Host1Hr(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newTimescaleDBDevopsCommon(dbConfig, start, end).(*TimescaleDBDevops)
	return &TimescaleDBDevops5Metrics1Host1Hr{
		TimescaleDBDevops: *underlying,
	}
}

func (d *TimescaleDBDevops5Metrics1Host1Hr) Dispatch(_, scaleVar int) Query {
	q := NewTimescaleDBQuery() // from pool
	d.CPU5MetricsHourByMinuteOneHost(q, scaleVar)
	return q
}

// TimescaleDBDevops5Metrics1Host12Hrs produces TimescaleDB-specific query for the devops 5-metrics, 1 host, 12 hrs
type TimescaleDBDevops5Metrics1Host12Hrs struct {
	TimescaleDBDevops
}

// NewTimescaleDBDevops5Metrics1Host12Hrs creates a new query for the given db and time
func NewTimescaleDBDevops5Metrics1Host12Hrs(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newTimescaleDBDevopsCommon(dbConfig, start, end).(*TimescaleDBDevops)
	return &TimescaleDBDevops5Metrics1Host12Hrs{
		TimescaleDBDevops: *underlying,
	}
}

func (d *TimescaleDBDevops5Metrics1Host12Hrs) Dispatch(_, scaleVar int) Query {
	q := NewTimescaleDBQuery() // from pool
	d.CPU5Metrics12HoursByMinuteOneHost(q, scaleVar)
	return q
}

// TimescaleDBDevops5Metrics8Hosts1Hr produces TimescaleDB-specific query for the devops 5-metrics, 8 hosts, 1 hr
type TimescaleDBDevops5Metrics8Hosts1Hr struct {
	TimescaleDBDevops
}

// NewTimescaleDBDevops5Metrics8Hosts1Hr creates a new query for the given db and time
func NewTimescaleDBDevops5Metrics8Hosts1Hr(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newTimescaleDBDevopsCommon(dbConfig, start, end).(*TimescaleDBDevops)
	return &TimescaleDBDevops5Metrics8Hosts1Hr{
		TimescaleDBDevops: *underlying,
	}
}

func (d *TimescaleDBDevops5Metrics8Hosts1Hr) Dispatch(_, scaleVar int) Query {
	q := NewTimescaleDBQuery() // from pool
	d.CPU5MetricsHourByMinuteEightHosts(q, scaleVar)
	return q
}
