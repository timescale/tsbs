package main

import "time"

// TimescaleDBDevops8Hosts produces TimescaleDB-specific queries for the devops groupby case.
type TimescaleDBDevops8Hosts struct {
	TimescaleDBDevops
}

func NewTimescaleDBDevops8Hosts(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newTimescaleDBDevopsCommon(dbConfig, start, end).(*TimescaleDBDevops)
	return &TimescaleDBDevops8Hosts{
		TimescaleDBDevops: *underlying,
	}
}

func (d *TimescaleDBDevops8Hosts) Dispatch(_, scaleVar int) Query {
	q := NewTimescaleDBQuery() // from pool
	d.MaxCPUUsageHourByMinuteEightHosts(q, scaleVar)
	return q
}
