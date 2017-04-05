package main

import "time"

// TimescaleDBDevopsSingleHost produces TimescaleDB-specific queries for the devops single-host case.
type TimescaleDBDevopsAllMaxCPUEightHosts struct {
	TimescaleDBDevops
}

func NewTimescaleDBDevopsAllMaxCPUEightHosts(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newTimescaleDBDevopsCommon(dbConfig, start, end).(*TimescaleDBDevops)
	return &TimescaleDBDevopsAllMaxCPUEightHosts{
		TimescaleDBDevops: *underlying,
	}
}

func (d *TimescaleDBDevopsAllMaxCPUEightHosts) Dispatch(i, scaleVar int) Query {
	q := NewTimescaleDBQuery() // from pool
	d.MaxAllCPUHourByMinuteEightHosts(q, scaleVar)
	return q
}
