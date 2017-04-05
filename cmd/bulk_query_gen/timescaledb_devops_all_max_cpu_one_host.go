package main

import "time"

// TimescaleDBDevopsSingleHost produces TimescaleDB-specific queries for the devops single-host case.
type TimescaleDBDevopsAllMaxCPUOneHost struct {
	TimescaleDBDevops
}

func NewTimescaleDBDevopsAllMaxCPUOneHost(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newTimescaleDBDevopsCommon(dbConfig, start, end).(*TimescaleDBDevops)
	return &TimescaleDBDevopsAllMaxCPUOneHost{
		TimescaleDBDevops: *underlying,
	}
}

func (d *TimescaleDBDevopsAllMaxCPUOneHost) Dispatch(i, scaleVar int) Query {
	q := NewTimescaleDBQuery() // from pool
	d.MaxAllCPUHourByMinuteOneHost(q, scaleVar)
	return q
}
