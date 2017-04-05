package main

import "time"

// TimescaleDBDevopsSingleHost produces TimescaleDB-specific queries for the devops single-host case.
type TimescaleDBDevopsSingleHost struct {
	TimescaleDBDevops
}

func NewTimescaleDBDevopsSingleHost(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newTimescaleDBDevopsCommon(dbConfig, start, end).(*TimescaleDBDevops)
	return &TimescaleDBDevopsSingleHost{
		TimescaleDBDevops: *underlying,
	}
}

func (d *TimescaleDBDevopsSingleHost) Dispatch(i, scaleVar int) Query {
	q := NewTimescaleDBQuery() // from pool
	d.MaxCPUUsageHourByMinuteOneHost(q, scaleVar)
	return q
}
