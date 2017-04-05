package main

import "time"

// TimescaleDBDevopsSingleHost produces TimescaleDB-specific queries for the devops single-host case.
type TimescaleDBDevopsSingleHost12hr struct {
	TimescaleDBDevops
}

func NewTimescaleDBDevopsSingleHost12hr(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newTimescaleDBDevopsCommon(dbConfig, start, end).(*TimescaleDBDevops)
	return &TimescaleDBDevopsSingleHost12hr{
		TimescaleDBDevops: *underlying,
	}
}

func (d *TimescaleDBDevopsSingleHost12hr) Dispatch(i, scaleVar int) Query {
	q := NewTimescaleDBQuery() // from pool
	d.MaxCPUUsage12HoursByMinuteOneHost(q, scaleVar)
	return q
}
