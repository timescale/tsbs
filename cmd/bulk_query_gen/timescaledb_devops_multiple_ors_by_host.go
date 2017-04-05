package main

import "time"

// TimescaleDBDevopsSingleHost produces TimescaleDB-specific queries for the devops single-host case.
type TimescaleDBDevopsMultipleOrsByHost struct {
	TimescaleDBDevops
}

func NewTimescaleDBDevopsMultipleOrsByHost(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newTimescaleDBDevopsCommon(dbConfig, start, end).(*TimescaleDBDevops)
	return &TimescaleDBDevopsMultipleOrsByHost{
		TimescaleDBDevops: *underlying,
	}
}

func (d *TimescaleDBDevopsMultipleOrsByHost) Dispatch(i, scaleVar int) Query {
	q := NewTimescaleDBQuery() // from pool
	d.MultipleMemOrsByHost(q, scaleVar)
	return q
}
