package main

import "time"

// TimescaleDBDevopsSingleHost produces TimescaleDB-specific queries for the devops single-host case.
type TimescaleDBDevopsGroupByOrderByLimit struct {
	TimescaleDBDevops
}

func NewTimescaleDBDevopsGroupByOrderByLimit(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newTimescaleDBDevopsCommon(dbConfig, start, end).(*TimescaleDBDevops)
	return &TimescaleDBDevopsGroupByOrderByLimit{
		TimescaleDBDevops: *underlying,
	}
}

func (d *TimescaleDBDevopsGroupByOrderByLimit) Dispatch(i, scaleVar int) Query {
	q := NewTimescaleDBQuery() // from pool
	d.GroupByOrderByLimit(q, scaleVar)
	return q
}
