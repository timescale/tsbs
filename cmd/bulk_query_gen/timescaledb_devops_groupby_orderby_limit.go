package main

import "time"

// TimescaleDBDevopsGroupByOrderByLimit produces TimescaleDB-specific queries for the devops groupby-orderby-limit case.
type TimescaleDBDevopsGroupByOrderByLimit struct {
	TimescaleDBDevops
}

// NewTimescaleDBDevopsGroupByOrderByLimit returns a new TimescaleDBDevopsGroupByOrderByLimit for given paremeters
func NewTimescaleDBDevopsGroupByOrderByLimit(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newTimescaleDBDevopsCommon(dbConfig, start, end).(*TimescaleDBDevops)
	return &TimescaleDBDevopsGroupByOrderByLimit{
		TimescaleDBDevops: *underlying,
	}
}

// Dispatch fills in the Query
func (d *TimescaleDBDevopsGroupByOrderByLimit) Dispatch(i, scaleVar int) Query {
	q := NewTimescaleDBQuery() // from pool
	d.GroupByOrderByLimit(q, scaleVar)
	return q
}
