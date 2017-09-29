package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// TimescaleDBDevopsGroupByOrderByLimit produces TimescaleDB-specific queries for the devops groupby-orderby-limit case.
type TimescaleDBDevopsGroupByOrderByLimit struct {
	TimescaleDBDevops
}

// NewTimescaleDBDevopsGroupByOrderByLimit returns a new TimescaleDBDevopsGroupByOrderByLimit for given paremeters
func NewTimescaleDBDevopsGroupByOrderByLimit(start, end time.Time) QueryGenerator {
	underlying := newTimescaleDBDevopsCommon(start, end).(*TimescaleDBDevops)
	return &TimescaleDBDevopsGroupByOrderByLimit{
		TimescaleDBDevops: *underlying,
	}
}

// Dispatch fills in the query.Query
func (d *TimescaleDBDevopsGroupByOrderByLimit) Dispatch(i, scaleVar int) query.Query {
	q := query.NewTimescaleDB() // from pool
	d.GroupByOrderByLimit(q, scaleVar)
	return q
}
