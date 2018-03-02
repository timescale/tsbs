package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// TimescaleDBDevopsSingleHost produces TimescaleDB-specific queries for the devops single-host case.
type TimescaleDBDevopsMultipleOrsByHost struct {
	TimescaleDBDevops
}

func NewTimescaleDBDevopsMultipleOrsByHost(start, end time.Time) QueryGenerator {
	underlying := newTimescaleDBDevopsCommon(start, end)
	return &TimescaleDBDevopsMultipleOrsByHost{
		TimescaleDBDevops: *underlying,
	}
}

func (d *TimescaleDBDevopsMultipleOrsByHost) Dispatch(scaleVar int) query.Query {
	q := query.NewTimescaleDB() // from pool
	d.MultipleMemOrsByHost(q, scaleVar)
	return q
}
