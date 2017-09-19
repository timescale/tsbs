package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// TimescaleDBDevopsLastPointPerHost produces TimescaleDB-specific queries for the devops lastpoint case
type TimescaleDBDevopsLastPointPerHost struct {
	TimescaleDBDevops
}

// NewTimescaleDBDevopsLastPointPerHost returns a new TimescaleDBDevopsLastPointPerHost for given paremeters
func NewTimescaleDBDevopsLastPointPerHost(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newTimescaleDBDevopsCommon(start, end).(*TimescaleDBDevops)
	return &TimescaleDBDevopsLastPointPerHost{
		TimescaleDBDevops: *underlying,
	}

}

// Dispatch fills in the query.Query
func (d *TimescaleDBDevopsLastPointPerHost) Dispatch(i, scaleVar int) query.Query {
	q := query.NewTimescaleDB() // from pool
	d.LastPointPerHost(q, scaleVar)
	return q
}
