package main

import "time"

// TimescaleDBDevopsLastPointPerHost produces TimescaleDB-specific queries for the devops lastpoint case
type TimescaleDBDevopsLastPointPerHost struct {
	TimescaleDBDevops
}

// NewTimescaleDBDevopsLastPointPerHost returns a new TimescaleDBDevopsLastPointPerHost for given paremeters
func NewTimescaleDBDevopsLastPointPerHost(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newTimescaleDBDevopsCommon(dbConfig, start, end).(*TimescaleDBDevops)
	return &TimescaleDBDevopsLastPointPerHost{
		TimescaleDBDevops: *underlying,
	}

}

// Dispatch fills in the Query
func (d *TimescaleDBDevopsLastPointPerHost) Dispatch(i, scaleVar int) Query {
	q := NewTimescaleDBQuery() // from pool
	d.LastPointPerHost(q, scaleVar)
	return q
}
