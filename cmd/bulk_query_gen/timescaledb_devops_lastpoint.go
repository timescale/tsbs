package main

import "time"

// TimescaleDBDevopsGroupby produces TimescaleDB-specific queries for the devops groupby case.
type TimescaleDBDevopsLastPointPerHost struct {
	TimescaleDBDevops
}

func NewTimescaleDBDevopsLastPointPerHost(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newTimescaleDBDevopsCommon(dbConfig, start, end).(*TimescaleDBDevops)
	return &TimescaleDBDevopsLastPointPerHost{
		TimescaleDBDevops: *underlying,
	}

}

func (d *TimescaleDBDevopsLastPointPerHost) Dispatch(i, scaleVar int) Query {
	q := NewTimescaleDBQuery() // from pool
	d.LastPointPerHost(q, scaleVar)
	return q
}
