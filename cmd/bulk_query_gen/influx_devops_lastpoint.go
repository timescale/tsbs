package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// InfluxDevopsLastPointPerHost produces Influx-specific queries for the devops lastpoint case
type InfluxDevopsLastPointPerHost struct {
	InfluxDevops
}

// NewInfluxDevopsLastPointPerHost returns a new InfluxDevopsLastPointPerHost for given paremeters
func NewInfluxDevopsLastPointPerHost(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newInfluxDevopsCommon(dbConfig, start, end).(*InfluxDevops)
	return &InfluxDevopsLastPointPerHost{
		InfluxDevops: *underlying,
	}
}

// Dispatch fills in the query.Query
func (d *InfluxDevopsLastPointPerHost) Dispatch(i, scaleVar int) query.Query {
	q := query.NewHTTP() // from pool
	d.LastPointPerHost(q, scaleVar)
	return q
}
