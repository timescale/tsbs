package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// InfluxDevopsGroupByOrderByLimit produces queries for group by with limits
type InfluxDevopsGroupByOrderByLimit struct {
	InfluxDevops
}

// NewInfluxDevopsGroupByOrderByLimit returns a new InfluxDevopsGroupByOrderByLimit for given paremeters
func NewInfluxDevopsGroupByOrderByLimit(start, end time.Time, scale int) QueryGenerator {
	underlying := newInfluxDevopsCommon(start, end, scale)
	return &InfluxDevopsGroupByOrderByLimit{
		InfluxDevops: *underlying,
	}
}

// Dispatch fills in the query.Query
func (d *InfluxDevopsGroupByOrderByLimit) Dispatch() query.Query {
	q := query.NewHTTP() // from pool
	d.GroupByOrderByLimit(q)
	return q
}
