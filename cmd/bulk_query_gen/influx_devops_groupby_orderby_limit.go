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
func NewInfluxDevopsGroupByOrderByLimit(start, end time.Time) QueryGenerator {
	underlying := newInfluxDevopsCommon(start, end).(*InfluxDevops)
	return &InfluxDevopsGroupByOrderByLimit{
		InfluxDevops: *underlying,
	}
}

// Dispatch fills in the query.Query
func (d *InfluxDevopsGroupByOrderByLimit) Dispatch(i, scaleVar int) query.Query {
	q := query.NewHTTP() // from pool
	d.GroupByOrderByLimit(q, scaleVar)
	return q
}
