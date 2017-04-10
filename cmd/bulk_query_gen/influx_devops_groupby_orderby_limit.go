package main

import "time"

// InfluxDevopsGroupByOrderByLimit produces queries for group by with limits
type InfluxDevopsGroupByOrderByLimit struct {
	InfluxDevops
}

func NewInfluxDevopsGroupByOrderByLimit(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newInfluxDevopsCommon(dbConfig, start, end).(*InfluxDevops)
	return &InfluxDevopsGroupByOrderByLimit{
		InfluxDevops: *underlying,
	}
}

func (d *InfluxDevopsGroupByOrderByLimit) Dispatch(i, scaleVar int) Query {
	q := NewHTTPQuery() // from pool
	d.GroupByOrderByLimit(q, scaleVar)
	return q
}
