package main

import "time"

// InfluxDevopsSingleHost produces Influx-specific queries for the devops single-host case.
type InfluxDevopsLastPointPerHost struct {
	InfluxDevops
}

func NewInfluxDevopsLastPointPerHost(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newInfluxDevopsCommon(dbConfig, start, end).(*InfluxDevops)
	return &InfluxDevopsLastPointPerHost{
		InfluxDevops: *underlying,
	}
}

func (d *InfluxDevopsLastPointPerHost) Dispatch(i, scaleVar int) Query {
	q := NewHTTPQuery() // from pool
	d.LastPointPerHost(q, scaleVar)
	return q
}
