package main

import "time"

// InfluxDevopsSingleHost produces Influx-specific queries for the devops single-host case.
type InfluxDevopsMultipleOrs struct {
	InfluxDevops
}

func NewInfluxDevopsMultipleOrs(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newInfluxDevopsCommon(dbConfig, start, end).(*InfluxDevops)
	return &InfluxDevopsMultipleOrs{
		InfluxDevops: *underlying,
	}
}

func (d *InfluxDevopsMultipleOrs) Dispatch(i, scaleVar int) Query {
	q := NewHTTPQuery() // from pool
	d.MultipleMemFieldsOrs(q, scaleVar)
	return q
}
