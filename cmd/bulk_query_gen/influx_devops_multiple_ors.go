package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// InfluxDevopsSingleHost produces Influx-specific queries for the devops single-host case.
type InfluxDevopsMultipleOrs struct {
	InfluxDevops
}

func NewInfluxDevopsMultipleOrs(start, end time.Time) QueryGenerator {
	underlying := newInfluxDevopsCommon(start, end).(*InfluxDevops)
	return &InfluxDevopsMultipleOrs{
		InfluxDevops: *underlying,
	}
}

func (d *InfluxDevopsMultipleOrs) Dispatch(i, scaleVar int) query.Query {
	q := query.NewHTTP() // from pool
	d.MultipleMemFieldsOrs(q, scaleVar)
	return q
}
