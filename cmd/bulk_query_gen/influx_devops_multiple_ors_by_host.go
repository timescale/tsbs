package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// InfluxDevopsSingleHost produces Influx-specific queries for the devops single-host case.
type InfluxDevopsMultipleOrsByHost struct {
	InfluxDevops
}

func NewInfluxDevopsMultipleOrsByHost(start, end time.Time) QueryGenerator {
	underlying := newInfluxDevopsCommon(start, end)
	return &InfluxDevopsMultipleOrsByHost{
		InfluxDevops: *underlying,
	}
}

func (d *InfluxDevopsMultipleOrsByHost) Dispatch(scaleVar int) query.Query {
	q := query.NewHTTP() // from pool
	d.MultipleMemFieldsOrsGroupedByHost(q, scaleVar)
	return q
}
