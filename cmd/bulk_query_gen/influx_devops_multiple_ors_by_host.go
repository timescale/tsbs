package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// InfluxDevopsSingleHost produces Influx-specific queries for the devops single-host case.
type InfluxDevopsMultipleOrsByHost struct {
	InfluxDevops
}

func NewInfluxDevopsMultipleOrsByHost(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newInfluxDevopsCommon(dbConfig, start, end).(*InfluxDevops)
	return &InfluxDevopsMultipleOrsByHost{
		InfluxDevops: *underlying,
	}
}

func (d *InfluxDevopsMultipleOrsByHost) Dispatch(i, scaleVar int) query.Query {
	q := query.NewHTTP() // from pool
	d.MultipleMemFieldsOrsGroupedByHost(q, scaleVar)
	return q
}
