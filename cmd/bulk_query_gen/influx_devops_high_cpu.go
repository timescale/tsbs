package main

import "time"

// InfluxDevopsSingleHost produces Influx-specific queries for the devops single-host case.
type InfluxDevopsHighCPU struct {
	InfluxDevops
}

func NewInfluxDevopsHighCPU(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newInfluxDevopsCommon(dbConfig, start, end).(*InfluxDevops)
	return &InfluxDevopsHighCPU{
		InfluxDevops: *underlying,
	}
}

func (d *InfluxDevopsHighCPU) Dispatch(i, scaleVar int) Query {
	q := NewHTTPQuery() // from pool
	d.HighCPU(q, scaleVar)
	return q
}
