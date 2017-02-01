package main

import "time"

// InfluxDevopsSingleHost produces Influx-specific queries for the devops single-host case.
type InfluxDevopsHighCPUAndField struct {
	InfluxDevops
}

func NewInfluxDevopsHighCPUAndField(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newInfluxDevopsCommon(dbConfig, start, end).(*InfluxDevops)
	return &InfluxDevopsHighCPU{
		InfluxDevops: *underlying,
	}
}

func (d *InfluxDevopsHighCPUAndField) Dispatch(i, scaleVar int) Query {
	q := NewHTTPQuery() // from pool
	d.HighCPUAndField(q, scaleVar)
	return q
}
