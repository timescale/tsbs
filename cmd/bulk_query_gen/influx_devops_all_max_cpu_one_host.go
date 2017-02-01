package main

import "time"

// InfluxDevopsSingleHost produces Influx-specific queries for the devops single-host case.
type InfluxDevopsAllMaxCPUOneHost struct {
	InfluxDevops
}

func NewInfluxDevopsAllMaxCPUOneHost(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newInfluxDevopsCommon(dbConfig, start, end).(*InfluxDevops)
	return &InfluxDevopsAllMaxCPUOneHost{
		InfluxDevops: *underlying,
	}
}

func (d *InfluxDevopsAllMaxCPUOneHost) Dispatch(i, scaleVar int) Query {
	q := NewHTTPQuery() // from pool
	d.MaxAllCPUHourByMinuteOneHost(q, scaleVar)
	return q
}
