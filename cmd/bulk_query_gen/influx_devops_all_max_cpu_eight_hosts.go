package main

import "time"

// InfluxDevopsSingleHost produces Influx-specific queries for the devops single-host case.
type InfluxDevopsAllMaxCPUEightHosts struct {
	InfluxDevops
}

func NewInfluxDevopsAllMaxCPUEightHosts(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newInfluxDevopsCommon(dbConfig, start, end).(*InfluxDevops)
	return &InfluxDevopsAllMaxCPUEightHosts{
		InfluxDevops: *underlying,
	}
}

func (d *InfluxDevopsAllMaxCPUEightHosts) Dispatch(i, scaleVar int) Query {
	q := NewHTTPQuery() // from pool
	d.MaxAllCPUHourByMinuteEightHosts(q, scaleVar)
	return q
}
