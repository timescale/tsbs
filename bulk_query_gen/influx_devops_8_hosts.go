package main

import "time"

// InfluxDevops8Hosts produces Influx-specific queries for the devops groupby case.
type InfluxDevops8Hosts struct {
	InfluxDevops
}

func NewInfluxDevops8Hosts(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newInfluxDevopsCommon(dbConfig, start, end).(*InfluxDevops)
	return &InfluxDevops8Hosts{
		InfluxDevops: *underlying,
	}
}

func (d *InfluxDevops8Hosts) Dispatch(_, scaleVar int) Query {
	q := NewHTTPQuery() // from pool
	d.MaxCPUUsageHourByMinuteEightHosts(q, scaleVar)
	return q
}
