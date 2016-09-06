package main

import "time"

// InfluxDevopsSingleHost12hr produces Influx-specific queries for the devops single-host case over a 12hr period.
type InfluxDevopsSingleHost12hr struct {
	InfluxDevops
}

func NewInfluxDevopsSingleHost12hr(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newInfluxDevopsCommon(dbConfig, start, end).(*InfluxDevops)
	return &InfluxDevopsSingleHost12hr{
		InfluxDevops: *underlying,
	}
}

func (d *InfluxDevopsSingleHost12hr) Dispatch(i, scaleVar int) Query {
	q := NewHTTPQuery() // from pool
	d.MaxCPUUsage12HoursByMinuteOneHost(q, scaleVar)
	return q
}

