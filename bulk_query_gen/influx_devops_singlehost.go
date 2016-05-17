package main

import "time"

// InfluxDevops produces Influx-specific queries for the devops single-host case.
type InfluxDevopsSingleHost struct {
	InfluxDevops
}

func NewInfluxDevopsSingleHost(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newInfluxDevopsCommon(dbConfig, start, end).(*InfluxDevops)
	return &InfluxDevopsSingleHost{
		InfluxDevops: *underlying,
	}

}

func (d *InfluxDevopsSingleHost) Dispatch(i int, q *Query, scaleVar int) {
	d.MaxCPUUsageHourByMinuteOneHost(q, scaleVar)
}
