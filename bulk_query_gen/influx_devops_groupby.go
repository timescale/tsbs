package main

import "time"

// InfluxDevopsGroupby produces Influx-specific queries for the devops groupby case.
type InfluxDevopsGroupby struct {
	InfluxDevops
}

func NewInfluxDevopsGroupby(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newInfluxDevopsCommon(dbConfig, start, end).(*InfluxDevops)
	return &InfluxDevopsGroupby{
		InfluxDevops: *underlying,
	}

}

func (d *InfluxDevopsGroupby) Dispatch(i int, q *Query, scaleVar int) {
	d.MeanCPUUsageDayByHourAllHosts(q)
}
