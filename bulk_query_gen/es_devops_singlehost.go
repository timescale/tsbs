package main

import "time"

// ElasticSearchDevopsSingleHost produces ES-specific queries for the devops single-host case.
type ElasticSearchDevopsSingleHost struct {
	ElasticSearchDevops
}

func NewElasticSearchDevopsSingleHost(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := NewElasticSearchDevops(dbConfig, start, end).(*ElasticSearchDevops)
	return &ElasticSearchDevopsSingleHost{
		ElasticSearchDevops: *underlying,
	}
}

func (d *ElasticSearchDevopsSingleHost) Dispatch(i int, q *Query, scaleVar int) {
	d.MaxCPUUsageHourByMinuteOneHost(q, scaleVar)
}
