package main

import "time"

// ElasticSearchDevopsGroupBy produces ES-specific queries for the devops groupby case.
type ElasticSearchDevopsGroupBy struct {
	ElasticSearchDevops
}

func NewElasticSearchDevopsGroupBy(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := NewElasticSearchDevops(dbConfig, start, end).(*ElasticSearchDevops)
	return &ElasticSearchDevopsGroupBy{
		ElasticSearchDevops: *underlying,
	}
}

func (d *ElasticSearchDevopsGroupBy) Dispatch(i int, q *Query, scaleVar int) {
	d.MeanCPUUsageDayByHourAllHosts(q)
}
