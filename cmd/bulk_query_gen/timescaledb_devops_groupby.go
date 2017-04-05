package main

import "time"

// TimescaleDBDevopsGroupby produces TimescaleDB-specific queries for the devops groupby case.
type TimescaleDBDevopsGroupby struct {
	TimescaleDBDevops
}

func NewTimescaleDBDevopsGroupBy(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newTimescaleDBDevopsCommon(dbConfig, start, end).(*TimescaleDBDevops)
	return &TimescaleDBDevopsGroupby{
		TimescaleDBDevops: *underlying,
	}

}

func (d *TimescaleDBDevopsGroupby) Dispatch(i, scaleVar int) Query {
	q := NewTimescaleDBQuery() // from pool
	d.MeanCPUUsageDayByHourAllHostsGroupbyHost(q, scaleVar)
	return q
}
