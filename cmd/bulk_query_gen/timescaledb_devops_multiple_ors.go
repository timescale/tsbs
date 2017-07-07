package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

type TimescaleDBDevopsMultipleOrs struct {
	TimescaleDBDevops
}

func NewTimescaleDBDevopsMultipleOrs(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newTimescaleDBDevopsCommon(dbConfig, start, end).(*TimescaleDBDevops)
	return &TimescaleDBDevopsMultipleOrs{
		TimescaleDBDevops: *underlying,
	}
}

func (d *TimescaleDBDevopsMultipleOrs) Dispatch(i, scaleVar int) query.Query {
	q := query.NewTimescaleDB() // from pool
	d.MultipleMemOrs(q, scaleVar)
	return q
}
