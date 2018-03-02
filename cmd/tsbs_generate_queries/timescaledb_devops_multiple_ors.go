package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

type TimescaleDBDevopsMultipleOrs struct {
	TimescaleDBDevops
}

func NewTimescaleDBDevopsMultipleOrs(start, end time.Time) QueryGenerator {
	underlying := newTimescaleDBDevopsCommon(start, end)
	return &TimescaleDBDevopsMultipleOrs{
		TimescaleDBDevops: *underlying,
	}
}

func (d *TimescaleDBDevopsMultipleOrs) Dispatch(scaleVar int) query.Query {
	q := query.NewTimescaleDB() // from pool
	d.MultipleMemOrs(q, scaleVar)
	return q
}
