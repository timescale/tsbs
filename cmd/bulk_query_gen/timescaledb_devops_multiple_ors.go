package main

import "time"

type TimescaleDBDevopsMultipleOrs struct {
	TimescaleDBDevops
}

func NewTimescaleDBDevopsMultipleOrs(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := newTimescaleDBDevopsCommon(dbConfig, start, end).(*TimescaleDBDevops)
	return &TimescaleDBDevopsMultipleOrs{
		TimescaleDBDevops: *underlying,
	}
}

func (d *TimescaleDBDevopsMultipleOrs) Dispatch(i, scaleVar int) Query {
	q := NewTimescaleDBQuery() // from pool
	d.MultipleMemOrs(q, scaleVar)
	return q
}
