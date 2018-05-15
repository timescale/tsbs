package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// QueryGenerator describes a generator of queries, typically according to a
// use case.
type QueryGenerator interface {
	Dispatch() query.Query
}

// QueryGeneratorMaker is a function that takes a time range and returns a QueryGenerator
// to build a Query from for the given time parameters
type QueryGeneratorMaker func(time.Time, time.Time, int) QueryGenerator
