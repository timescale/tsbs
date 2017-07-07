package main

import (
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// QueryGenerator describes a generator of queries, typically according to a
// use case.
type QueryGenerator interface {
	Dispatch(int, int) query.Query
}

type QueryGeneratorMaker func(DatabaseConfig, time.Time, time.Time) QueryGenerator
