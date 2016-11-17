package main

import "time"

// QueryGenerator describes a generator of queries, typically according to a
// use case.
type QueryGenerator interface {
	Dispatch(int, int) Query
}

type QueryGeneratorMaker func(DatabaseConfig, time.Time, time.Time) QueryGenerator
