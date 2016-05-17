package main

import "time"

// QueryGenerator describes a generator of queries, typically according to a
// use case.
type QueryGenerator interface {
	Dispatch(int, *Query, int)
}

type QueryGeneratorMaker func(DatabaseConfig, time.Time, time.Time) QueryGenerator
