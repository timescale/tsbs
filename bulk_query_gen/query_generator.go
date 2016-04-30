package main

// QueryGenerator describes a generator of queries, typically according to a
// use case.
type QueryGenerator interface {
	Dispatch(int, *Query, int)
}
