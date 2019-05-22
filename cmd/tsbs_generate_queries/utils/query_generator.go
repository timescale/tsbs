package utils

import "github.com/timescale/tsbs/query"

// QueryGenerator is an interface that a database-specific implementation of a
// use case implements to set basic configuration that can then be used by
// a specific QueryFiller, ultimately yielding a query.Query with information
// to be run.
type QueryGenerator interface {
	GenerateEmptyQuery() query.Query
}

// QueryFiller describes a type that can fill in a query and return it
type QueryFiller interface {
	// Fill fills in the query.Query with query details
	Fill(query.Query) query.Query
}

// QueryFillerMaker is a function that takes a QueryGenerator and returns a QueryFiller
type QueryFillerMaker func(QueryGenerator) QueryFiller
