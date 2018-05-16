package main

import (
	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// QueryFiller describes a type that can fill in a query and return it
type QueryFiller interface {
	// Fill fills in the query.Query with query details
	Fill(query.Query) query.Query
}

// QueryFillerMaker is a function that takes a DevopsGenerator and returns a QueryFiller
type QueryFillerMaker func(DevopsGenerator) QueryFiller
