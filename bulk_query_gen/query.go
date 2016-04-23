package main

import "fmt"

// Query encodes an HTTP request. This will typically by serialized for use by
// the query_benchmarker program.
type Query struct {
	HumanLabel       []byte
	HumanDescription []byte
	Method           []byte
	Path             []byte
	Body             []byte
}

// String produces a debug-ready description of a Query.
func (q *Query) String() string {
	return fmt.Sprintf("HumanLabel: \"%s\", HumanDescription: \"%s\", Method: \"%s\", Path: \"%s\", Body: \"%s\"", q.HumanLabel, q.HumanDescription, q.Method, q.Path, q.Body)
}
