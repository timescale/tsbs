package main

import "fmt"

type QueryBytes struct {
	HumanLabel       []byte
	HumanDescription []byte
	Method           []byte
	Path             []byte
	Body             []byte
}

// String produces a debug-ready description of a Query.
func (q *QueryBytes) String() string {
	return fmt.Sprintf("HumanLabel: \"%s\", HumanDescription: \"%s\", Method: \"%s\", Path: \"%s\", Body: \"%s\"", q.HumanLabel, q.HumanDescription, q.Method, q.Path, q.Body)
}
