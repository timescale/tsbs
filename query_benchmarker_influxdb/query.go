package main

import "fmt"

// Query holds HTTP request data, typically decoded from the program's input.
type Query struct {
	HumanLabel       []byte
	HumanDescription []byte
	Method           []byte
	Path             []byte
	Body             []byte
	ID               int64
}

// String produces a debug-ready description of a Query.
func (q *Query) String() string {
	return fmt.Sprintf("ID: %d, HumanLabel: %s, HumanDescription: %s, Method: %s, Path: %s, Body:%s", q.ID, q.HumanLabel, q.HumanDescription, q.Method, q.Path, q.Body)
}
