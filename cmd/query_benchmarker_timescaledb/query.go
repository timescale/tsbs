package main

import "fmt"

type Query struct {
	HumanLabel       []byte
	HumanDescription []byte

	NamespaceName []byte // e.g. "cpu"
	SqlQuery      []byte
	ID            int64
}

// String produces a debug-ready description of a Query.
func (q *Query) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, NamespaceName: %s, Query: %s", q.HumanLabel, q.HumanDescription, q.NamespaceName, q.SqlQuery)
}
