package main

import "fmt"

type Query struct {
	HumanLabel       []byte
	HumanDescription []byte

	NamespaceName []byte // e.g. "cpu"
	FieldName     []byte // e.g. "usage_user"
	SqlQuery      []byte
}

// String produces a debug-ready description of a Query.
func (q *Query) String() string {
	return fmt.Sprintf("HumanLabel: %s, HumanDescription: %s, NamespaceName: %s, FieldName: %s, Query: %s", q.HumanLabel, q.HumanDescription, q.NamespaceName, q.FieldName, q.SqlQuery)
}
