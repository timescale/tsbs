package main

import "fmt"

// Query holds Mongo BSON request data, typically decoded from the program's
// input.
type Query struct {
	HumanLabel       []byte
	HumanDescription []byte
	DatabaseName     []byte
	CollectionName   []byte
	BsonDoc          []M
	ID               int64
}

// String produces a debug-ready description of a Query.
func (q *Query) String() string {
	return fmt.Sprintf("ID: %d, HumanLabel: %s, HumanDescription: %s, Database: %s, Collection: %s", q.ID, q.HumanLabel, q.HumanDescription, q.DatabaseName, q.CollectionName)
}
