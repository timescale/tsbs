package main

import (
	"fmt"

	"github.com/gocql/gocql"
)

type QueryExecutor struct {
	session *gocql.Session
	csi     *ClientSideIndex
	debug   int
}

func NewQueryExecutor(session *gocql.Session, csi *ClientSideIndex, debug int) *QueryExecutor {
	return &QueryExecutor{
		session: session,
		csi:     csi,
		debug:   debug,
	}
}

type QueryExecutorDoOptions struct {
	Debug                int
	PrettyPrintResponses bool
}

func (qe *QueryExecutor) Do(q *Query, opts QueryExecutorDoOptions) (lagMs float64, err error) {
	fmt.Println("executed a query")
	fmt.Println(q)

	subQs := q.toSubQueries(qe.csi)
	for _, subQ := range subQs {
		fmt.Println(subQ)
	}
	//lag := time.Now().Sub(start).Nanoseconds()
	return
}

