package main

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
)

type HLQueryExecutor struct {
	session             *gocql.Session
	csi                 *ClientSideIndex
	debug               int
}

func NewHLQueryExecutor(session *gocql.Session, csi *ClientSideIndex, debug int) *HLQueryExecutor {
	return &HLQueryExecutor{
		session: session,
		csi:     csi,
		debug:   debug,
	}
}

type HLQueryExecutorDoOptions struct {
	SubQueryParallelism  int
	Debug                int
	PrettyPrintResponses bool
}

func (qe *HLQueryExecutor) Do(q *HLQuery, opts HLQueryExecutorDoOptions) (lagMs float64, err error) {
	var qp *QueryPlan
	qpStart := time.Now()
	qp, err = q.ToQueryPlan(qe.csi)
	qpLag := time.Now().Sub(qpStart).Seconds()
	fmt.Printf("[hlqe] query planning took %fs\n", qpLag)
	if err != nil {
		return
	}
	start := time.Now()
	err = qp.Execute(qe.session)
	lag := time.Now().Sub(start).Nanoseconds()
	lagMs = float64(lag) / 1e6
	return
	if opts.SubQueryParallelism <= 0 {
		panic("logic error: subQueryParallelism must be > 0")
	}

	//fmt.Println("executed a query")
	//fmt.Println(q)

	//subQs := q.toSubQueries(qe.csi)
	//subResults :=  make([]float64, len(subQs))
	//err = qe.scatter(subQs, subResults)
	//if err != nil {
	//	return
	//}
	//result := qe.gather(subResults)
	//scatterResults := []string
	return
}

func (qe *HLQueryExecutor) scatter(subQueries []subQuery, subResults []float64) error {
	if len(subQueries) != len(subResults) {
		panic("logic error: bad `scatter` arguments")
	}
	for i, subQ := range subQueries {
		raw := subQ.ToCQL()
		iter := qe.session.Query(raw).Iter()
		var x float64
		iter.Scan(&x)
		err := iter.Close()
		if err != nil  {
			return err
		}
		subResults[i] = x
		//fmt.Println(subQ)
	}
	fmt.Println(subResults)
	return nil
}
