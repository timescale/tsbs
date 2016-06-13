package main

import (
	"fmt"
	"log"
	"time"
)

// Query holds HTTP request data, typically decoded from the program's input.
type Query struct {
	HumanLabel       []byte
	HumanDescription []byte
	ID               int64

	MeasurementName string // e.g. "cpu"
	FieldName       string // e.g. "usage_user"
	AggregationType string // e.g. "avg" or "sum". used literally in the cassandra query.
	TimeStart       time.Time
	TimeEnd         time.Time
	TagFilters      []TagFilter // semantically, these are AND'ed.
}

type TagFilter string

// String produces a debug-ready description of a Query.
func (q *Query) String() string {
	return fmt.Sprintf("ID: %d, HumanLabel: %s, HumanDescription: %s, AggregationType: %s, TimeStart: %s, TimeEnd: %s, TagFilters: %s", q.ID, q.HumanLabel, q.HumanDescription, q.AggregationType, q.TimeStart, q.TimeEnd, q.TagFilters)
}

func (q *Query) CassandraFormat() string {
	return fmt.Sprintf("SELECT %s(value)", q.AggregationType)
}


func (q *Query) toSubQueries(csi *ClientSideIndex) []subQuery {
	// TODO: time filtering
	applicableSeries := csi.SeriesSelector(q)
	if len(applicableSeries) == 0 {
		log.Fatal("logic error: no applicable series")
	}
	ret := make([]subQuery, len(applicableSeries))

	for i := 0 ; i < len(ret); i++ {
		s := applicableSeries[i]
		ret[i] = subQuery{
			table: s.table,
			rowId: s.id,
			aggregation: string(q.AggregationType),
			timeStartNanos: q.TimeStart.UnixNano(),
			timeEndNanos: q.TimeEnd.UnixNano(),
		}
	}

	return ret
}

type subQuery struct {
	table, rowId string
	aggregation string
	timeStartNanos int64
	timeEndNanos int64
}
