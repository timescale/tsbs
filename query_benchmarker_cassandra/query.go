package main

import (
	"fmt"
	"time"
)

// HLQuery is a high-level query, deserialized from the bulk query generator.
//
// The primary use of an HLQuery is to combine it with a ClientSideIndex to
// construct a QueryPlan.
type HLQuery struct {
	HumanLabel       []byte
	HumanDescription []byte
	ID               int64

	MeasurementName []byte // e.g. "cpu"
	FieldName       []byte // e.g. "usage_user"
	AggregationType []byte // e.g. "avg" or "sum". used literally in the cassandra query.
	TimeStart       time.Time
	TimeEnd         time.Time
	GroupByDuration time.Duration
	TagFilters      []TagFilter // semantically, these are AND'ed.
}

type TagFilter string

// String produces a debug-ready description of a Query.
func (q *HLQuery) String() string {
	return fmt.Sprintf("ID: %d, HumanLabel: %s, HumanDescription: %s, MeasurementName: %s, FieldName: %s, AggregationType: %s, TimeStart: %s, TimeEnd: %s, GroupByDuration: %s, TagFilters: %s", q.ID, q.HumanLabel, q.HumanDescription, q.MeasurementName, q.FieldName, q.AggregationType, q.TimeStart, q.TimeEnd, q.GroupByDuration, q.TagFilters)
}

// ToQueryPlan combines an HLQuery with a ClientSideIndex to make a QueryPlan.
func (q *HLQuery) ToQueryPlan(csi *ClientSideIndex) (qp *QueryPlan, err error) {
	seriesChoices := csi.CopyOfSeriesCollection()

	// Build the time buckets used for 'group by time'-type queries.
	//
	// It is important to populate these even if they end up being empty,
	// so that we get correct results for different 'time buckets'.
	tis := bucketTimeIntervals(q.TimeStart, q.TimeEnd, q.GroupByDuration)
	bucketedSeries := map[TimeInterval][]Series{}
	for _, ti := range tis {
		bucketedSeries[ti] = []Series{}
	}

	// For each known db series, associate it to its applicable time
	// buckets, if any:
	for _, s := range seriesChoices {
		// quick skip if the series doesn't match at all:
		if !s.MatchesMeasurementName(string(q.MeasurementName)) {
			continue
		}
		if !s.MatchesFieldName(string(q.FieldName)) {
			continue
		}
		if !s.MatchesTagFilters(q.TagFilters) {
			continue
		}

		// check each group-by interval to see if it applies:
		for _, ti := range tis {
			if !s.MatchesTimeInterval(&ti) {
				continue
			}
			bucketedSeries[ti] = append(bucketedSeries[ti], s)
		}
	}

	// For each group-by time bucket, convert its series into CQLQueries:
	cqlBuckets := make(map[TimeInterval][]CQLQuery, len(bucketedSeries))
	for k, seriesSlice := range bucketedSeries {
		cqlQueries := make([]CQLQuery, len(seriesSlice))
		for i, ser := range seriesSlice {
			cqlQueries[i] = NewCQLQuery(string(q.AggregationType), ser.Table, ser.Id, ser.TimeInterval.Start.UnixNano(), ser.TimeInterval.End.UnixNano())
		}
		cqlBuckets[k] = cqlQueries
	}

	qp, err = NewQueryPlan(string(q.AggregationType), cqlBuckets)
	return
}

func (q *HLQuery) toSubQueryGroups(csi *ClientSideIndex) []subQuery {
	return nil
	// TODO: time filtering
//	applicableSeries := csi.SeriesSelector(q)
//	//fmt.Printf("got %d applicableSeries for %v\n", len(applicableSeries), q)
//	if len(applicableSeries) == 0 {
//		log.Fatal("logic error: no applicable series")
//	}
//	ret := make([]subQuery, len(applicableSeries))
//
//	for i := 0; i < len(ret); i++ {
//		//s := applicableSeries[i]
//		ret[i] = subQuery{
//			//table:          s.table,
//			//rowId:          s.id,
//			//aggregation:    string(q.AggregationType),
//			//timeStartNanos: q.TimeStart.UnixNano(),
//			//timeEndNanos:   q.TimeEnd.UnixNano(),
//		}
//	}
//
//	return ret
}

type subQuery struct {
	table, rowId   string
	aggregation    string
	timeStartNanos int64
	timeEndNanos   int64
}

func (sq *subQuery) ToCQL() string {
	return fmt.Sprintf("SELECT %s(value) FROM %s WHERE series_id = '%s' AND timestamp_ns >= %d AND timestamp_ns < %d",
		sq.aggregation, sq.table, sq.rowId, sq.timeStartNanos, sq.timeEndNanos)
}

type subQueryGroup []subQuery

type CQLQuery string

func NewCQLQuery(aggrLabel, tableName, rowName string, timeStartNanos, timeEndNanos int64) CQLQuery {
	fmtStr := "SELECT %s(value) FROM %s WHERE series_id = '%s' AND timestamp_ns >= %d AND timestamp_ns < %d"
	return CQLQuery(fmt.Sprintf(fmtStr, aggrLabel, tableName, rowName, timeStartNanos, timeEndNanos))
}
