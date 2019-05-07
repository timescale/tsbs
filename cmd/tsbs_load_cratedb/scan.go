package main

import (
	"bufio"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/timescale/tsbs/load"
)

type row = []interface{}

// Point is a single row of data keyed by which table it belongs
type point struct {
	table string
	row   row
}

// scan.Batch interface implementation
type eventsBatch struct {
	batches map[string][]*row
	rowCnt  int
}

// scan.Batch interface implementation
func (eb *eventsBatch) Len() int {
	return eb.rowCnt
}

// scan.Batch interface implementation
func (eb *eventsBatch) Append(item *load.Point) {
	p := item.Data.(*point)
	table := p.table
	eb.batches[table] = append(eb.batches[table], &p.row)
	eb.rowCnt++
}

var ePool = &sync.Pool{New: func() interface{} {
	return &eventsBatch{batches: map[string][]*row{}}
}}

// scan.BatchFactory interface implementation
type factory struct{}

// scan.BatchFactory interface implementation
func (f *factory) New() load.Batch {
	return ePool.Get().(*eventsBatch)
}

// scan.PointDecoder interface implementation
type decoder struct {
	scanner *bufio.Scanner
}

// scan.PointDecoder interface implementation
//
// Decodes a data point of a following format:
//       <measurement_type>\t<tags>\t<timestamp>\t<metric1>\t...\t<metricN>
//
// Converts metric values to double-precision floating-point number, timestamp
// to time.Time and tags to bytes array.
func (d *decoder) Decode(_ *bufio.Reader) *load.Point {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil {
		// nothing scanned & no error = EOF
		return nil
	} else if !ok {
		fatal("scan error: %v", d.scanner.Err())
		return nil
	}

	// split a point record into a measurement type, timestamp, tags,
	// and field values
	parts := strings.SplitN(d.scanner.Text(), "\t", 4)
	if len(parts) != 4 {
		fatal("incorrect point format, some fields are missing")
		return nil
	}
	table := parts[0]
	tags := []byte(parts[1])

	metrics, err := parseMetrics(strings.Split(parts[3], "\t"))
	if err != nil {
		fatal("cannot parse metrics: %v", err)
		return nil
	}

	ts, err := parseTime(parts[2])
	if err != nil {
		fatal("cannot parse timestamp: %v", err)
		return nil
	}

	row := append(row{tags, ts}, metrics...)
	return load.NewPoint(&point{table: table, row: row})
}

func parseTime(v string) (time.Time, error) {
	ts, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, ts), nil
}

func parseMetrics(values []string) (row, error) {
	metrics := make(row, len(values))
	for i := range values {
		metric, err := strconv.ParseFloat(values[i], 64)
		if err != nil {
			return nil, err
		}
		metrics[i] = metric
	}
	return metrics, nil
}
