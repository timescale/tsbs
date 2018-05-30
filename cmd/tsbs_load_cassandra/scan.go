package main

import (
	"bufio"
	"fmt"
	"log"
	"strings"
	"sync"

	"bitbucket.org/440-labs/tsbs/load"
)

type decoder struct {
	scanner *bufio.Scanner
}

// Reads and returns a CSV line that encodes a data point.
// Since scanning happens in a single thread, we hold off on transforming it
// to an INSERT statement until it's being processed concurrently by a worker.
func (d *decoder) Decode(_ *bufio.Reader) *load.Point {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return nil
	} else if !ok {
		log.Fatalf("scan error: %v", d.scanner.Err())
	}

	return load.NewPoint(d.scanner.Text())
}

// Transforms a CSV string encoding a single metric into a CQL INSERT statement.
// We currently only support a 1-line:1-metric mapping for Cassandra. Implement
// other functions here to support other formats.
func singleMetricToInsertStatement(text string) string {
	const numTags = 11 // TODO: make number of tags dynamic
	var insertStatement = "INSERT INTO %s(series_id, timestamp_ns, value) VALUES('%s#%s#%s', %s, %s)"

	parts := strings.Split(text, ",")

	// Each line must consist of a table name, all comma separated tags, the measurement type, a day bucket, a timestamp, and a value
	if len(parts) != numTags+5 {
		log.Fatalf("Format error: Invalid number of values on CSV line")
	}

	table := parts[0]
	tags := strings.Join(parts[1:numTags+1], ",") // offset: table
	measurementName := parts[numTags+1]           // offset: table + numTags
	dayBucket := parts[numTags+2]                 // offset: table + numTags + measurementName
	timestampNS := parts[numTags+3]               // offset: table + numTags + numTags + measurementName + dayBucket
	value := parts[numTags+4]                     // offset: table + numTags + timestamp + measurementName + dayBucket + timestampNS

	return fmt.Sprintf(insertStatement, table, tags, measurementName, dayBucket, timestampNS, value)
}

type eventsBatch struct {
	rows []string
}

func (eb *eventsBatch) Len() int {
	return len(eb.rows)
}

func (eb *eventsBatch) Append(item *load.Point) {
	that := item.Data.(string)
	eb.rows = append(eb.rows, that)
}

var ePool = &sync.Pool{New: func() interface{} { return &eventsBatch{rows: []string{}} }}

type factory struct{}

func (f *factory) New() load.Batch {
	return ePool.Get().(*eventsBatch)
}
