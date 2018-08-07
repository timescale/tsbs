package main

import (
	"bufio"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/timescale/tsbs/load"
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
	insertStatement := "INSERT INTO %s(series_id, timestamp_ns, value) VALUES('%s#%s#%s', %s, %s)"
	parts := strings.Split(text, ",")
	tagsBeginIndex := 1                  // list of tags begins after the table name
	tagsEndIndex := (len(parts) - 1) - 4 // list of tags ends right before the last 4 parts of the line

	table := parts[0]
	tags := strings.Join(parts[tagsBeginIndex:tagsEndIndex+1], ",") // offset: table
	measurementName := parts[tagsEndIndex+1]                        // offset: table + numTags
	dayBucket := parts[tagsEndIndex+2]                              // offset: table + numTags + measurementName
	timestampNS := parts[tagsEndIndex+3]                            // offset: table + numTags + numTags + measurementName + dayBucket
	value := parts[tagsEndIndex+4]                                  // offset: table + numTags + timestamp + measurementName + dayBucket + timestampNS

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
