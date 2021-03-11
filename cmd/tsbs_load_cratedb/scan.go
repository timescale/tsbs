package main

import (
	"bufio"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
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
	rowCnt  uint
}

// scan.Batch interface implementation
func (eb *eventsBatch) Len() uint {
	return eb.rowCnt
}

// scan.Batch interface implementation
func (eb *eventsBatch) Append(item data.LoadedPoint) {
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
func (f *factory) New() targets.Batch {
	return ePool.Get().(*eventsBatch)
}

// source.DataSource interface implementation
type fileDataSource struct {
	scanner *bufio.Scanner
	headers *common.GeneratedDataHeaders
}

// source.DataSource interface implementation
//
// Decodes a data point of a following format:
//       <measurement_type>\t<tags>\t<timestamp>\t<metric1>\t...\t<metricN>
//
// Converts metric values to double-precision floating-point number, timestamp
// to time.Time and tags to bytes array.
func (d *fileDataSource) NextItem() data.LoadedPoint {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil {
		// nothing scanned & no error = EOF
		return data.LoadedPoint{}
	} else if !ok {
		fatal("scan error: %v", d.scanner.Err())
		return data.LoadedPoint{}
	}

	// split a point record into a measurement type, timestamp, tags,
	// and field values
	parts := strings.SplitN(d.scanner.Text(), "\t", 4)
	if len(parts) != 4 {
		fatal("incorrect point format, some fields are missing")
		return data.LoadedPoint{}
	}
	table := parts[0]
	tags := []byte(parts[1])

	metrics, err := parseMetrics(strings.Split(parts[3], "\t"))
	if err != nil {
		fatal("cannot parse metrics: %v", err)
		return data.LoadedPoint{}
	}

	ts, err := parseTime(parts[2])
	if err != nil {
		fatal("cannot parse timestamp: %v", err)
		return data.LoadedPoint{}
	}

	row := append(row{tags, ts}, metrics...)
	return data.NewLoadedPoint(&point{table: table, row: row})
}

// cratedb file format doesn't have headers
func (d *fileDataSource) Headers() *common.GeneratedDataHeaders {
	if d.headers != nil {
		return d.headers
	}

	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil {
		fatal("not enough lines, no tags scanned")
		// nothing scanned & no error = EOF
		return nil
	} else if !ok {
		fatal("scan error: %v", d.scanner.Err())
		return nil
	}
	line := d.scanner.Text()
	line = strings.TrimSpace(line)
	tagsLine := strings.Split(line, ",")
	if tagsLine[0] != "tags" {
		fatal("first header line doesn't contain tags")
		return nil
	}
	tagsAndTypes := tagsLine[1:]
	tags := make([]string, len(tagsAndTypes))
	tagTypes := make([]string, len(tagsAndTypes))
	for i, tt := range tagsAndTypes {
		tagAndTypeSplit := strings.Split(tt, " ")
		if len(tagAndTypeSplit) != 2 {
			fatal("first header line should be of format 'tags, tagName1 tagType1, ..., tagNameN tagTypeN")
			return nil
		}
		tags[i] = tagAndTypeSplit[0]
		tagTypes[i] = tagAndTypeSplit[1]
	}
	fields := make(map[string][]string)
	for {
		ok := d.scanner.Scan()
		if !ok && d.scanner.Err() == nil {
			fatal("not enough lines, no cols scanned")
			// nothing scanned & no error = EOF
			return nil
		} else if !ok {
			fatal("scan error: %v", d.scanner.Err())
			return nil
		}
		line := d.scanner.Text()
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			break
		}

		parts := strings.SplitN(line, ",", 2)
		if len(parts) < 2 {
			fatal("metric columns are missing")
			return nil
		}
		fields[parts[0]] = strings.Split(parts[1], ",")
	}
	d.headers = &common.GeneratedDataHeaders{
		TagTypes:  tagTypes,
		TagKeys:   tags,
		FieldKeys: fields,
	}
	return d.headers
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
