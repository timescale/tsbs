package main

import (
	"bufio"
	"bytes"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
)

const errNotThreeTuplesFmt = "parse error: line does not have 3 tuples, has %d"

var newLine = []byte("\n")

type fileDataSource struct {
	scanner *bufio.Scanner
}

func (d *fileDataSource) NextItem() data.LoadedPoint {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return data.LoadedPoint{}
	} else if !ok {
		fatal("scan error: %v", d.scanner.Err())
		return data.LoadedPoint{}
	}
	return data.NewLoadedPoint(d.scanner.Bytes())
}

func (d *fileDataSource) Headers() *common.GeneratedDataHeaders { return nil }

type batch struct {
	buf           *bytes.Buffer
	rows          uint
	metrics       uint64
	metricsPerRow uint64
}

func (b *batch) Len() uint {
	return b.rows
}

func (b *batch) Append(item data.LoadedPoint) {
	that := item.Data.([]byte)
	b.rows++

	// We only validate the very first row per batch since it's an expensive operation.
	// As a part of the validation we also calculate the number of metrics per row.
	if b.metricsPerRow == 0 {
		// Each influx line is format "csv-tags csv-fields timestamp", so we split by space.
		var tuples, metrics uint64 = 1, 1
		for i := 0; i < len(that); i++ {
			if that[i] == byte(' ') {
				tuples++
			}
			// On the middle element, we split by comma to count number of fields added.
			if tuples == 2 && that[i] == byte(',') {
				metrics++
			}
		}
		if tuples != 3 {
			fatal(errNotThreeTuplesFmt, tuples)
			return
		}
		b.metricsPerRow = metrics
	}
	b.metrics += b.metricsPerRow

	b.buf.Write(that)
	b.buf.Write(newLine)
}

type factory struct{}

func (f *factory) New() targets.Batch {
	return &batch{buf: bufPool.Get().(*bytes.Buffer)}
}
