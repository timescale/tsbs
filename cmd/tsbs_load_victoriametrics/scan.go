package main

import (
	"bufio"
	"bytes"
	"github.com/timescale/tsbs/load"
	"log"
)

const errNotThreeTuplesFmt = "parse error: line does not have 3 tuples, has %d"

var newLine = []byte("\n")

type decoder struct {
	scanner *bufio.Scanner
}

func (d *decoder) Decode(_ *bufio.Reader) *load.Point {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return nil
	} else if !ok {
		log.Fatalf("scan error: %v", d.scanner.Err())
		return nil
	}
	return load.NewPoint(d.scanner.Bytes())
}

type batch struct {
	buf     *bytes.Buffer
	rows    uint64
	metrics uint64
}

func (b *batch) Len() int {
	return int(b.rows)
}

var (
	spaceSep = []byte(" ")
	commaSep = []byte(",")
)

func (b *batch) Append(item *load.Point) {
	that := item.Data.([]byte)
	b.rows++

	// Each influx line is format "csv-tags csv-fields timestamp"
	if args := bytes.Count(that, spaceSep); args != 2 {
		log.Fatalf(errNotThreeTuplesFmt, args+1)
		return
	}

	// seek for fields position in slice
	fieldsPos := bytes.Index(that, spaceSep)
	// seek for timestamps position in slice
	timestampPos := bytes.Index(that[fieldsPos+1:], spaceSep) + fieldsPos
	fields := that[fieldsPos+1 : timestampPos]
	b.metrics += uint64(bytes.Count(fields, commaSep) + 1)

	b.buf.Write(that)
	b.buf.Write(newLine)
}

type factory struct{}

func (f *factory) New() load.Batch {
	return &batch{buf: bufPool.Get().(*bytes.Buffer)}
}
