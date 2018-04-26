package main

import (
	"bufio"
	"bytes"
	"log"
	"strings"

	"bitbucket.org/440-labs/influxdb-comparisons/load"
)

var newLine = []byte("\n")

type decoder struct {
	scanner *bufio.Scanner
}

func (d *decoder) Decode(_ *bufio.Reader) interface{} {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return nil
	} else if !ok {
		log.Fatalf("scan error: %v", d.scanner.Err())
	}
	return d.scanner.Bytes()
}

type batch struct {
	buf     *bytes.Buffer
	rows    uint64
	metrics uint64
}

func (b *batch) Len() int {
	return int(b.rows)
}

func (b *batch) Append(item interface{}) {
	that := item.([]byte)
	thatStr := string(that)
	b.rows++
	// Each influx line is format "csv-tags csv-fields timestamp", so we split by space
	// and then on the middle element, we split by comma to count number of fields added
	b.metrics += uint64(len(strings.Split(strings.Split(thatStr, " ")[1], ",")))

	b.buf.Write(that)
	b.buf.Write(newLine)
}

type factory struct{}

func (f *factory) New() load.Batch {
	return &batch{buf: bufPool.Get().(*bytes.Buffer)}
}
