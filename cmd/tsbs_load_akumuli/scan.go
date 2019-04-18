package main

import (
	"bufio"
	"bytes"

	"github.com/timescale/tsbs/load"
)

type decoder struct {
	scanner *bufio.Scanner
}

func (d *decoder) Decode(_ *bufio.Reader) *load.Point {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return nil
	} else if !ok {
		fatal("scan error: %v", d.scanner.Err())
		return nil
	}
	d.scanner.Bytes()
	return load.NewPoint(d.scanner.Bytes())
}

type batch struct {
	buf *bytes.Buffer
}

func (b *batch) Len() int {
	return int(b.buf.Len())
}

func (b *batch) Append(item *load.Point) {
}

type factory struct{}

func (f *factory) New() load.Batch {
	return &batch{buf: bufPool.Get().(*bytes.Buffer)}
}
