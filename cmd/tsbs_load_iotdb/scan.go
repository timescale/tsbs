package main

import (
	"bufio"
	"bytes"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
)

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
	buf     *bytes.Buffer
	rows    uint
	metrics uint64
}

func (b *batch) Len() uint {
	return b.rows
}

func (b *batch) Append(item data.LoadedPoint) {
	// CRTODO 可能需要解析内容
	// CRINTERFACE
	b.buf.Write([]byte("\n"))
}

type factory struct{}

func (f *factory) New() targets.Batch {
	return &batch{buf: bufPool.Get().(*bytes.Buffer)}
}
