package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"

	"github.com/timescale/tsbs/load"
)

type decoder struct {
	reader *bufio.Reader
}

func (d *decoder) Decode(_ *bufio.Reader) *load.Point {
	hdr, err := d.reader.Peek(6)
	if err == io.EOF {
		return nil
	}
	nbytes := binary.LittleEndian.Uint16(hdr[4:6])
	body := make([]byte, nbytes)
	_, err = io.ReadFull(d.reader, body)
	if err == io.EOF {
		return nil
	}
	return load.NewPoint(body)
}

type batch struct {
	buf  *bytes.Buffer
	rows uint64
}

func (b *batch) Len() int {
	return int(b.rows)
}

func (b *batch) Append(item *load.Point) {
	payload := item.Data.([]byte)
	b.buf.Write(payload)
}

type factory struct{}

func (f *factory) New() load.Batch {
	return &batch{buf: bufPool.Get().(*bytes.Buffer)}
}
