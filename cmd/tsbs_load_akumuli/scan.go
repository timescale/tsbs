package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"github.com/timescale/tsbs/pkg/targets"
	"io"
)

type decoder struct {
	reader *bufio.Reader
}

func (d *decoder) Decode(_ *bufio.Reader) *targets.Point {
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
	return targets.NewPoint(body)
}

type pointIndexer struct {
	nchan uint
}

func (i *pointIndexer) GetIndex(p *targets.Point) int {
	hdr := p.Data.([]byte)
	id := binary.LittleEndian.Uint32(hdr[0:4])
	return int(id % uint32(i.nchan))
}

type batch struct {
	buf  *bytes.Buffer
	rows uint64
}

func (b *batch) Len() int {
	return int(b.rows)
}

func (b *batch) Append(item *targets.Point) {
	payload := item.Data.([]byte)
	b.buf.Write(payload)
	b.rows++
}

type factory struct{}

func (f *factory) New() targets.Batch {
	return &batch{buf: bufPool.Get().(*bytes.Buffer)}
}
