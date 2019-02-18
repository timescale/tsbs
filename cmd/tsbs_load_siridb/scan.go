package main

import (
	"bufio"
	"encoding/binary"
	"io"
	"log"

	"github.com/timescale/tsbs/load"
)

type point struct {
	data    map[string][]byte
	dataCnt uint64
}

type batch struct {
	series    map[string][]byte
	batchCnt  int
	metricCnt uint64
}

func (b *batch) Len() int {
	return b.batchCnt
}

func (b *batch) Append(item *load.Point) {
	that := item.Data.(*point)
	for k, v := range that.data {
		if len(b.series[k]) == 0 {
			b.series[k] = append(b.series[k], byte(252)) // qpack: open array
		}
		b.series[k] = append(b.series[k], v...)
	}
	b.metricCnt += that.dataCnt
	b.batchCnt++
}

type factory struct{}

func (f *factory) New() load.Batch {
	return &batch{
		series:    map[string][]byte{},
		batchCnt:  0,
		metricCnt: 0,
	}
}

type decoder struct {
	buf []byte
	len uint32
}

func (d *decoder) Read(bf *bufio.Reader) int {
	buf := make([]byte, 8192)
	n, err := bf.Read(buf)
	if err == io.EOF {
		return n
	}
	if err != nil {
		log.Fatal(err.Error())
	}

	d.len += uint32(n)
	d.buf = append(d.buf, buf[:n]...)
	return n
}

func (d *decoder) Decode(bf *bufio.Reader) *load.Point {
	if d.len < 8 {
		if n := d.Read(bf); n == 0 {
			return nil
		}
	}
	valueCnt := binary.LittleEndian.Uint32(d.buf[:4])
	nameCnt := binary.LittleEndian.Uint32(d.buf[4:8])

	d.buf = d.buf[8:]
	d.len -= 8

	if d.len < nameCnt {
		if n := d.Read(bf); n == 0 {
			return nil
		}
	}

	name := d.buf[:nameCnt]

	d.buf = d.buf[nameCnt:]
	d.len -= nameCnt

	data := make(map[string][]byte)
	for i := 0; uint32(i) < valueCnt; i++ {
		if d.len < 8 {
			if n := d.Read(bf); n == 0 {
				return nil
			}
		}
		lengthKey := binary.LittleEndian.Uint32(d.buf[:4])
		lengthData := binary.LittleEndian.Uint32(d.buf[4:8])

		total := lengthData + lengthKey + 8
		for d.len < total {
			if n := d.Read(bf); n == 0 {
				return nil
			}
		}

		key := string(name) + string(d.buf[8:lengthKey+8])
		data[key] = d.buf[lengthKey+8 : total]

		d.buf = d.buf[total:]
		d.len -= total
	}

	return load.NewPoint(&point{
		data:    data,
		dataCnt: uint64(valueCnt),
	})
}
