package main

import (
	"bufio"
	"encoding/binary"
	"io"
	"log"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
)

type point struct {
	data    map[string][]byte
	dataCnt uint64
}

type batch struct {
	series    map[string][]byte
	batchCnt  uint
	metricCnt uint64
}

func (b *batch) Len() uint {
	return b.batchCnt
}

func (b *batch) Append(item data.LoadedPoint) {
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

func (f *factory) New() targets.Batch {
	return &batch{
		series:    map[string][]byte{},
		batchCnt:  0,
		metricCnt: 0,
	}
}

type fileDataSource struct {
	buf []byte
	len uint32
	br  *bufio.Reader
}

func (d *fileDataSource) Read() int {
	buf := make([]byte, 8192)
	n, err := d.br.Read(buf)
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

func (d *fileDataSource) Headers() *common.GeneratedDataHeaders {
	return nil
}

func (d *fileDataSource) NextItem() data.LoadedPoint {
	if d.len < 8 {
		if n := d.Read(); n == 0 {
			return data.LoadedPoint{}
		}
	}
	valueCnt := binary.LittleEndian.Uint32(d.buf[:4])
	nameCnt := binary.LittleEndian.Uint32(d.buf[4:8])

	d.buf = d.buf[8:]
	d.len -= 8

	if d.len < nameCnt {
		if n := d.Read(); n == 0 {
			return data.LoadedPoint{}
		}
	}

	name := d.buf[:nameCnt]

	d.buf = d.buf[nameCnt:]
	d.len -= nameCnt

	newPoint := make(map[string][]byte)
	for i := 0; uint32(i) < valueCnt; i++ {
		if d.len < 8 {
			if n := d.Read(); n == 0 {
				return data.LoadedPoint{}
			}
		}
		lengthKey := binary.LittleEndian.Uint32(d.buf[:4])
		lengthData := binary.LittleEndian.Uint32(d.buf[4:8])

		total := lengthData + lengthKey + 8
		for d.len < total {
			if n := d.Read(); n == 0 {
				return data.LoadedPoint{}
			}
		}

		key := string(name) + string(d.buf[8:lengthKey+8])
		newPoint[key] = d.buf[lengthKey+8 : total]

		d.buf = d.buf[total:]
		d.len -= total
	}

	return data.NewLoadedPoint(&point{
		data:    newPoint,
		dataCnt: uint64(valueCnt),
	})
}
