package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/mongo"
	"io"
	"log"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/timescale/tsbs/load"
)

type decoder struct {
	lenBuf []byte
}

func (d *decoder) Decode(r *bufio.Reader) *targets.Point {
	item := &mongo.MongoPoint{}

	_, err := r.Read(d.lenBuf)
	if err == io.EOF {
		return nil
	}
	if err != nil {
		log.Fatal(err.Error())
	}

	// ensure correct len of receiving buffer
	l := int(binary.LittleEndian.Uint64(d.lenBuf))
	itemBuf := make([]byte, l)

	// read the bytes and init the flatbuffer object
	totRead := 0
	for totRead < l {
		m, err := r.Read(itemBuf[totRead:])
		// (EOF is also fatal)
		if err != nil {
			log.Fatal(err.Error())
		}
		totRead += m
	}
	if totRead != len(itemBuf) {
		panic(fmt.Sprintf("reader/writer logic error, %d != %d", totRead, len(itemBuf)))
	}
	n := flatbuffers.GetUOffsetT(itemBuf)
	item.Init(itemBuf, n)

	return targets.NewPoint(item)
}

type batch struct {
	arr []*mongo.MongoPoint
}

func (b *batch) Len() int {
	return len(b.arr)
}

func (b *batch) Append(item *targets.Point) {
	that := item.Data.(*mongo.MongoPoint)
	b.arr = append(b.arr, that)
}

type factory struct{}

func (f *factory) New() targets.Batch {
	return &batch{arr: []*mongo.MongoPoint{}}
}

type mongoBenchmark struct {
	l   *load.BenchmarkRunner
	dbc *dbCreator
}

func (b *mongoBenchmark) GetPointDecoder(_ *bufio.Reader) targets.PointDecoder {
	return &decoder{lenBuf: make([]byte, 8)}
}

func (b *mongoBenchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{}
}

func (b *mongoBenchmark) GetDBCreator() targets.DBCreator {
	return b.dbc
}
