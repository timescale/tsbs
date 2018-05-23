package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"

	"bitbucket.org/440-labs/tsbs/cmd/tsbs_generate_data/serialize"
	"bitbucket.org/440-labs/tsbs/load"
	"github.com/globalsign/mgo"
	flatbuffers "github.com/google/flatbuffers/go"
)

type decoder struct {
	lenBuf []byte
}

func (d *decoder) Decode(r *bufio.Reader) *load.Point {
	item := &serialize.MongoPoint{}

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

	return load.NewPoint(item)
}

type batch struct {
	arr []*serialize.MongoPoint
}

func (b *batch) Len() int {
	return len(b.arr)
}

func (b *batch) Append(item *load.Point) {
	that := item.Data.(*serialize.MongoPoint)
	b.arr = append(b.arr, that)
}

type factory struct{}

func (f *factory) New() load.Batch {
	return &batch{arr: []*serialize.MongoPoint{}}
}

type mongoBenchmark struct {
	l       *load.BenchmarkRunner
	session *mgo.Session
}

func (b *mongoBenchmark) GetPointDecoder(_ *bufio.Reader) load.PointDecoder {
	return &decoder{lenBuf: make([]byte, 8)}
}

func (b *mongoBenchmark) GetBatchFactory() load.BatchFactory {
	return &factory{}
}
