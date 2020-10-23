package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/mongo"
)

type fileDataSource struct {
	lenBuf []byte
	r      *bufio.Reader
}

func (d *fileDataSource) NextItem() data.LoadedPoint {
	item := &mongo.MongoPoint{}

	_, err := d.r.Read(d.lenBuf)
	if err == io.EOF {
		return data.LoadedPoint{}
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
		m, err := d.r.Read(itemBuf[totRead:])
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

	return data.NewLoadedPoint(item)
}

func (d *fileDataSource) Headers() *common.GeneratedDataHeaders {
	return nil
}

type batch struct {
	arr []*mongo.MongoPoint
}

func (b *batch) Len() uint {
	return uint(len(b.arr))
}

func (b *batch) Append(item data.LoadedPoint) {
	that := item.Data.(*mongo.MongoPoint)
	b.arr = append(b.arr, that)
}

type factory struct{}

func (f *factory) New() targets.Batch {
	return &batch{arr: []*mongo.MongoPoint{}}
}

type mongoBenchmark struct {
	loaderFileName string
	l              load.BenchmarkRunner
	dbc            *dbCreator
}

func (b *mongoBenchmark) GetDataSource() targets.DataSource {
	return &fileDataSource{lenBuf: make([]byte, 8), r: load.GetBufferedReader(b.loaderFileName)}
}

func (b *mongoBenchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{}
}

func (b *mongoBenchmark) GetDBCreator() targets.DBCreator {
	return b.dbc
}
