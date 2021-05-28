package main

import (
	"bufio"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/targets"
	"log"
)

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	return &RedisIndexer{partitions: maxPartitions}
}

func (b *benchmark) GetProcessor() targets.Processor {
	return &processor{b.dbc, nil, nil, nil}
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return b.dbc
}

type benchmark struct {
	dbc *dbCreator
}

func (b *benchmark) GetDataSource() targets.DataSource {
	log.Printf("creating DS from %s", config.FileName)
	return &fileDataSource{scanner: bufio.NewScanner(load.GetBufferedReader(config.FileName))}
}
