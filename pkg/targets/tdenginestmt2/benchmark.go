package tdenginestmt2

import "C"
import (
	"math"

	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/tdengine"
)

func NewBenchmark(dbName string, opts *tdengine.LoadingOptions, dataSourceConfig *source.DataSourceConfig) (targets.Benchmark, error) {
	var ds targets.DataSource
	if dataSourceConfig.Type == source.FileDataSourceType {
		ds = newFileDataSource(dataSourceConfig.File.Location)
		useCase, scale := ds.(*fileDataSource).readHeader()
		return &benchmark{
			opts:       opts,
			dataSource: ds,
			dbName:     dbName,
			factory:    NewBatchFactory(),
			useCase:    useCase,
			scale:      scale,
		}, nil
	} else {
		panic("not implement")
	}
}

type benchmark struct {
	opts            *tdengine.LoadingOptions
	dataSource      targets.DataSource
	dbName          string
	batchSize       uint
	factory         targets.BatchFactory
	indexer         targets.PointIndexer
	useCase         byte
	scale           uint32
	tableOffset     [3][]uint32
	partitionTables [3][][]uint32
}

func (b *benchmark) GetDataSource() targets.DataSource {
	return b.dataSource
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	return b.factory
}

func (b *benchmark) GetPointIndexer(uint) targets.PointIndexer {
	return b.indexer
}

func (b *benchmark) GetProcessor() targets.Processor {
	return newProcessor(b.opts, b.dbName, b.batchSize, b.useCase, b.scale, b.partitionTables, b.tableOffset)
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &DbCreator{
		DbCreator: tdengine.DbCreator{Opts: b.opts},
		useCase:   b.useCase,
		ds:        b.dataSource.(*fileDataSource),
	}
}

func (b *benchmark) SetConfig(batchSize uint, workers uint) {
	b.batchSize = batchSize

	b.dataSource.(*fileDataSource).SetConfig(int(workers), int(batchSize), int(b.scale))
	factory := b.factory.(*BatchFactory)
	factory.batchSize = batchSize
	if workers > 1 {
		interval := uint32(math.MaxUint32 / workers)
		hashEndGroups := make([]uint32, workers)
		for i := 0; i < int(workers); i++ {
			if i == int(workers)-1 {
				hashEndGroups[i] = math.MaxUint32
			} else {
				hashEndGroups[i] = interval*uint32(i+1) - 1
			}
		}
		prefix := []byte("1." + b.dbName + ".")
		idx, tableOffset, hostTableIndex, readingsTableIndex, diagnosticsTableIndex := NewIndexer(prefix, int(workers), hashEndGroups, b.useCase, b.scale)
		b.tableOffset = tableOffset
		b.partitionTables = [3][][]uint32{hostTableIndex, readingsTableIndex, diagnosticsTableIndex}
		b.indexer = idx
	} else {
		b.indexer = &targets.ConstantIndexer{}
		tmp := make([]uint32, b.scale+1)
		for i := uint32(0); i < b.scale+1; i++ {
			tmp[i] = i
		}
		switch b.useCase {
		case CpuCase:
			b.partitionTables[SuperTableHost] = [][]uint32{tmp}
			b.tableOffset[SuperTableHost] = tmp
		case IoTCase:
			b.partitionTables[SuperTableReadings] = [][]uint32{tmp}
			b.tableOffset[SuperTableReadings] = tmp
			b.partitionTables[SuperTableDiagnostics] = [][]uint32{tmp}
			b.tableOffset[SuperTableDiagnostics] = tmp
		}
	}
}
