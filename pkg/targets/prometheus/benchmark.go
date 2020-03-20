package prometheus

import (
	"github.com/prometheus/prometheus/prompb"
	"github.com/timescale/tsbs/internal/inputs"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
	"log"
	"sync"
	"time"
)

func NewBenchmark(promSpecificConfig *SpecificConfig, dataSourceConfig *source.DataSourceConfig) (targets.Benchmark, error) {
	var ds targets.DataSource
	if dataSourceConfig.Type == source.FileDataSourceType {
		promIter, err := NewPrometheusIterator(load.GetBufferedReader(dataSourceConfig.File.Location))
		if err != nil {
			log.Printf("could not create prometheus file data source; %v", err)
			return nil, err
		}
		ds = &FileDataSource{iterator: promIter}
	} else {
		dataGenerator := &inputs.DataGenerator{}
		simulator, err := dataGenerator.CreateSimulator(dataSourceConfig.Simulator)
		if err != nil {
			return nil, err
		}
		ds = newSimulationDataSource(simulator)
	}

	batchPool := &sync.Pool{New: func() interface{} {
		return &Batch{}
	}}

	return &Benchmark{
		dataSource:      ds,
		batchPool:       batchPool,
		adapterWriteUrl: promSpecificConfig.AdapterWriteURL,
	}, nil
}

// Batch implements load.Batch interface
type Batch struct {
	series []prompb.TimeSeries
}

func (pb *Batch) Len() int {
	return len(pb.series)
}

func (pb *Batch) Append(item *data.LoadedPoint) {
	pb.series = append(pb.series, item.Data.(prompb.TimeSeries))
}

// FileDataSource implements the source.DataSource interface
type FileDataSource struct {
	iterator *Iterator
}

func (pd *FileDataSource) NextItem() *data.LoadedPoint {
	if pd.iterator.HasNext() {
		ts, err := pd.iterator.Next()
		if err != nil {
			panic(err)
		}
		return data.NewLoadedPoint(*ts)
	}
	return nil
}

func (pd *FileDataSource) Headers() *common.GeneratedDataHeaders {
	return nil
}

// PrometheusProcessor implements load.Processor interface
type Processor struct {
	client    *Client
	batchPool *sync.Pool
}

func (pp *Processor) Init(_ int, _, _ bool) {}

// ProcessBatch ..
func (pp *Processor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	promBatch := b.(*Batch)
	nrSamples := uint64(promBatch.Len())
	if doLoad {
		err := pp.client.Post(promBatch.series)
		if err != nil {
			panic(err)
		}
	}
	// reset batch
	promBatch.series = promBatch.series[:0]
	pp.batchPool.Put(promBatch)
	return nrSamples, nrSamples
}

// PrometheusBatchFactory implements Factory interface
type BatchFactory struct {
	batchPool *sync.Pool
}

func (pbf *BatchFactory) New() targets.Batch {
	return pbf.batchPool.Get().(*Batch)
}

// Benchmark implements targets.Benchmark interface
type Benchmark struct {
	adapterWriteUrl string
	dataSource      targets.DataSource
	batchPool       *sync.Pool
}

func (pm *Benchmark) GetDataSource() targets.DataSource {
	return pm.dataSource
}

func (pm *Benchmark) GetBatchFactory() targets.BatchFactory {
	return &BatchFactory{
		batchPool: pm.batchPool,
	}
}

func (pm *Benchmark) GetPointIndexer(_ uint) targets.PointIndexer {
	return &targets.ConstantIndexer{}
}

func (pm *Benchmark) GetProcessor() targets.Processor {
	client, err := NewClient(pm.adapterWriteUrl, time.Second*30)
	if err != nil {
		panic(err)
	}
	return &Processor{client: client, batchPool: pm.batchPool}
}

func (pm *Benchmark) GetDBCreator() targets.DBCreator {
	return nil
}
