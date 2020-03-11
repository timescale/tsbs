package prometheus

import (
	"github.com/prometheus/prometheus/prompb"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
	"sync"
	"time"
)

var promBatchPool = sync.Pool{New: func() interface{} { return &Batch{} }}

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
	iterator *PrometheusIterator
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
	client *Client
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
	promBatchPool.Put(promBatch)
	return nrSamples, nrSamples
}

// PrometheusBatchFactory implements Factory interface
type BatchFactory struct{}

func (pbf *BatchFactory) New() targets.Batch {
	return promBatchPool.Get().(*Batch)
}

// Benchmark implements targets.Benchmark interface
type Benchmark struct {
	AdapterWriteUrl string
	FileNameToLoad  string
}

func (pm *Benchmark) GetDataSource() targets.DataSource {
	promIter, err := NewPrometheusIterator(load.GetBufferedReader(pm.FileNameToLoad))
	if err != nil {
		panic(err)
	}
	return &FileDataSource{iterator: promIter}
}

func (pm *Benchmark) GetBatchFactory() targets.BatchFactory {
	return &BatchFactory{}
}

func (pm *Benchmark) GetPointIndexer(_ uint) targets.PointIndexer {
	return &targets.ConstantIndexer{}
}

func (pm *Benchmark) GetProcessor() targets.Processor {
	client, err := NewClient(pm.AdapterWriteUrl, time.Second*30)
	if err != nil {
		panic(err)
	}
	return &Processor{client: client}
}

func (pm *Benchmark) GetDBCreator() targets.DBCreator {
	return nil
}
