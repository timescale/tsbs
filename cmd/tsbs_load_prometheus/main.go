package main

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/prometheus"
	"sync"
	"time"

	"github.com/prometheus/prometheus/prompb"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// runs the benchmark
var loader *load.BenchmarkRunner
var promBatchPool sync.Pool = sync.Pool{New: func() interface{} { return &PrometheusBatch{} }}

// PrometheusBatch implements load.Batch interface
type PrometheusBatch struct {
	series []prompb.TimeSeries
}

func (pb *PrometheusBatch) Len() int {
	return len(pb.series)
}

func (pb *PrometheusBatch) Append(item *data.LoadedPoint) {
	pb.series = append(pb.series, item.Data.(prompb.TimeSeries))
}

// PrometheusDecoder implements scan.PointDecoder interface
type PrometheusFileDataSource struct {
	iterator *prometheus.PrometheusIterator
}

func (pd *PrometheusFileDataSource) NextItem() *data.LoadedPoint {
	if pd.iterator.HasNext() {
		ts, err := pd.iterator.Next()
		if err != nil {
			panic(err)
		}
		return data.NewLoadedPoint(*ts)
	}
	return nil
}

func (pd *PrometheusFileDataSource) Headers() *common.GeneratedDataHeaders {
	return nil
}

// PrometheusProcessor implements load.Processor interface
type PrometheusProcessor struct {
	client *Client
}

func (pp *PrometheusProcessor) Init(_ int, _ bool) {}

// ProcessBatch ..
func (pp *PrometheusProcessor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	promBatch := b.(*PrometheusBatch)
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
type PrometheusBatchFactory struct{}

func (pbf *PrometheusBatchFactory) New() targets.Batch {
	return promBatchPool.Get().(*PrometheusBatch)
}

// PrometheusBenchmark implements Benchmark interface
type PrometheusBenchmark struct{}

func (pm *PrometheusBenchmark) GetDataSource() source.DataSource {
	promIter, err := prometheus.NewPrometheusIterator(load.GetBufferedReader(loader.FileName))
	if err != nil {
		panic(err)
	}
	return &PrometheusFileDataSource{iterator: promIter}
}

func (pm *PrometheusBenchmark) GetBatchFactory() targets.BatchFactory {
	return &PrometheusBatchFactory{}
}

func (pm *PrometheusBenchmark) GetPointIndexer(_ uint) targets.PointIndexer {
	return &targets.ConstantIndexer{}
}

func (pm *PrometheusBenchmark) GetProcessor() targets.Processor {
	client, err := NewClient(adapterWriteUrl, time.Second*30)
	if err != nil {
		panic(err)
	}
	return &PrometheusProcessor{client: client}
}

func (pm *PrometheusBenchmark) GetDBCreator() targets.DBCreator {
	return nil
}

var adapterWriteUrl string

func init() {
	var config load.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.StringVar(&adapterWriteUrl, "adapter-write-url", "", "Prometheus adapter url to send data to")
	pflag.Parse()

	err := utils.SetupConfigFile()
	if err != nil {
		panic(fmt.Errorf("error setting up a config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	loader = load.GetBenchmarkRunner(config)
}

func main() {
	loader.RunBenchmark(&PrometheusBenchmark{}, load.SingleQueue)
}
