package clickhouse

import (
	"bufio"
	"fmt"
	"log"

	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/targets"
)

const dbType = "clickhouse"

type ClickhouseConfig struct {
	Host     string
	User     string
	Password string

	LogBatches bool
	InTableTag bool
	Debug      int
	DbName     string
}

// String values of tags and fields to insert - string representation
type insertData struct {
	tags   string // hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,rack=67,os=Ubuntu16.10,arch=x86,team=NYC,service=7,service_version=0,service_environment=production
	fields string // 1451606400000000000,58,2,24,61,22,63,6,44,80,38
}

var tableCols map[string][]string

var tagColumnTypes []string

// allows for testing
var fatal = log.Fatalf

// getConnectString() builds connect string to ClickHouse
// db - whether database specification should be added to the connection string
func getConnectString(conf *ClickhouseConfig, db bool) string {
	// connectString: tcp://127.0.0.1:9000?debug=true
	// ClickHouse ex.:
	// tcp://host1:9000?username=user&password=qwerty&database=clicks&read_timeout=10&write_timeout=20&alt_hosts=host2:9000,host3:9000
	if db {
		return fmt.Sprintf("tcp://%s:9000?username=%s&password=%s&database=%s", conf.Host, conf.User, conf.Password, conf.DbName)
	}

	return fmt.Sprintf("tcp://%s:9000?username=%s&password=%s", conf.Host, conf.User, conf.Password)
}

// Point is a single row of data keyed by which table it belongs
// Ex.:
// tags,hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,rack=67,os=Ubuntu16.10,arch=x86,team=NYC,service=7,service_version=0,service_environment=production
// cpu,1451606400000000000,58,2,24,61,22,63,6,44,80,38
type point struct {
	table string
	row   *insertData
}

// scan.Batch interface implementation
type tableArr struct {
	m   map[string][]*insertData
	cnt uint
}

// scan.Batch interface implementation
func (ta *tableArr) Len() uint {
	return ta.cnt
}

// scan.Batch interface implementation
func (ta *tableArr) Append(item data.LoadedPoint) {
	that := item.Data.(*point)
	k := that.table
	ta.m[k] = append(ta.m[k], that.row)
	ta.cnt++
}

// scan.BatchFactory interface implementation
type factory struct{}

// scan.BatchFactory interface implementation
func (f *factory) New() targets.Batch {
	return &tableArr{
		m:   map[string][]*insertData{},
		cnt: 0,
	}
}

const tagsPrefix = "tags"

func NewBenchmark(file string, hashWorkers bool, conf *ClickhouseConfig) targets.Benchmark {
	return &benchmark{
		ds: &fileDataSource{
			scanner: bufio.NewScanner(load.GetBufferedReader(file)),
		},
		hashWorkers: hashWorkers,
		conf:        conf,
	}
}

// targets.Benchmark interface implementation
type benchmark struct {
	ds          targets.DataSource
	hashWorkers bool
	conf        *ClickhouseConfig
}

func (b *benchmark) GetDataSource() targets.DataSource {
	return b.ds
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	if b.hashWorkers {
		return &hostnameIndexer{
			partitions: maxPartitions,
		}
	}
	return &targets.ConstantIndexer{}
}

// loader.Benchmark interface implementation
func (b *benchmark) GetProcessor() targets.Processor {
	return &processor{conf: b.conf}
}

// loader.Benchmark interface implementation
func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{ds: b.GetDataSource(), config: b.conf}
}
