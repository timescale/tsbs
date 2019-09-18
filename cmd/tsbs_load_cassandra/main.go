// bulk_load_cassandra loads a Cassandra daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gocql/gocql"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
)

// Program option vars:
var (
	hosts             string
	replicationFactor int
	consistencyLevel  string
	writeTimeout      time.Duration
)

// Global vars
var (
	loader *load.BenchmarkRunner
)

// Map of user specified strings to gocql consistency settings
var consistencyMapping = map[string]gocql.Consistency{
	"ALL":    gocql.All,
	"ANY":    gocql.Any,
	"QUORUM": gocql.Quorum,
	"ONE":    gocql.One,
	"TWO":    gocql.Two,
	"THREE":  gocql.Three,
}

// Parse args:
func init() {
	var config load.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("hosts", "localhost:9042", "Comma separated list of Cassandra hosts in a cluster.")

	pflag.Int("replication-factor", 1, "Number of nodes that must have a copy of each key.")
	pflag.String("consistency", "ALL", "Desired write consistency level. See Cassandra consistency documentation. Default: ALL")
	pflag.Duration("write-timeout", 10*time.Second, "Write timeout.")

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	hosts = viper.GetString("hosts")
	replicationFactor = viper.GetInt("replication-factor")
	consistencyLevel = viper.GetString("consistency")
	writeTimeout = viper.GetDuration("write-timeout")

	if _, ok := consistencyMapping[consistencyLevel]; !ok {
		fmt.Println("Invalid consistency level.")
		os.Exit(1)
	}

	loader = load.GetBenchmarkRunnerWithBatchSize(config, 100)
}

type benchmark struct {
	dbc *dbCreator
}

func (b *benchmark) GetPointDecoder(br *bufio.Reader) load.PointDecoder {
	return &decoder{scanner: bufio.NewScanner(br)}
}

func (b *benchmark) GetBatchFactory() load.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(_ uint) load.PointIndexer {
	return &load.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() load.Processor {
	return &processor{b.dbc}
}

func (b *benchmark) GetDBCreator() load.DBCreator {
	return b.dbc
}

func main() {
	loader.RunBenchmark(&benchmark{dbc: &dbCreator{}}, load.SingleQueue)
}

type processor struct {
	dbc *dbCreator
}

func (p *processor) Init(_ int, _ bool) {}

// ProcessBatch reads eventsBatches which contain rows of CQL strings and
// creates a gocql.LoggedBatch to insert
func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
	events := b.(*eventsBatch)

	if doLoad {
		batch := p.dbc.clientSession.NewBatch(gocql.LoggedBatch)
		for _, event := range events.rows {
			batch.Query(singleMetricToInsertStatement(event))
		}

		err := p.dbc.clientSession.ExecuteBatch(batch)
		if err != nil {
			log.Fatalf("Error writing: %s\n", err.Error())
		}
	}
	metricCnt := uint64(len(events.rows))
	events.rows = events.rows[:0]
	ePool.Put(events)
	return metricCnt, 0
}
