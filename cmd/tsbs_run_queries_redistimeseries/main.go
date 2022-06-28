// tsbs_run_queries_redistimeseries speed tests RedisTimeSeries using requests from stdin or file
//

// This program has no knowledge of the internals of the endpoint.
package main

import (
	"fmt"
	"github.com/mediocregopher/radix/v3"
	"math/rand"
	"time"

	"github.com/blagojts/viper"
	_ "github.com/lib/pq"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// Program option vars:
var (
	host        string
	showExplain bool
	clusterMode bool
	cluster     *radix.Cluster
	standalone  *radix.Pool
	addresses   []string
	slots       [][][2]uint16
	conns       []radix.Client
	r           *rand.Rand
)

// Global vars:
var (
	runner        *query.BenchmarkRunner
	cmdMrange     = []byte("TS.MRANGE")
	cmdMRevRange  = []byte("TS.MREVRANGE")
	cmdQueryIndex = []byte("TS.QUERYINDEX")
)

// Parse args:
func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.StringVar(&host, "host", "localhost:6379", "Redis host address and port")
	pflag.BoolVar(&clusterMode, "cluster", false, "Whether to use OSS cluster API")
	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	s := rand.NewSource(time.Now().Unix())
	r = rand.New(s) // initialize local pseudorandom generator

	opts := make([]radix.DialOpt, 0)
	opts = append(opts, radix.DialReadTimeout(120*time.Second))
	if clusterMode {
		cluster = getOSSClusterConn(host, opts, uint64(config.Workers))
		cluster.Sync()
		topology := cluster.Topo().Primaries().Map()
		addresses = make([]string, 0)
		slots = make([][][2]uint16, 0)
		conns = make([]radix.Client, 0)
		for nodeAddress, node := range topology {
			addresses = append(addresses, nodeAddress)
			slots = append(slots, node.Slots)
			conn, _ := cluster.Client(nodeAddress)
			conns = append(conns, conn)
		}
		// Print cluster addresses after sync
		if config.Debug > 0 {
			fmt.Println("Printing cluster connection details after ")
			fmt.Println(fmt.Sprintf("Cluster Addresses: %s", addresses))
			fmt.Println(fmt.Sprintf("Cluster slots: %s", slots))
		}

	} else {
		standalone = getStandaloneConn(host, opts, uint64(config.Workers))
	}
	runner = query.NewBenchmarkRunner(config)
}
func main() {
	runner.Run(&query.RedisTimeSeriesPool, newProcessor)
}

type queryExecutorOptions struct {
	showExplain   bool
	debug         bool
	printResponse bool
}

type processor struct {
	opts *queryExecutorOptions
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(numWorker int) {
	p.opts = &queryExecutorOptions{
		showExplain:   showExplain,
		debug:         runner.DebugLevel() > 0,
		printResponse: runner.DoPrintResponses(),
	}
}

func (p *processor) ProcessQuery(q query.Query, isWarm bool) (queryStats []*query.Stat, err error) {

	// No need to run again for EXPLAIN
	if isWarm && p.opts.showExplain {
		return nil, nil
	}
	tq := q.(*query.RedisTimeSeries)

	var cmds = make([][]string, 0, 0)
	var replies = make([][]interface{}, 0, 0)
	for _, qry := range tq.RedisQueries {
		cmds = append(cmds, ByteArrayToStringArray(qry))
		replies = append(replies, []interface{}{})
	}

	start := time.Now()
	for idx, commandArgs := range cmds {
		err := inner_cmd_logic(p, tq, idx, replies, commandArgs)
		if tq.Functor == "FILTER_BY_TS" {
			err = highCpuFilterByTsFunctor(tq, replies, idx, commandArgs, p, err)
		}
		if err != nil {
			return nil, err
		}
		if p.opts.debug {
			debug_print_redistimeseries_reply(replies, idx, tq)
		}
	}
	took := float64(time.Since(start).Nanoseconds()) / 1e6

	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), took)
	queryStats = []*query.Stat{stat}
	return queryStats, err
}
