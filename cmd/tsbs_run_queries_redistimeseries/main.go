// tsbs_run_queries_redistimeseries speed tests RedisTimeSeries using requests from stdin or file
//

// This program has no knowledge of the internals of the endpoint.
package main

import (
	"fmt"
	"github.com/mediocregopher/radix/v3"
	"log"
	"math/rand"
	"strings"
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
	runner                            *query.BenchmarkRunner
	cmdMrange                         = []byte("TS.MRANGE")
	cmdMRevRange                      = []byte("TS.MREVRANGE")
	cmdQueryIndex                     = []byte("TS.QUERYINDEX")
	reflect_SingleGroupByTime         = query.GetFunctionName(query.SingleGroupByTime)
	reflect_GroupByTimeAndMax         = query.GetFunctionName(query.GroupByTimeAndMax)
	reflect_GroupByTimeAndTagMax      = query.GetFunctionName(query.GroupByTimeAndTagMax)
	reflect_GroupByTimeAndTagHostname = query.GetFunctionName(query.GroupByTimeAndTagHostname)
	reflect_HighCpu                   = query.GetFunctionName(query.HighCpu)
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
		//if p.opts.debug {
		//	fmt.Println(addresses)
		//	fmt.Println(slots)
		//	fmt.Println(conns)
		//}

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
	for _, qry := range tq.RedisQueries {
		cmds = append(cmds, ByteArrayToStringArray(qry))
	}
	start := time.Now()
	for idx, commandArgs := range cmds {
		var err error = nil
		if p.opts.debug {
			fmt.Println(fmt.Sprintf("Issuing command (%s %s)", string(tq.CommandNames[idx]), strings.Join(ByteArrayToStringArray(tq.RedisQueries[idx]), " ")))
		}
		if clusterMode {
			if string(tq.CommandNames[idx]) == "TS.MRANGE" || string(tq.CommandNames[idx]) == "TS.QUERYINDEX" || string(tq.CommandNames[idx]) == "TS.MGET" || string(tq.CommandNames[idx]) == "TS.MREVRANGE" {
				rPos := r.Intn(len(conns))
				conn := conns[rPos]
				err = conn.Do(radix.Cmd(nil, string(tq.CommandNames[idx]), commandArgs...))
			} else {
				err = cluster.Do(radix.Cmd(nil, string(tq.CommandNames[idx]), commandArgs...))
			}
		} else {
			err = standalone.Do(radix.Cmd(nil, string(tq.CommandNames[idx]), commandArgs...))
		}
		if err != nil {
			log.Fatalf("Command (%s %s) failed with error: %v\n", string(tq.CommandNames[idx]), strings.Join(ByteArrayToStringArray(tq.RedisQueries[idx]), " "), err)
		}
		if err != nil {
			return nil, err
		}
	}
	took := float64(time.Since(start).Nanoseconds()) / 1e6

	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), took)
	queryStats = []*query.Stat{stat}
	return queryStats, err
}

func ByteArrayToInterfaceArray(qry [][]byte) []interface{} {
	commandArgs := make([]interface{}, len(qry))
	for i := 0; i < len(qry); i++ {
		commandArgs[i] = qry[i]
	}
	return commandArgs
}

func ByteArrayToStringArray(qry [][]byte) []string {
	commandArgs := make([]string, len(qry))
	for i := 0; i < len(qry); i++ {
		commandArgs[i] = string(qry[i])
	}
	return commandArgs
}
