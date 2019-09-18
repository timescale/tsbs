// tsbs_run_queries_siridb speed tests SiriDB using requests from stdin or file
//

// This program has no knowledge of the internals of the endpoint.
package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	siridb "github.com/SiriDB/go-siridb-connector"
	_ "github.com/lib/pq"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/query"
)

// Program option vars:
var (
	hosts        string
	writeTimeout int
	dbUser       string
	dbPass       string
	showExplain  bool
	scale        uint64
	queryLimit   uint64
)

// Global vars:
var (
	runner *query.BenchmarkRunner
)

var (
	siridbConnector *siridb.Client
)

// Parse args:
func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("dbuser", "iris", "Username to enter SiriDB")
	pflag.String("dbpass", "siri", "Password to enter SiriDB")
	pflag.String("hosts", "localhost:9000", "Comma separated list of SiriDB hosts in a cluster.")
	pflag.Uint64("scale", 8, "Scaling variable (Must be the equal to the scalevar used for data generation).")
	pflag.Uint64("query-limit", 1000000, "Changes the maximum points which can be returned by a select query.")
	pflag.Int("write-timeout", 10, "Write timeout.")
	pflag.Bool("show-explain", false, "Print out the EXPLAIN output for sample query")

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	dbUser = viper.GetString("dbuser")
	dbPass = viper.GetString("dbpass")
	hosts = viper.GetString("hosts")
	scale = viper.GetUint64("scale")
	queryLimit = viper.GetUint64("query-limit")
	writeTimeout = viper.GetInt("write-timeout")
	showExplain = viper.GetBool("show-explain")

	runner = query.NewBenchmarkRunner(config)

	if showExplain {
		runner.SetLimit(1)
	}

	hostlist := [][]interface{}{}
	listhosts := strings.Split(hosts, ",")

	for _, hostport := range listhosts {
		x := strings.Split(hostport, ":")
		host := x[0]
		port, err := strconv.ParseInt(x[1], 10, 0)
		if err != nil {
			log.Fatal(err)
		}
		hostlist = append(hostlist, []interface{}{host, int(port)})
	}

	siridbConnector = siridb.NewClient(
		dbUser,                // username
		dbPass,                // password
		runner.DatabaseName(), // database
		hostlist,              // siridb server(s)
		nil,                   // optional log channel
	)
}

func main() {
	siridbConnector.Connect()
	ChangeQueryLimit()
	CreateGroups()

	runner.Run(&query.SiriDBPool, newProcessor)
	siridbConnector.Close()
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

// Changes the maximum points which can be returned by a select query. The default
// and recommended value is set to one million points. This value is chosen to
// prevent a single query for taking to much memory and ensures SiriDB can respond
// to almost any query in a reasonable amount of time.
func ChangeQueryLimit() {
	qry := fmt.Sprintf("alter database set select_points_limit %d", queryLimit)

	if siridbConnector.IsConnected() {
		if _, err := siridbConnector.Query(qry, uint16(writeTimeout)); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("not even a single server is connected...")
	}
}

// CreateGroups makes groups representing regular expression to enhance performance
func CreateGroups() {
	created := true
	metrics := devops.GetAllCPUMetrics()
	siriql := make([]string, 0, 2048)
	for _, m := range metrics {
		siriql = append(siriql, fmt.Sprintf("create group `%s` for /.*%s$/", m, m))
	}

	var n uint64
	for n = 0; n < scale; n++ {
		host := fmt.Sprintf("host_%d", n)
		siriql = append(siriql, fmt.Sprintf("create group `%s` for /.*%s,.*/", host, host))
	}
	siriql = append(siriql, fmt.Sprintf("create group `cpu` for /.*^cpu.*/"))
	for _, qry := range siriql {
		if siridbConnector.IsConnected() {
			if _, err := siridbConnector.Query(qry, uint16(writeTimeout)); err != nil {
				created = false
			}
		} else {
			log.Fatal("not even a single server is connected...")
		}
	}
	if created {
		time.Sleep(6 * time.Second) // because the groups are created in a seperate thread every 2 seconds.
	}
}

func (p *processor) Init(numWorker int) {
	p.opts = &queryExecutorOptions{
		showExplain:   showExplain,
		debug:         runner.DebugLevel() > 0,
		printResponse: runner.DoPrintResponses(),
	}
}

func (p *processor) ProcessQuery(q query.Query, isWarm bool) ([]*query.Stat, error) {

	// No need to run again for EXPLAIN
	if isWarm && p.opts.showExplain {
		return nil, nil
	}
	tq := q.(*query.SiriDB)

	start := time.Now()
	qry := string(tq.SqlQuery)

	var res interface{}
	var err error

	if siridbConnector.IsConnected() {
		if res, err = siridbConnector.Query(qry, uint16(writeTimeout)); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("not even a single server is connected...")
	}

	if p.opts.debug {
		fmt.Println(qry)
	}

	if p.opts.printResponse {
		fmt.Println("\n", res)
	}

	took := float64(time.Since(start).Nanoseconds()) / 1e6
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), took)

	return []*query.Stat{stat}, err
}
