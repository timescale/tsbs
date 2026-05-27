package main

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"time"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"
	"github.com/timescale/tsbs/pkg/targets/tdengine/async"
	"github.com/timescale/tsbs/pkg/targets/tdengine/commonpool"
)

var (
	user   string
	pass   string
	host   string
	port   int
	runner *query.BenchmarkRunner
)

func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("user", "root", "User to connect to TDengine")
	pflag.String("pass", "taosdata", "Password for the user connecting to TDengine")
	pflag.String("host", "", "TDengine host")
	pflag.Int("port", 6030, "TDengine Port")
	pflag.Parse()
	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}
	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}
	user = viper.GetString("user")
	pass = viper.GetString("pass")
	host = viper.GetString("host")
	port = viper.GetInt("port")
	runner = query.NewBenchmarkRunner(config)
}
func main() {
	runner.Run(&query.TDenginePool, newProcessor)
}

type queryExecutorOptions struct {
	debug         bool
	printResponse bool
}

type processor struct {
	db   *commonpool.Conn
	opts *queryExecutorOptions
}

func (p *processor) Init(workerNum int) {
	async.Init()
	db, err := commonpool.GetConnection(user, pass, host, port)
	if err != nil {
		panic(err)
	}
	dbName := runner.DatabaseName()
	err = async.GlobalAsync.TaosExecWithoutResult(db.TaosConnection, "use "+dbName)
	if err != nil {
		panic(err)
	}
	p.db = db
	p.opts = &queryExecutorOptions{
		debug:         runner.DebugLevel() > 0,
		printResponse: runner.DoPrintResponses(),
	}
}

func (p *processor) ProcessQuery(q query.Query, _ bool) ([]*query.Stat, error) {
	tq := q.(*query.TDengine)

	start := time.Now()
	qry := string(tq.SqlQuery)
	if p.opts.debug {
		fmt.Println(qry)
	}
	querys := strings.Split(qry, ";")
	if len(querys) > 1 {
		var preQuerys []string
		for i := 0; i < len(querys); i++ {
			if len(querys[i]) > 0 {
				preQuerys = append(preQuerys, querys[i])
			}
		}
		if len(preQuerys) > 1 {
			for i := 0; i < len(preQuerys)-1; i++ {
				err := async.GlobalAsync.TaosExecWithoutResult(p.db.TaosConnection, preQuerys[i])
				if err != nil {
					return nil, err
				}
			}
		}
		qry = querys[len(preQuerys)-1]
	}
	data, err := async.GlobalAsync.TaosExec(p.db.TaosConnection, qry, func(ts int64, precision int) driver.Value {
		return ts
	})
	if err != nil {
		return nil, err
	}
	if p.opts.printResponse {
		fmt.Printf("%#v\n", data)
	}
	took := float64(time.Since(start).Nanoseconds()) / 1e6
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), took)

	return []*query.Stat{stat}, err
}

func newProcessor() query.Processor { return &processor{} }
