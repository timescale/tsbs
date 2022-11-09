package main

import (
	"fmt"
	"log"
	"time"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"

	"github.com/apache/iotdb-client-go/client"
)

// database option vars
var (
	clientConfig client.Config
	timeoutInMs  int64 // 0 for no timeout
)

// Global vars:
var (
	runner *query.BenchmarkRunner
)

// Parse args:
func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("host", "localhost", "Hostname of IoTDB instance")
	pflag.String("port", "6667", "Which port to connect to on the database host")
	pflag.String("user", "root", "The user who connect to IoTDB")
	pflag.String("password", "root", "The password for user connecting to IoTDB")

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	host := viper.GetString("host")
	port := viper.GetString("port")
	user := viper.GetString("user")
	password := viper.GetString("password")
	workers := viper.GetUint("workers")
	timeoutInMs = 0 // 0 for no timeout

	log.Printf("tsbs_run_queries_iotdb target: %s:%s. Loading with %d workers.\n", host, port, workers)
	if workers < 5 {
		log.Println("Insertion throughput is strongly related to the number of threads. Use more workers for better performance.")
	}

	clientConfig = client.Config{
		Host:     host,
		Port:     port,
		UserName: user,
		Password: password,
	}

	runner = query.NewBenchmarkRunner(config)
}

func main() {
	runner.Run(&query.IoTDBPool, newProcessor)
}

type processor struct {
	session client.Session
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(workerNumber int) {
	p.session = client.NewSession(&clientConfig)
	if err := p.session.Open(false, int(timeoutInMs)); err != nil {
		errMsg := fmt.Sprintf("query processor init error, session is not open: %v\n", err)
		errMsg = errMsg + fmt.Sprintf("timeout setting: %d ms", timeoutInMs)
		log.Fatal(errMsg)
	}
}

func (p *processor) ProcessQuery(q query.Query, _ bool) ([]*query.Stat, error) {
	iotdbQ := q.(*query.IoTDB)
	sql := string(iotdbQ.SqlQuery)

	start := time.Now().UnixNano()
	_, err := p.session.ExecuteQueryStatement(sql, &timeoutInMs) // 0 for no timeout

	took := time.Now().UnixNano() - start
	if err != nil {
		// CRTODO 更换一个更合适的方式
		log.Printf("log! ERROR! %v", err)
		return nil, err
	}
	lag := float64(took) / float64(time.Millisecond) // in milliseconds
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), lag)
	return []*query.Stat{stat}, err
}
