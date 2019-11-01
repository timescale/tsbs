package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4/pgxpool"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	_ "github.com/jackc/pgx/v4/stdlib"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/query"
)

var (
	hosts       string
	user        string
	pass        string
	port        int
	showExplain bool
)

var runner *query.BenchmarkRunner

func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("hosts", "localhost", "CrateDB hostnames")
	pflag.String("user", "crate", "User to connect to CrateDB")
	pflag.String("pass", "", "Password for user connecting to CrateDB")
	pflag.Int("port", 5432, "A port to connect to database instances")
	pflag.Bool("show-explain", false, "Print out the EXPLAIN output for sample query")

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	hosts = viper.GetString("hosts")
	user = viper.GetString("user")
	pass = viper.GetString("pass")
	port = viper.GetInt("port")
	showExplain = viper.GetBool("show-explain")

	runner = query.NewBenchmarkRunner(config)

	if showExplain {
		runner.SetLimit(1)
	}
}

func main() {
	runner.Run(&query.CrateDBPool, newProcessor)
}

type processor struct {
	pool    *pgxpool.Pool
	connCfg *pgx.ConnConfig
	opts    *executorOptions
}

type executorOptions struct {
	showExplain   bool
	debug         bool
	printResponse bool
}

func newProcessor() query.Processor {
	return &processor{
		connCfg: &pgx.ConnConfig{
			Config: pgconn.Config{
				Host:     hosts,
				Port:     uint16(port),
				User:     user,
				Password: pass,
				Database: runner.DatabaseName(),
			},
		},
		opts: &executorOptions{
			showExplain:   showExplain,
			debug:         runner.DebugLevel() > 0,
			printResponse: runner.DoPrintResponses(),
		},
	}
}

func (p *processor) Init(workerNumber int) {
	pool, err := pgxpool.ConnectConfig(context.Background(),
		&pgxpool.Config{
			MaxConns:   int32(workerNumber),
			ConnConfig: p.connCfg,
		})
	if err != nil {
		panic(err)
	}
	p.pool = pool
}

func (p *processor) ProcessQuery(q query.Query, isWarm bool) ([]*query.Stat, error) {
	// No need to run again for EXPLAIN
	if isWarm && p.opts.showExplain {
		return nil, nil
	}
	tq := q.(*query.CrateDB)

	start := time.Now()
	qry := string(tq.SqlQuery)
	if showExplain {
		qry = "EXPLAIN ANALYZE " + qry
	}
	rows, err := p.pool.Query(context.Background(), qry)
	if err != nil {
		return nil, err
	}

	if p.opts.debug {
		fmt.Println(qry)
	}
	if showExplain {
		fmt.Printf("Explian Query:\n")
		prettyPrintResponse(rows, tq)
		fmt.Printf("\n-----------\n\n")
	} else if p.opts.printResponse {
		prettyPrintResponse(rows, tq)
	}
	defer rows.Close()

	took := float64(time.Since(start).Nanoseconds()) / 1e6
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), took)

	return []*query.Stat{stat}, err
}

// prettyPrintResponse prints a Query and its response in JSON format with two
// keys: 'query' which has a value of the SQL used to generate the second key
// 'results' which is an array of each row in the return set.
func prettyPrintResponse(rows pgx.Rows, q *query.CrateDB) {
	resp := make(map[string]interface{})
	resp["query"] = string(q.SqlQuery)
	resp["results"] = mapRows(rows)

	line, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		panic(err)
	}

	fmt.Println(string(line) + "\n")
}

func mapRows(r pgx.Rows) []map[string]interface{} {
	var rows []map[string]interface{}
	cols := r.FieldDescriptions()
	for r.Next() {
		row := make(map[string]interface{})
		values := make([]interface{}, len(cols))
		for i := range values {
			values[i] = new(interface{})
		}

		err := r.Scan(values...)
		if err != nil {
			panic(errors.Wrap(err, "error while reading values"))
		}

		for i, column := range cols {
			row[string(column.Name)] = *values[i].(*interface{})
		}
		rows = append(rows, row)
	}
	return rows
}
