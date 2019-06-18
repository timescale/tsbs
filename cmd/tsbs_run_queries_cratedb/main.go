package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/jackc/pgx"
	"time"

	_ "github.com/jackc/pgx/stdlib"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
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
	runner = query.NewBenchmarkRunner()

	flag.StringVar(&hosts, "hosts", "localhost", "CrateDB hostnames")
	flag.StringVar(&user, "user", "crate", "User to connect to CrateDB")
	flag.StringVar(&pass, "pass", "", "Password for user connecting to CrateDB")
	flag.IntVar(&port, "port", 5432, "A port to connect to database instances")
	flag.BoolVar(&showExplain, "show-explain", false, "Print out the EXPLAIN output for sample query")

	flag.Parse()

	if showExplain {
		runner.SetLimit(1)
	}

}
func main() {
	runner.Run(&query.CrateDBPool, newProcessor)
}

type processor struct {
	pool    *pgx.ConnPool
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
			Host:     hosts,
			Port:     uint16(port),
			User:     user,
			Password: pass,
			Database: runner.DatabaseName(),
		},
		opts: &executorOptions{
			showExplain:   showExplain,
			debug:         runner.DebugLevel() > 0,
			printResponse: runner.DoPrintResponses(),
		},
	}
}

func (p *processor) Init(workerNumber int) {
	pool, err := pgx.NewConnPool(
		pgx.ConnPoolConfig{
			MaxConnections: workerNumber,
			ConnConfig:     *p.connCfg,
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
	rows, err := p.pool.Query(qry)
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
func prettyPrintResponse(rows *pgx.Rows, q *query.CrateDB) {
	resp := make(map[string]interface{})
	resp["query"] = string(q.SqlQuery)
	resp["results"] = mapRows(rows)

	line, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		panic(err)
	}

	fmt.Println(string(line) + "\n")
}

func mapRows(r *pgx.Rows) []map[string]interface{} {
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
			row[column.Name] = *values[i].(*interface{})
		}
		rows = append(rows, row)
	}
	return rows
}
