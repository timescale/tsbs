package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/timescale/tsbs/pkg/targets"
	"strings"
)

type processor struct {
	tableDefs map[string]*tableDef
	connCfg   *pgx.ConnConfig
	conn      *pgx.Conn
}

// load.Processor interface implementation
func (p *processor) Init(workerNum int, doLoad, _ bool) {
	if !doLoad {
		return
	}
	conn, err := pgx.ConnectConfig(context.Background(), p.connCfg)
	if err != nil {
		fatal("cannot create a new connection pool: %v", err)
		panic(err)
	}
	p.conn = conn
}

const InsertStmt = "INSERT INTO %s (%s) VALUES (%s)"

func (p *processor) createInsertStmt(table *tableDef) (string, error) {
	var cols []string
	cols = append(cols, "tags", "ts")

	for _, col := range table.cols {
		cols = append(cols, col)
	}

	stmt := fmt.Sprintf(
		InsertStmt,
		table.fqn(),
		strings.Join(cols, ","),
		strings.Repeat(",?", len(cols))[1:],
	)

	return stmt, nil
}

// load.Processor interface implementation
func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	eb := b.(*eventsBatch)
	rowCnt := uint64(0)
	metricCnt := uint64(0)

	for table, rows := range eb.batches {
		rowCnt += uint64(len(rows))
		if doLoad {
			metricCnt += p.InsertBatch(table, rows)
		}
	}
	return metricCnt, rowCnt
}

// load.Processor interface implementation
func (p *processor) InsertBatch(table string, rows []*row) uint64 {
	metricCnt := uint64(0)
	b := pgx.Batch{}
	for _, row := range rows {
		insertStmt, err := p.createInsertStmt(p.tableDefs[table])
		if err != nil {
			fatal("could not create insert statement for table %s", table)
		}
		b.Queue(insertStmt, *row...)
		// a number of metric values is all row values minus tags and timestamp
		// this is required by the framework to count the number of inserted
		// metric values
		metricCnt += uint64(len(*row) - 2)
	}
	batchResults := p.conn.SendBatch(context.Background(), &b)
	if err := batchResults.Close(); err != nil {
		fatal("failed to close a batch operation %v", err)
	}
	return metricCnt
}

// load.ProcessorCloser interface implementation
func (p *processor) Close(doLoad bool) {
	if doLoad {
		p.conn.Close(context.Background())
	}
}
