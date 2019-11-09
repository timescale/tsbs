package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/timescale/tsbs/load"
	"strings"
)

type processor struct {
	tableDefs []*tableDef
	connCfg   *pgx.ConnConfig
	pool      *pgxpool.Pool
}

// load.Processor interface implementation
func (p *processor) Init(workerNum int, doLoad bool) {
	if !doLoad {
		return
	}
	pool, err := pgxpool.ConnectConfig(context.Background(), &pgxpool.Config{
		ConnConfig: p.connCfg,
		MaxConns:   int32(workerNum),
	})
	if err != nil {
		fatal("cannot create a new connection pool: %v", err)
		panic(err)
	}
	p.pool = pool
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
func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
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
		b.Queue(table, *row, nil, nil)
		// a number of metric values is all row values minus tags and timestamp
		// this is required by the framework to count the number of inserted
		// metric values
		metricCnt += uint64(len(*row) - 2)
	}
	batchResults := p.pool.SendBatch(context.Background(), &b)
	if err := batchResults.Close(); err != nil {
		fatal("failed to close a batch operation %v", err)
	}
	return metricCnt
}

// load.ProcessorCloser interface implementation
func (p *processor) Close(doLoad bool) {
	if doLoad {
		p.pool.Close()
	}
}
