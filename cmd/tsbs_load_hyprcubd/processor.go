package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/timescale/tsbs/load"
)

type processor struct {
}

// Init does per-worker setup needed before receiving data
func (p *processor) Init(workerNum int, doLoad bool) {
	log.Println("processor.Init")
}

// ProcessBatch handles a single batch of data
func (p *processor) ProcessBatch(lb load.Batch, doLoad bool) (metricCount, rowCount uint64) {
	b := lb.(*batch)

	var numMetrics uint64
	tables := map[string]*tableBatch{}

	for _, p := range b.points {
		tb, ok := tables[p.table]
		if !ok {
			tb = newTableBatch(p.table)
			tables[p.table] = tb
		}

		tb.build(p)
	}

	for _, t := range tables {
		err := t.process()
		if err != nil {
			log.Fatalln(err)
		}
		numMetrics += t.numMetrics
	}

	return numMetrics, uint64(len(b.points))
}

type tableBatch struct {
	name       string
	schema     tableSchema
	stmt       strings.Builder
	first      bool
	numMetrics uint64
}

func newTableBatch(name string) *tableBatch {
	tb := &tableBatch{
		name:   name,
		schema: creator.tables[name],
		first:  true,
	}

	tb.stmt.WriteString("INSERT INTO " + name + " (time")

	for _, col := range tb.schema.cols {
		tb.stmt.WriteString(",")
		tb.stmt.WriteString(col.name)
	}

	tb.stmt.WriteString(",tags) VALUES ")

	return tb
}

func (tb *tableBatch) build(p *point) {
	// One field is timestmap
	tb.numMetrics += uint64(len(p.vals)) - 1

	if tb.first {
		tb.stmt.WriteString("(")
		tb.first = false
	} else {
		tb.stmt.WriteString(",(")
	}

	tb.stmt.WriteString(fmt.Sprintf("'%s'", p.ts.Format(time.RFC3339)))
	tb.stmt.WriteString("," + strings.Join(p.vals[1:], ","))

	tags := map[string]interface{}{}
	for _, tag := range p.tags {
		tags[tag.key] = tag.value
	}

	out, err := json.Marshal(tags)
	if err != nil {
		panic(err)
	}

	tb.stmt.WriteString(",'")
	tb.stmt.Write(out)
	tb.stmt.WriteString("')")
}

func (tb *tableBatch) string() string {
	return tb.stmt.String()
}

func (tb *tableBatch) process() error {
	_, err := runQuery(QueryRequest{
		Database: creator.db,
		Query:    tb.string(),
	})
	return err
}
