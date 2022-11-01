package main

import (
	"fmt"

	"github.com/apache/iotdb-client-go/client"
	"github.com/timescale/tsbs/pkg/targets"
)

type processor struct {
	session client.Session
}

func (p *processor) Init(_ int, doLoad, _ bool) {
	if !doLoad {
		return
	}
	p.session = client.NewSession(&clientConfig)
	if err := p.session.Open(false, timeoutInNs); err != nil {
		errMsg := fmt.Sprintf("dbCreator init error, session is not open: %v\n", err)
		errMsg = errMsg + fmt.Sprintf("Maybe your configuration is inappropriate, please check: %v", clientConfig)
		fatal(errMsg)
	}
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
	batch := b.(*iotdbBatch)

	// Write records
	if doLoad {
		for _, row := range batch.points {
			sql := row.generateInsertStatement()
			p.session.ExecuteUpdateStatement(sql)
		}
	}

	metricCount = batch.metrics
	rowCount = uint64(batch.rows)
	return metricCount, rowCount
}
