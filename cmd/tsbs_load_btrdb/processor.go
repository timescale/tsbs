package main

import (
	"github.com/iznauy/tsbs/load"
	"time"
)

type processor struct {
	backingOffChan chan bool
	backingOffDone chan struct{}
	client         *btrdbClient
}

func (p *processor) Init(workerNum int, doLoad bool) {
	p.client = NewBTrDBClient()
}

func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (metricCount, rowCount uint64) {
	batch := b.(*insertionBatch)
	if doLoad {
		for _, insert := range batch.insertions {
			if insert == nil {
				continue
			}
			err := p.client.insert(insert)

			if err != nil {
				fatal("encounter error while inserting data into btrdb: %v", err)
			}
			time.Sleep(backoff)
		}
	}
	return batch.metrics, batch.rows
}

func (p *processor) Close(doLoad bool) {

}

