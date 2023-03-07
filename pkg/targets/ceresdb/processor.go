package ceresdb

import (
	"context"
	"log"
	"time"

	"github.com/CeresDB/ceresdb-client-go/ceresdb"
	"github.com/timescale/tsbs/pkg/targets"
)

type processor struct {
	addr   string
	client ceresdb.Client
}

func (p *processor) Init(workerNum int, doLoad, hashWorkers bool) {
	client, err := ceresdb.NewClient(p.addr, ceresdb.Direct, ceresdb.WithDefaultDatabase("public"))
	if err != nil {
		panic(err)
	}
	p.client = client
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
	batch := b.(*batch)
	if !doLoad {
		return batch.fieldCount, batch.pointCount
	}
	mc, rc := p.do(batch)
	return mc, rc
}

func (p *processor) do(b *batch) (uint64, uint64) {
	for {
		ret, err := p.client.Write(context.TODO(), ceresdb.WriteRequest{Points: b.points})

		if err == nil {
			// log.Printf("success :%d\n", ret)
			return b.fieldCount, b.pointCount
		}

		log.Printf("Retrying, write failed. err:%s, ret:%d", err, ret)
		time.Sleep(time.Millisecond * 10)
	}
}
