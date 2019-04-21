package main

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/timescale/tsbs/load"
)

type processor struct {
	endpoint string
	pool     *tsdbConnPool
	ncon     uint
}

func (p *processor) Init(numWorker int, _ bool) {
	fmt.Println("processor - ix worker:", numWorker, "num connections:", p.ncon)
	p.pool = createTsdbPool(uint32(p.ncon), p.endpoint, time.Second*10, time.Second*10)
}

func (p *processor) Close(_ bool) {
	p.pool.Close()
}

func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batch)
	var nmetrics uint64 = 0
	if doLoad {
		head := batch.buf.Bytes()
		for len(head) != 0 {
			nbytes := binary.LittleEndian.Uint16(head[4:6])
			nfields := binary.LittleEndian.Uint16(head[6:8])
			shardid := binary.LittleEndian.Uint32(head[:4])
			payload := head[8:nbytes]
			p.pool.Write(shardid, payload)
			head = head[nbytes:]
			nmetrics += uint64(nfields)
		}
	}
	batch.buf.Reset()
	bufPool.Put(batch.buf)
	return nmetrics, batch.rows
}
