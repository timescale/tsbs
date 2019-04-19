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
	fmt.Println("processor - NumWroker:", numWorker)
	p.pool = createTsdbPool(uint32(p.ncon), p.endpoint, time.Second*10, time.Second*10)
}

func (p *processor) Close(_ bool) {
	p.pool.Close()
}

func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batch)
	if doLoad {
		head := batch.buf.Bytes()
		fmt.Println("processor - ProcessBatch:", len(head))
		for len(head) != 0 {
			nbytes := binary.LittleEndian.Uint16(head[4:6])
			shardid := binary.LittleEndian.Uint32(head[:4])
			payload := head[6:nbytes]
			p.pool.Write(shardid, payload)
			head = head[nbytes:]
		}
	}
	batch.buf.Reset()
	bufPool.Put(batch.buf)
	return batch.rows, batch.rows
}
