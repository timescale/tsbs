package akumuli

import (
	"encoding/binary"
	"github.com/timescale/tsbs/pkg/targets"
	"log"
	"net"
	"sync"
)

type processor struct {
	bufPool  *sync.Pool
	endpoint string
	conn     net.Conn
	worker   int
}

func (p *processor) Init(numWorker int, _, _ bool) {
	p.worker = numWorker
	c, err := net.Dial("tcp", p.endpoint)
	if err == nil {
		p.conn = c
		log.Println("Connection with", p.endpoint, "successful")
	} else {
		log.Println("Can't establish connection with", p.endpoint)
		panic("Connection error")
	}
}

func (p *processor) Close(doLoad bool) {
	if doLoad {
		p.conn.Close()
	}
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batch)
	var nmetrics uint64
	if doLoad {
		head := batch.buf.Bytes()
		for len(head) != 0 {
			nbytes := binary.LittleEndian.Uint16(head[4:6])
			nfields := binary.LittleEndian.Uint16(head[6:8])
			payload := head[8:nbytes]
			p.conn.Write(payload)
			nmetrics += uint64(nfields)
			head = head[nbytes:]
		}
	}
	batch.buf.Reset()
	p.bufPool.Put(batch.buf)
	return nmetrics, uint64(batch.rows)
}
