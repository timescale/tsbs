package main

import (
	"encoding/binary"
	"log"
	"net"

	"github.com/timescale/tsbs/load"
)

type processor struct {
	endpoint string
	conn     net.Conn
	worker   int
}

func (p *processor) Init(numWorker int, _ bool) {
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

func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
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
	bufPool.Put(batch.buf)
	return nmetrics, batch.rows
}
