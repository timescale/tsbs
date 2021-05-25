package main

import (
	"fmt"
	"net"

	"github.com/timescale/tsbs/pkg/targets"
)

// allows for testing
var printFn = fmt.Printf

type processor struct {
	ilpConn (*net.TCPConn)
}

func (p *processor) Init(numWorker int, _, _ bool) {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", questdbILPBindTo)
	if err != nil {
		fatal("Failed to resolve %s: %s\n", questdbILPBindTo, err.Error())
	}
	p.ilpConn, err = net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		fatal("Failed connect to %s: %s\n", questdbILPBindTo, err.Error())
	}
}

func (p *processor) Close(_ bool) {
	defer p.ilpConn.Close()
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batch)

	// Write the batch: try until backoff is not needed.
	if doLoad {
		var err error
		_, err = p.ilpConn.Write(batch.buf.Bytes())
		if err != nil {
			fatal("Error writing: %s\n", err.Error())
		}
	}

	metricCnt := batch.metrics
	rowCnt := batch.rows

	// Return the batch buffer to the pool.
	batch.buf.Reset()
	bufPool.Put(batch.buf)
	return metricCnt, uint64(rowCnt)
}
