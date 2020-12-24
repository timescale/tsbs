package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/SiriDB/go-siridb-connector"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/transceptor-technology/go-qpack"
)

type processor struct {
	connection *siridb.Connection
}

func (p *processor) Init(numWorker int, _, _ bool) {
	hostlist := strings.Split(hosts, ",")
	h := hostlist[numWorker%len(hostlist)]
	x := strings.Split(h, ":")
	host := x[0]
	port, err := strconv.ParseUint(x[1], 10, 16)
	if err != nil {
		fatal(err)
	}
	p.connection = siridb.NewConnection(host, uint16(port))
}

func (p *processor) Close(doLoad bool) {
	if doLoad {
		p.connection.Close()
	}
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rows uint64) {
	batch := b.(*batch)
	if doLoad {
		if err := p.connection.Connect(dbUser, dbPass, loader.DatabaseName()); err != nil {
			fatal(err)
		}
		series := make([]byte, 0)
		series = append(series, byte(253)) // qpack: "open map"
		for k, v := range batch.series {
			key, err := qpack.Pack(k) // packs a string in the right format for SiriDB
			if err != nil {
				log.Fatal(err)
			}
			series = append(series, key...)
			series = append(series, v...)
		}
		start := time.Now()
		if _, err := p.connection.InsertBin(series, uint16(writeTimeout)); err != nil {
			fatal(err)
		}
		if logBatches {
			now := time.Now()
			took := now.Sub(start)
			batchSize := batch.batchCnt
			fmt.Printf("BATCH: batchsize %d insert rate %f/sec (took %v)\n", batchSize, float64(batchSize)/float64(took.Seconds()), took)
		}
	}
	metricCount = batch.metricCnt
	batch.series = map[string][]byte{}
	batch.batchCnt = 0
	batch.metricCnt = 0
	return metricCount, 0
}
