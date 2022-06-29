package main

import (
	"strconv"
	"strings"
	"sync"

	"github.com/timescale/tsbs/pkg/targets"
)

type processor struct {
	dbc     *dbCreator
	rows    []chan string
	metrics chan uint64
	wg      *sync.WaitGroup
}

func (p *processor) Init(_ int, _ bool, _ bool) {}

// ProcessBatch reads eventsBatches which contain rows of data for TS.ADD redis command string
func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	events := b.(*eventsBatch)
	rowCnt := uint64(len(events.rows))
	metricCnt := uint64(0)

	if doLoad {
		buflen := rowCnt + 1
		p.rows = make([]chan string, connections)
		p.metrics = make(chan uint64, buflen)
		p.wg = &sync.WaitGroup{}

		for i := uint64(0); i < connections; i++ {
			p.rows[i] = make(chan string, buflen)
			p.wg.Add(1)
			if clusterMode {
				go connectionProcessorCluster(p.wg, compressionType, p.rows[i], p.metrics, cluster, len(addresses), addresses, slots, conns)
			} else {
				go connectionProcessor(p.wg, compressionType, p.rows[i], p.metrics, standalone)
			}
		}
		for _, row := range events.rows {
			slotS := strings.Split(row, " ")[0]
			clusterSlot, _ := strconv.ParseInt(slotS, 10, 0)
			i := uint64(clusterSlot) % connections
			p.rows[i] <- row
		}

		for i := uint64(0); i < connections; i++ {
			close(p.rows[i])
		}
		p.wg.Wait()
		close(p.metrics)
		for val := range p.metrics {
			metricCnt += val
		}
	}
	events.rows = events.rows[:0]
	ePool.Put(events)
	return metricCnt, rowCnt
}

func (p *processor) Close(_ bool) {
}
