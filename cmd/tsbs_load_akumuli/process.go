package main

import (
	"fmt"

	"github.com/timescale/tsbs/load"
)

type processor struct {
}

func (p *processor) Init(numWorker int, _ bool) {
	fmt.Println("processor - NumWroker:", numWorker)
}

func (p *processor) Close(_ bool) {
}

func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
	panic("Not implemented")
}
