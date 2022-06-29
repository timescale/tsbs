package main

import (
	"strconv"

	"github.com/mediocregopher/radix/v3"

	"strings"
	"sync"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/targets"
)

func buildCommand(line string, compression_type string) (clusterSlot int, cmdA radix.CmdAction, tscreate bool, metricCount int) {
	t := strings.Split(line, " ")
	metricCount = 1
	tscreate = false
	v, _ := strconv.ParseInt(t[0], 10, 0)
	clusterSlot = int(v)
	cmdname := t[1]
	if cmdname == "TS.CREATE" {
		tscreate = true
		metricCount = 0
		t = append([]string{t[0], t[1], t[2], compression_type}, t[3:]...)
	}
	if cmdname == "TS.MADD" {
		metricCount = (len(t) - 2) / 3
	}
	cmdA = radix.Cmd(nil, cmdname, t[2:]...)
	return
}

type eventsBatch struct {
	rows []string
}

func (eb *eventsBatch) Len() uint {
	return uint(len(eb.rows))
}

func (eb *eventsBatch) Append(item data.LoadedPoint) {
	that := item.Data.(string)
	eb.rows = append(eb.rows, that)
}

var ePool = &sync.Pool{New: func() interface{} { return &eventsBatch{rows: []string{}} }}

type factory struct{}

func (f *factory) New() targets.Batch {
	return ePool.Get().(*eventsBatch)
}
