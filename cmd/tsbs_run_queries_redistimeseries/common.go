package main

import (
	"fmt"
	"github.com/mediocregopher/radix/v3"
	"github.com/timescale/tsbs/pkg/query"
	"log"
	"strings"
)

func inner_cmd_logic(p *processor, tq *query.RedisTimeSeries, idx int, replies [][]interface{}, commandArgs []string) error {
	var err error = nil
	if p.opts.debug {
		fmt.Println(fmt.Sprintf("Issuing command (%s %s)", string(tq.CommandNames[idx]), strings.Join(commandArgs, " ")))
	}
	if clusterMode {
		if string(tq.CommandNames[idx]) == "TS.MRANGE" || string(tq.CommandNames[idx]) == "TS.QUERYINDEX" || string(tq.CommandNames[idx]) == "TS.MGET" || string(tq.CommandNames[idx]) == "TS.MREVRANGE" {
			rPos := r.Intn(len(conns))
			conn := conns[rPos]
			err = conn.Do(radix.Cmd(&replies[idx], string(tq.CommandNames[idx]), commandArgs...))
		} else {
			err = cluster.Do(radix.Cmd(&replies[idx], string(tq.CommandNames[idx]), commandArgs...))
		}
	} else {
		err = standalone.Do(radix.Cmd(&replies[idx], string(tq.CommandNames[idx]), commandArgs...))
	}
	if err != nil {
		log.Fatalf("Command (%s %s) failed with error: %v\n", string(tq.CommandNames[idx]), strings.Join(ByteArrayToStringArray(tq.RedisQueries[idx]), " "), err)
	}
	return err
}
