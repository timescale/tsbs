package main

import (
	"github.com/mediocregopher/radix/v3"
	"log"
	"sync"
)

func getStandaloneConn(addr string, opts []radix.DialOpt, clients uint64) *radix.Pool {
	var pool *radix.Pool
	var err error

	customConnFunc := func(network, addr string) (radix.Conn, error) {
		return radix.Dial(network, addr, opts...,
		)
	}
	network := "tcp"
	pool, err = radix.NewPool(network, addr, int(clients), radix.PoolConnFunc(customConnFunc), radix.PoolPipelineWindow(0, 0))
	if err != nil {
		log.Fatalf("Error preparing for benchmark, while creating new connection. error = %v", err)
	}
	return pool
}

func connectionProcessor(wg *sync.WaitGroup, rows chan string, metrics chan uint64, conn radix.Client) {
	cmds := make([][]radix.CmdAction, 1, 1)
	cmds[0] = make([]radix.CmdAction, 0, 0)
	curPipe := make([]uint64, 1, 1)
	curPipe[0] = 0
	currMetricCount := 0
	comdPos := 0

	for row := range rows {
		_, cmd, _, metricCount := buildCommand(row, compressionEnabled == false)
		currMetricCount += metricCount
		cmds[comdPos] = append(cmds[comdPos], cmd)
		curPipe[comdPos]++

		if curPipe[comdPos] == pipeline {
			err := conn.Do(radix.Pipeline(cmds[comdPos]...))
			if err != nil {
				log.Fatalf("Flush failed with %v", err)
			}
			metrics <- uint64(currMetricCount)
			currMetricCount = 0
			cmds[comdPos] = make([]radix.CmdAction, 0, 0)
			curPipe[comdPos] = 0
		}
	}
	for comdPos, u := range curPipe {
		if u > 0 {
			err := conn.Do(radix.Pipeline(cmds[comdPos]...))
			if err != nil {
				log.Fatalf("Flush failed with %v", err)
			}
			metrics <- uint64(currMetricCount)
		}
	}
	wg.Done()
}
