package main

import (
	"github.com/mediocregopher/radix/v3"
	"github.com/timescale/tsbs/pkg/data"
	"log"
	"strconv"
	"strings"
	"sync"
)

func getOSSClusterConn(addr string, opts []radix.DialOpt, clients uint64) *radix.Cluster {
	var vanillaCluster *radix.Cluster
	var err error

	customConnFunc := func(network, addr string) (radix.Conn, error) {
		return radix.Dial(network, addr, opts...,
		)
	}

	// this cluster will use the ClientFunc to create a pool to each node in the
	// cluster.
	poolFunc := func(network, addr string) (radix.Client, error) {
		return radix.NewPool(network, addr, int(clients), radix.PoolConnFunc(customConnFunc), radix.PoolPipelineWindow(0, 0))
	}

	vanillaCluster, err = radix.NewCluster([]string{addr}, radix.ClusterPoolFunc(poolFunc))
	if err != nil {
		log.Fatalf("Error preparing for benchmark, while creating new connection. error = %v", err)
	}
	// Issue CLUSTER SLOTS command
	err = vanillaCluster.Sync()
	if err != nil {
		log.Fatalf("Error preparing for benchmark, while issuing CLUSTER SLOTS. error = %v", err)
	}
	return vanillaCluster
}

func nodeThatContainsSlot(slots [][][2]uint16, slot int) (result int) {
	result = -1
	for nodePos, slotGroup := range slots {
		for _, i2 := range slotGroup {
			if slot >= int(i2[0]) && slot < int(i2[1]) {
				result = nodePos
				return
			}
		}
	}
	return
}

func connectionProcessorCluster(wg *sync.WaitGroup, rows chan string, metrics chan uint64, cluster *radix.Cluster, clusterNodes int, addresses []string, slots [][][2]uint16, conns []radix.Client) {
	cmds := make([][]radix.CmdAction, clusterNodes, clusterNodes)
	curPipe := make([]uint64, clusterNodes, clusterNodes)
	currMetricCount := make([]int, clusterNodes, clusterNodes)
	for i := 0; i < clusterNodes; i++ {
		cmds[i] = make([]radix.CmdAction, 0, 0)
		curPipe[i] = 0
		currMetricCount[i] = 0
	}

	for row := range rows {
		slot, cmd, _, metricCount := buildCommand(row, compressionEnabled == false)
		comdPos := nodeThatContainsSlot(slots, slot)
		var err error = nil

		currMetricCount[comdPos] += metricCount
		cmds[comdPos] = append(cmds[comdPos], cmd)
		curPipe[comdPos]++

		if curPipe[comdPos] == pipeline {
			err = conns[comdPos].Do(radix.Pipeline(cmds[comdPos]...))
			if err != nil {
				log.Fatalf("Flush failed with %v", err)
			}
			metrics <- uint64(currMetricCount[comdPos])
			currMetricCount[comdPos] = 0
			cmds[comdPos] = make([]radix.CmdAction, 0, 0)
			curPipe[comdPos] = 0
		}

	}
	for comdPos, u := range curPipe {
		if u > 0 {
			var err error = nil
			err = conns[comdPos].Do(radix.Pipeline(cmds[comdPos]...))
			if err != nil {
				log.Fatalf("Flush failed with %v", err)
			}
			metrics <- uint64(currMetricCount[comdPos])
		}
	}
	wg.Done()
}

type RedisIndexer struct {
	partitions uint
}

func (i *RedisIndexer) GetIndex(p data.LoadedPoint) uint {
	row := p.Data.(string)
	slotS := strings.Split(row, " ")[0]
	clusterSlot, _ := strconv.ParseInt(slotS, 10, 0)
	return uint(clusterSlot) % i.partitions
}
