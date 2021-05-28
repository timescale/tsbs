package main

import (
	"github.com/mediocregopher/radix/v3"
	"log"
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
