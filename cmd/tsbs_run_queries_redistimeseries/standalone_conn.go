package main

import (
	"github.com/mediocregopher/radix/v3"
	"log"
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
