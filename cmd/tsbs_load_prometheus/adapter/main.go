package main

import (
	"flag"

	"github.com/timescale/tsbs/cmd/tsbs_load_prometheus/adapter/noop"
)

var port int

func init() {
	flag.IntVar(&port, "port", 9876, "a port for adapter to listen on")
}

// Start noop Prometheus adapter. Useful for testing purposes
func main() {
	adapter := noop.NewAdapter(port)
	err := adapter.Start()
	if err != nil {
		panic(err)
	}
}
