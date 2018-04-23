package main

import "bitbucket.org/440-labs/influxdb-comparisons/cmd/tsbs_generate_data/serialize"

// duplexChannel acts as a two-way channel for communicating from a scan routine
// to a worker goroutine. The toWorker channel sends data to the worker for it
// to process and the toScan channel allows the worker to acknowledge completion.
// Using this we can accomplish better flow control between the scanner and workers.
type duplexChannel struct {
	toWorker  chan []*serialize.MongoPoint
	toScanner chan bool
}

func newDuplexChannel(queue int) *duplexChannel {
	return &duplexChannel{
		toWorker:  make(chan []*serialize.MongoPoint, queue),
		toScanner: make(chan bool, queue),
	}
}

// sendToWorker passes a batch of work on to the worker from the scanner
func (dc *duplexChannel) sendToWorker(b []*serialize.MongoPoint) {
	dc.toWorker <- b
}

// sendToScan passes an acknowledge to the scanner from the worker
func (dc *duplexChannel) sendToScanner() {
	dc.toScanner <- true
}

func (dc *duplexChannel) close() {
	close(dc.toWorker)
	close(dc.toScanner)
}
