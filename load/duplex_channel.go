package load

import "github.com/timescale/tsbs/pkg/targets"

// duplexChannel acts as a two-way channel for communicating from a scan routine
// to a worker goroutine. The toWorker channel sends data to the worker for it
// to process and the toScan channel allows the worker to acknowledge completion.
// Using this we can accomplish better flow control between the scanner and workers.
type duplexChannel struct {
	toWorker  chan targets.Batch
	toScanner chan bool
}

// newDuplexChannel returns a duplexChannel with specified buffer sizes
func newDuplexChannel(queueLen int) *duplexChannel {
	return &duplexChannel{
		toWorker:  make(chan targets.Batch, queueLen),
		toScanner: make(chan bool, queueLen),
	}
}

// sendToWorker passes a batch of work on to the worker from the scanner
func (dc *duplexChannel) sendToWorker(b targets.Batch) {
	dc.toWorker <- b
}

// sendToScanner passes an acknowledge to the scanner from the worker
func (dc *duplexChannel) sendToScanner() {
	dc.toScanner <- true
}

// close closes down the duplexChannel
func (dc *duplexChannel) close() {
	close(dc.toWorker)
	close(dc.toScanner)
}
