package load

// DuplexChannel acts as a two-way channel for communicating from a scan routine
// to a worker goroutine. The toWorker channel sends data to the worker for it
// to process and the toScan channel allows the worker to acknowledge completion.
// Using this we can accomplish better flow control between the scanner and workers.
type DuplexChannel struct {
	toWorker  chan interface{}
	toScanner chan bool
}

// NewDuplexChannel returns a DuplexChannel whose buffer sizes are equal to queue
func NewDuplexChannel(queue int) *DuplexChannel {
	return &DuplexChannel{
		toWorker:  make(chan interface{}, queue),
		toScanner: make(chan bool, queue),
	}
}

// sendToWorker passes a batch of work on to the worker from the scanner
func (dc *DuplexChannel) sendToWorker(b interface{}) {
	dc.toWorker <- b
}

// SendToScanner passes an acknowledge to the scanner from the worker
func (dc *DuplexChannel) SendToScanner() {
	dc.toScanner <- true
}

// GetWorkerChannel returns the channel that data from the scanner comes on
func (dc *DuplexChannel) GetWorkerChannel() chan interface{} {
	return dc.toWorker
}

// Close closes down the DuplexChannel
func (dc *DuplexChannel) Close() {
	close(dc.toWorker)
	close(dc.toScanner)
}
