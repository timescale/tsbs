package load

import (
	"bufio"
	"reflect"
)

// ackAndMaybeSend adjust the outstanding batches count and potentially sends
// another batch to the worker via ch. If unsent is non-empty, send. Returns
// the updated state of unsent
func ackAndMaybeSend(ch *duplexChannel, count *int, unsent []Batch) []Batch {
	*count--
	// if there are still batches waiting, send the next
	if len(unsent) > 0 {
		ch.sendToWorker(unsent[0])
		if len(unsent) > 1 {
			return unsent[1:]
		}
		return unsent[:0]
	}
	return unsent
}

// sendOrQueueBatch attempts to send a Batch of data on a duplexChannel; if it would block
// or there is other work to be sent first, the Batch is stored on a queue. The count of
// outstanding work is adjusted upwards
func sendOrQueueBatch(ch *duplexChannel, count *int, batch Batch, unsent []Batch) []Batch {
	*count++
	if len(unsent) == 0 && len(ch.toWorker) < cap(ch.toWorker) {
		ch.sendToWorker(batch)
	} else {
		return append(unsent, batch)
	}
	return unsent
}

// Batch is an aggregate of points for a particular data system. It needs to have
// a way to measure it's size to make sure it does not get too large and a wait to
// append an item to it
type Batch interface {
	Len() int
	Append(*Point)
}

// Point acts a 'holder' for the internal representation of a point in a given
// load client. Instead of using interface{} as a return type, we get compile safety
// by using Point
type Point struct {
	Data interface{}
}

// NewPoint creates a Point with the provided data as the internal representation
func NewPoint(data interface{}) *Point {
	return &Point{Data: data}
}

// PointIndexer determines the index of the Batch (and subsequently the channel) that a particular
// point belongs to
type PointIndexer interface {
	// GetIndex returns a partition for the given Point
	GetIndex(*Point) int
}

// ConstantIndexer always puts the item on a single channel. This is the typical
// use case where all the workers share the same channel
type ConstantIndexer struct{}

// GetIndex returns a constant index (0) regardless of Point
func (i *ConstantIndexer) GetIndex(_ *Point) int { return 0 }

// BatchFactory returns a new empty batch for storing points.
type BatchFactory interface {
	// New returns a new Batch to add Points to
	New() Batch
}

// PointDecoder decodes the next data point in the process of scanning.
type PointDecoder interface {
	//Decode creates a Point from a data stream
	Decode(*bufio.Reader) *Point
}

// ScanWithIndexer reads data from the provided bufio.Reader br until a limit is reached (if -1, all items are read).
// Data is decoded by PointDecoder decoder and then placed into appropriate batches, using the supplied PointIndexer,
// which are then dispatched to workers (duplexChannel chosen by PointIndexer). Scan does flow control to make sure workers are not left idle for too long
// and also that the scanning process  does not starve them of CPU.
func scanWithIndexer(channels []*duplexChannel, batchSize int, limit int64, br *bufio.Reader, decoder PointDecoder, factory BatchFactory, indexer PointIndexer) int64 {
	var itemsRead int64
	numChannels := len(channels)

	// Current batches (per channel) that are being filled
	batches := make([]Batch, numChannels)
	for i := range batches {
		batches[i] = factory.New()
	}

	// Batches that are ready to be set when space on a channel opens
	unsent := make([][]Batch, numChannels)
	for i := range unsent {
		unsent[i] = []Batch{}
	}

	// We use Select via reflection to either select an acknowledged channel so
	// that we can potentially send another batch, or if none are ready to continue
	// on scanning. However, when we reach a limit of outstanding batches, we also
	// want to block until one worker is done, so as not to starve the workers.
	// Using an array with Select via reflection gives us this flexibility (i.e., we can
	// either pass the whole array of cases, or the array less the last item).
	cases := make([]reflect.SelectCase, numChannels+1)
	for i, ch := range channels {
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch.toScanner)}
	}
	cases[numChannels] = reflect.SelectCase{Dir: reflect.SelectDefault}

	// Keep track of how many batches are outstanding (ocnt) so we don't go over
	// a limit (olimit), in order to slow down the scanner so it doesn't starve the workers
	ocnt := 0
	olimit := numChannels * cap(channels[0].toWorker) * 3
	for {
		if itemsRead == limit {
			break
		}

		caseLimit := len(cases)
		if ocnt >= olimit { // if we have too many outstanding, wait until one finishes (i.e. no default)
			caseLimit--
		}

		// Only receive an 'ok' when it's from a channel, default does not return 'ok'
		chosen, _, ok := reflect.Select(cases[:caseLimit])
		if ok {
			unsent[chosen] = ackAndMaybeSend(channels[chosen], &ocnt, unsent[chosen])
		}

		item := decoder.Decode(br)
		if item == nil {
			break
		}

		idx := indexer.GetIndex(item)
		batches[idx].Append(item)

		itemsRead++
		if batches[idx].Len() >= batchSize {
			unsent[idx] = sendOrQueueBatch(channels[idx], &ocnt, batches[idx], unsent[idx])
			batches[idx] = factory.New()
		}
	}

	// Finished reading input, make sure last batch goes out.
	for idx, b := range batches {
		if b.Len() > 0 {
			unsent[idx] = sendOrQueueBatch(channels[idx], &ocnt, batches[idx], unsent[idx])
		}
	}

	// Wait until all the outstanding batches get acknowledged so we don't
	// prematurely close the acknowledge channels
	for {
		if ocnt == 0 {
			break
		}

		chosen, _, ok := reflect.Select(cases[:len(cases)-1])
		if ok {
			unsent[chosen] = ackAndMaybeSend(channels[chosen], &ocnt, unsent[chosen])
		}
	}

	return itemsRead
}
