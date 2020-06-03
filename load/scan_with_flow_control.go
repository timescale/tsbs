package load

import (
	"reflect"

	"github.com/timescale/tsbs/pkg/targets"
)

// ackAndMaybeSend adjust the unsent batches count
// and sends one batch (if any available) to the worker via ch.
// Returns the updated state of unsent
func ackAndMaybeSend(ch *duplexChannel, count *int, unsent []targets.Batch) []targets.Batch {
	*count--
	// If there are still batches waiting, send the next
	if len(unsent) > 0 {
		ch.sendToWorker(unsent[0])
		if len(unsent) > 1 {
			return unsent[1:]
		}
		return unsent[:0]
	}
	return unsent
}

// sendOrQueueBatch attempts to send a Batch of data on a duplexChannel.
// If it would block or there is other work to be sent first, the Batch is stored on a queue.
// The count of outstanding work is adjusted upwards
func sendOrQueueBatch(ch *duplexChannel, count *int, batch targets.Batch, unsent []targets.Batch) []targets.Batch {
	// In case there are no outstanding batches yet and there are empty positions in toWorker queue
	// we can send/put batch into toWorker queue
	*count++
	if len(unsent) == 0 && len(ch.toWorker) < cap(ch.toWorker) {
		ch.sendToWorker(batch)
	} else {
		return append(unsent, batch)
	}
	return unsent
}

// scanWithFlowControl reads data from the DataSource ds until a limit is reached (if -1, all items are read).
// Data is then placed into appropriate batches, using the supplied PointIndexer,
// which are then dispatched to workers (duplexChannel chosen by PointIndexer).
// Scan does flow control to make sure workers are not left idle for too long
// and also that the scanning process does not starve them of CPU.
func scanWithFlowControl(
	channels []*duplexChannel, batchSize uint, limit uint64,
	ds targets.DataSource, factory targets.BatchFactory, indexer targets.PointIndexer,
) uint64 {
	var itemsRead uint64
	numChannels := len(channels)

	if batchSize < 1 {
		panic("--batch-size cannot be less than 1")
	}

	// Batches details
	// 1. fillingBatches contains batches that are being filled with items from scanner.
	//    As soon a batch has batchSize items in it, or there is no more items to come, batch moves to unsentBatches.
	// 2. unsentBatches contains batches ready to be sent to a worker.
	//    As soon as a worker's chan is available (i.e., not blocking), the batch is placed onto that worker's chan.

	// Current batches (per channel) that are being filled with items from scanner
	fillingBatches := make([]targets.Batch, numChannels)
	for i := range fillingBatches {
		fillingBatches[i] = factory.New()
	}

	// Batches that are ready to be set when space on a channel opens
	unsentBatches := make([][]targets.Batch, numChannels)
	for i := range unsentBatches {
		unsentBatches[i] = []targets.Batch{}
	}

	// We use Select via reflection to either select an acknowledged channel so
	// that we can potentially send another batch, or if none are ready to continue
	// on scanning. However, when we reach a limit of outstanding (unsent) batches,
	// we also want to block until one worker is done, so as not to starve the workers.
	// Using an array with Select via reflection gives us this flexibility (i.e.,
	// we can either pass the whole array of cases, or the array less the last item).
	cases := make([]reflect.SelectCase, numChannels+1)
	for i, ch := range channels {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch.toScanner),
		}
	}
	cases[numChannels] = reflect.SelectCase{
		Dir: reflect.SelectDefault,
	}

	// Keep track of how many batches are outstanding (ocnt),
	// so we don't go over a limit (olimit), in order to slow down the scanner so it doesn't starve the workers
	ocnt := 0
	olimit := numChannels * cap(channels[0].toWorker) * 3
	for {

		// Check whether incoming items limit reached.
		// We do not want to process more items than specified.
		if limit > 0 && itemsRead == limit {
			break
		}

		caseLimit := len(cases)
		if ocnt >= olimit {
			// We have too many outstanding batches, wait until one finishes (i.e. no default)
			caseLimit--
		}

		// Only receive an 'ok' when it's from a channel, default does not return 'ok'
		chosen, _, ok := reflect.Select(cases[:caseLimit])
		if ok {
			unsentBatches[chosen] = ackAndMaybeSend(channels[chosen], &ocnt, unsentBatches[chosen])
		}

		// Prepare new batch - decode new item and append it to batch
		item := ds.NextItem()
		if item.Data == nil {
			// Nothing to scan any more - input is empty or failed
			// Time to exit
			break
		}
		itemsRead++

		// Append new item to batch
		idx := indexer.GetIndex(item)
		fillingBatches[idx].Append(item)

		if fillingBatches[idx].Len() >= batchSize {
			// Batch is full (contains at least batchSize items) - ready to be sent to worker,
			// or moved to outstanding, in case no workers available atm.
			unsentBatches[idx] = sendOrQueueBatch(channels[idx], &ocnt, fillingBatches[idx], unsentBatches[idx])
			// Place new empty batch
			fillingBatches[idx] = factory.New()
		}
	}

	// Finished reading input - no more items to come
	// Make sure last batch goes out - it may be smaller than batchSize requested - there is not more items
	for idx, b := range fillingBatches {
		// Do not enqueue empty batches (with 0 items)
		if b.Len() > 0 {
			unsentBatches[idx] = sendOrQueueBatch(channels[idx], &ocnt, fillingBatches[idx], unsentBatches[idx])
		}
	}

	// Wait until all the outstanding batches get acknowledged,
	// so we don't prematurely close the acknowledge channels
	for {
		if ocnt == 0 {
			// No outstanding batches any more
			break
		}

		// Try to send batches to workers
		chosen, _, ok := reflect.Select(cases[:len(cases)-1])
		if ok {
			unsentBatches[chosen] = ackAndMaybeSend(channels[chosen], &ocnt, unsentBatches[chosen])
		}
	}

	return itemsRead
}
