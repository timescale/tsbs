package main

import (
	"bufio"
	"os"
	"reflect"

	"bitbucket.org/440-labs/influxdb-comparisons/cmd/tsbs_generate_data/serialize"
)

// ackAndMaybeSend adjust the outstanding batches count and potentially sends
// another batch to the worker via ch. If unsent is non-empty, send. Returns
// the updated state of unsent
func ackAndMaybeSend(ch *duplexChannel, count *int, unsent [][]*serialize.MongoPoint) [][]*serialize.MongoPoint {
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

func sendOrQueueBatch(ch *duplexChannel, count *int, batch []*serialize.MongoPoint, unsent [][]*serialize.MongoPoint) [][]*serialize.MongoPoint {
	*count++
	if len(unsent) == 0 && len(ch.toWorker) < cap(ch.toWorker) {
		ch.sendToWorker(batch)
	} else {
		return append(unsent, batch)
	}
	return unsent
}

type batchIndexer interface {
	GetIndex(*serialize.MongoPoint) int
}

func scanWithIndexer(channels []*duplexChannel, itemsPerBatch int, indexer batchIndexer) int64 {
	var itemsRead int64
	r := bufio.NewReaderSize(os.Stdin, 1<<20)
	lenBuf := make([]byte, 8)
	numChannels := len(channels)

	batches := make([][]*serialize.MongoPoint, numChannels)

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

	unsent := make([][][]*serialize.MongoPoint, numChannels)
	for i := range unsent {
		unsent[i] = [][]*serialize.MongoPoint{}
	}

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
		chosen, _, ok := reflect.Select(cases[:caseLimit])

		// Only receive an 'ok' when it's from a channel, default does not return 'ok'
		if ok {
			unsent[chosen] = ackAndMaybeSend(channels[chosen], &ocnt, unsent[chosen])
		}

		item := decodeMongoPoint(r, lenBuf)
		if item == nil {
			break
		}

		idx := indexer.GetIndex(item)
		batches[idx] = append(batches[idx], item)

		itemsRead++
		if len(batches[idx]) >= itemsPerBatch {
			unsent[idx] = sendOrQueueBatch(channels[idx], &ocnt, batches[idx], unsent[idx])
			batches[idx] = batches[idx][:0]
		}
	}
	// Finished reading input, make sure last batch goes out.
	for i, val := range batches {
		if len(val) > 0 {
			unsent[i] = sendOrQueueBatch(channels[i], &ocnt, batches[i], unsent[i])
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
