package iot

import (
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

const (
	// The default size of a batch of entries within a simulation.
	defaultBatchSize = 10
)

// SimulatorConfig is used to create an IoT Simulator.
// It fulfills the common.SimulatorConfig interface.
type SimulatorConfig common.BaseSimulatorConfig

// NewSimulator produces an IoT Simulator with the given
// config over the specified interval and points limit.
func (sc *SimulatorConfig) NewSimulator(interval time.Duration, limit uint64) common.Simulator {
	s := (*common.BaseSimulatorConfig)(sc).NewSimulator(interval, limit)

	return &Simulator{
		base:            s,
		batchSize:       defaultBatchSize,
		configGenerator: newBatchConfig,
	}
}

// Simulator is responsible for simulating entries for the IoT use case.
// It will run on batches of entries and apply the generated batch configuration
// which it gets from the config generator. That way it can introduce things like
// missing entries or batches, out of order entries or batches etc.
type Simulator struct {
	base            common.Simulator
	batchSize       uint
	configGenerator func(outOfOrderBatchCount, outOfOrderEntryCount, fieldCount, tagCount int) *batchConfig

	// Mutable state.
	currBatch         []*serialize.Point
	outOfOrderBatches [][]*serialize.Point
	outOfOrderEntries []*serialize.Point
	// offset is used for dealing with batch generation and keeping the
	// insert index consistent.
	offset int
}

// Fields returns the fields of an entry.
func (s Simulator) Fields() map[string][][]byte {
	return s.base.Fields()
}

// TagKeys returns the tag keys of an entry.
func (s Simulator) TagKeys() [][]byte {
	return s.base.TagKeys()
}

// Finished checks if the simulator is done.
func (s Simulator) Finished() bool {
	return s.base.Finished() && len(s.currBatch) == 0 && !s.pendingOutOfOrderItems()
}

// Next populates the serialize.Point with the next entry from the batch.
// If the current pregenerated batch is empty, it tries to generate a new one
// in order to populate the next entry.
func (s *Simulator) Next(p *serialize.Point) bool {
	if s.batchSize == 0 {
		return s.base.Next(p)
	}

	if len(s.currBatch) > 0 || s.simulateNextBatch() {
		p.Copy(s.currBatch[0])
		s.currBatch = s.currBatch[1:]
		return true
	}

	return false
}

// pendingOutOfOrderItems returns whether the simulator has pending
// items (batches or separate entries) that need to be inserted.
func (s *Simulator) pendingOutOfOrderItems() bool {
	return len(s.outOfOrderBatches) > 0 || len(s.outOfOrderEntries) > 0
}

// batchPending creates a batch from the pending items which are stored in
// the Simulator when generating previous batches. These pending items consist
// of out of ourder batches and entries.
func (s *Simulator) batchPending() []*serialize.Point {
	var batch []*serialize.Point
	if len(s.outOfOrderBatches) > 0 {
		batch = s.outOfOrderBatches[0]
		s.outOfOrderBatches = s.outOfOrderBatches[1:]
		return batch
	}

	pendingEntries := len(s.outOfOrderEntries)

	if pendingEntries > 0 {
		if pendingEntries > int(s.batchSize) {
			batch = s.outOfOrderEntries[:s.batchSize]
			s.outOfOrderEntries = s.outOfOrderEntries[s.batchSize:]
			return batch
		}

		batch = s.outOfOrderEntries
		s.outOfOrderEntries = s.outOfOrderEntries[:0]
		return batch
	}

	return batch
}

// simulateNextBatch is used to generate a new batch of entries once the current one is depleted.
func (s *Simulator) simulateNextBatch() bool {
	if s.base.Finished() {
		if s.pendingOutOfOrderItems() {
			s.currBatch = s.batchPending()
			return true
		}

		return false
	}

	bc := s.configGenerator(len(s.outOfOrderBatches), len(s.outOfOrderEntries), len(s.Fields()), len(s.TagKeys()))

	if bc.InsertPrevious {
		if len(s.outOfOrderBatches) == 0 {
			panic("trying to insert an out of order batch when there are no out of order batches")
		}
		s.currBatch = s.outOfOrderBatches[0]
		s.outOfOrderBatches = s.outOfOrderBatches[1:]
		return true
	}

	if bc.Missing {
		s.flushBatch()
		return s.simulateNextBatch()
	}

	if bc.OutOfOrder {
		s.generateOutOfOrderBatch(bc)
		return s.simulateNextBatch()
	}

	s.currBatch = s.generateBatch(bc)

	// Edge case where we hit the finish of the base simulator but there are
	// still pending out of order items.
	if len(s.currBatch) == 0 {
		return s.simulateNextBatch()
	}

	return len(s.currBatch) > 0
}

// generateBatch is used to generate a batch from either out of order entries or
// entries from the base Simulator.
func (s *Simulator) generateBatch(bc *batchConfig) []*serialize.Point {
	batch := make([]*serialize.Point, s.batchSize)
	s.offset = 0

	for i := range batch {
		if s.base.Finished() {
			batch = batch[:i]
			break
		}

		entry, valid := s.getNextEntry(i, bc)

		if !valid {
			batch = batch[:i]
			break
		}

		if index, ok := bc.ZeroFields[i]; ok {
			keys := entry.FieldKeys()
			if len(keys) < index {
				panic("trying to zero a field value with a non-existant index")
			}
			entry.ClearFieldValue(keys[index])
		}

		if index, ok := bc.ZeroTags[i]; ok {
			keys := entry.TagKeys()
			if len(keys) < index {
				panic("trying to zero a tag value with a non-existant index")
			}
			entry.ClearTagValue(keys[index])
		}

		batch[i] = entry
	}

	return batch
}

// getNextEntry returns the next entry which, depending on the batch configuration,
// can be a previous out of order entry or the next entry from the base
// common.Simulator. It also deals with missing or out of order entries. Its
// setup so that it can declare an entry missing or out-of-order no matter if
// its a previous out-of-order entry or a new one.
func (s *Simulator) getNextEntry(index int, bc *batchConfig) (*serialize.Point, bool) {
	var result, entry *serialize.Point
	valid := true

	for result == nil {
		if bc.InsertPreviousEntry[index+s.offset] {
			if len(s.outOfOrderEntries) == 0 {
				panic("trying to insert an out of order entry when there are no out of order entries")
			}
			entry = s.outOfOrderEntries[0]
			s.outOfOrderEntries = s.outOfOrderEntries[1:]
		} else {
			entry = serialize.NewPoint()

			if valid = s.base.Next(entry); !valid {
				break
			}
		}

		if bc.MissingEntries[index+s.offset] {
			s.offset++
			continue
		}

		if bc.OutOfOrderEntries[index+s.offset] {
			s.outOfOrderEntries = append(s.outOfOrderEntries, entry)
			s.offset++
			continue
		}

		result = entry
	}

	return result, valid
}

// generateOutOfOrderBatch creates a batch and sends it straight to out-of-order batches.
func (s *Simulator) generateOutOfOrderBatch(bc *batchConfig) {
	batch := s.generateBatch(bc)

	if len(batch) > 0 {
		s.outOfOrderBatches = append(s.outOfOrderBatches, batch)
	}
}

// flushBatch discards the generated batch.
func (s *Simulator) flushBatch() {
	p := serialize.NewPoint()
	for i := 0; i < int(s.batchSize); i++ {
		valid := s.base.Next(p)
		if !valid {
			break
		}
	}
}
