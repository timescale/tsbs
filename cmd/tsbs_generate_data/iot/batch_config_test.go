package iot

import (
	"math/rand"
	"testing"

	"github.com/google/go-cmp/cmp"
)

var (
	numberOfRuns    = 5
	numberOfBatches = 150
)

func TestNewBatchConfig(t *testing.T) {

	batchRuns := make([][]*batchConfig, numberOfRuns)

	for i := 0; i < numberOfRuns; i++ {
		rand.Seed(123)
		batchRuns[i] = make([]*batchConfig, numberOfBatches)

		for j := 0; j < numberOfBatches; j++ {
			batchRuns[i][j] = newBatchConfig(j, j, j+5, j+5)
		}
	}

	var firstBatchRun []*batchConfig

	for i := range batchRuns {
		if firstBatchRun == nil {
			firstBatchRun = batchRuns[i]
			continue
		}

		for j := range batchRuns[i] {
			if !cmp.Equal(firstBatchRun[j], batchRuns[i][j]) {
				t.Errorf("batch configs don't match for index %d:\ngot\n%+v\nwant\n%+v", j, batchRuns[i][j], firstBatchRun[j])
			}
		}

	}

}
