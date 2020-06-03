package load

import (
	"bufio"
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/targets"
)

func TestScanWithoutFlowControl(t *testing.T) {
	testData := []byte{0x00, 0x01, 0x02}

	cases := []struct {
		desc             string
		batchSize        uint
		numChannels      uint
		limit            uint64
		wantCalls        uint64
		wantChannelCalls []int
		shouldPanic      bool
	}{
		{
			desc:             "scan w/ zero limit",
			numChannels:      1,
			batchSize:        1,
			limit:            0,
			wantChannelCalls: []int{len(testData)},
			wantCalls:        uint64(len(testData)),
		}, {
			desc:             "scan w/ one limit",
			batchSize:        1,
			numChannels:      1,
			wantChannelCalls: []int{1},
			limit:            1,
			wantCalls:        1,
		}, {
			desc:             "scan w/ over limit",
			batchSize:        1,
			numChannels:      1,
			limit:            4,
			wantChannelCalls: []int{len(testData)},
			wantCalls:        uint64(len(testData)),
		}, {
			desc:             "scan w/ leftover batches",
			batchSize:        2,
			numChannels:      1,
			limit:            4,
			wantChannelCalls: []int{2},
			wantCalls:        uint64(len(testData)),
		}, {
			desc:             "scan, no limit, multiple channels",
			batchSize:        1,
			numChannels:      2,
			wantChannelCalls: []int{2, 1},
			wantCalls:        uint64(len(testData)),
		}, {
			desc:             "scan, no limit, multiple channels, leftover",
			batchSize:        2,
			numChannels:      2,
			wantChannelCalls: []int{1, 1},
			wantCalls:        uint64(len(testData)),
		}, {
			desc:        "batchSize = 0 is panic",
			batchSize:   0,
			limit:       0,
			shouldPanic: true,
		},
	}
	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			channelCalls := make([]int, c.numChannels)
			br := bufio.NewReader(bytes.NewReader(testData))
			channels := make([]chan targets.Batch, c.numChannels)
			for i := uint(0); i < c.numChannels; i++ {
				channels[i] = make(chan targets.Batch, 1)
			}
			testDataSource := &testDataSource{called: 0, br: br}
			indexer := &modIndexer{mod: c.numChannels}
			if c.shouldPanic {
				func() {
					defer func() {
						if re := recover(); re == nil {
							t.Errorf("%s: did not panic when should", c.desc)
						}
					}()
					scanWithoutFlowControl(testDataSource, indexer, &testFactory{}, channels, c.batchSize, c.limit)
				}()
				return
			} else {
				wg := &sync.WaitGroup{}
				wg.Add(int(c.numChannels))
				for i := uint(0); i < c.numChannels; i++ {
					go _boringWorkerSingleChannel(channels[i], &channelCalls[i], wg)
				}
				read := scanWithoutFlowControl(testDataSource, indexer, &testFactory{}, channels, c.batchSize, c.limit)
				for i := uint(0); i < c.numChannels; i++ {
					close(channels[i])
				}
				wg.Wait()
				_checkScan(t, c.desc, testDataSource.called, read, c.wantCalls)
				for i := uint(0); i < c.numChannels; i++ {
					if c.wantChannelCalls[i] != channelCalls[i] {
						t.Errorf("unexpected channel calls, want %d, got %d", c.wantChannelCalls[i], channelCalls[i])
					}
				}
			}
		})
	}
}

func _boringWorkerSingleChannel(c chan targets.Batch, numRead *int, wg *sync.WaitGroup) {
	for range c {
		*numRead = *numRead + 1
		time.Sleep(time.Millisecond * 100)
	}
	wg.Done()
}

type modIndexer struct {
	cnt uint
	mod uint
}

func (m *modIndexer) GetIndex(data.LoadedPoint) uint {
	tmp := m.cnt
	m.cnt++
	return tmp % m.mod
}
