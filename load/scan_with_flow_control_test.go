package load

import (
	"bufio"
	"bytes"
	"io"
	"testing"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
)

type testBatch struct {
	id  int
	len uint
}

func (b *testBatch) Len() uint { return b.len }

func (b *testBatch) Append(p data.LoadedPoint) {
	b.len++
	b.id = int(p.Data.(byte))
}

func TestAckAndMaybeSend(t *testing.T) {
	cases := []struct {
		desc         string
		unsent       []targets.Batch
		count        int
		afterCount   int
		afterLen     int
		afterFirstID int
	}{
		{
			desc:       "unsent is nil",
			unsent:     nil,
			count:      0,
			afterCount: -1,
			afterLen:   0,
		},
		{
			desc:       "unsent has 0 elements",
			unsent:     []targets.Batch{},
			count:      0,
			afterCount: -1,
			afterLen:   0,
		},
		{
			desc:       "unsent has 1 element",
			unsent:     []targets.Batch{&testBatch{1, 1}},
			count:      1,
			afterCount: 0,
			afterLen:   0,
		},
		{
			desc:         "unsent has 2 elements",
			unsent:       []targets.Batch{&testBatch{1, 1}, &testBatch{2, 1}},
			count:        2,
			afterCount:   1,
			afterLen:     1,
			afterFirstID: 2,
		},
	}
	ch := newDuplexChannel(100)
	for _, c := range cases {
		c.unsent = ackAndMaybeSend(ch, &c.count, c.unsent)
		if c.afterCount != c.count {
			t.Errorf("%s: count incorrect: want %d got %d", c.desc, c.afterCount, c.count)
		}
		if c.afterLen != len(c.unsent) {
			t.Errorf("%s: len incorrect: want %d got %d", c.desc, c.afterLen, len(c.unsent))
		}
		if len(c.unsent) > 0 {
			if got := c.unsent[0].(*testBatch); c.afterFirstID != got.id {
				t.Errorf("%s: first element incorrect: want %d got %d", c.desc, c.afterFirstID, got.id)
			}
		}
	}
}

func TestSendOrQueueBatch(t *testing.T) {
	cases := []struct {
		desc           string
		unsent         []targets.Batch
		toSend         []targets.Batch
		queueSize      int
		count          int
		afterCount     int
		afterUnsentLen int
		afterChanLen   int
	}{
		{
			desc:           "unsent is empty, queue does not fill up",
			unsent:         []targets.Batch{},
			toSend:         []targets.Batch{&testBatch{1, 1}},
			queueSize:      1,
			count:          0,
			afterCount:     1,
			afterUnsentLen: 0,
			afterChanLen:   1,
		},
		{
			desc:           "unsent is empty, queue fills up",
			unsent:         []targets.Batch{},
			toSend:         []targets.Batch{&testBatch{1, 1}, &testBatch{2, 1}},
			queueSize:      1,
			count:          0,
			afterCount:     2,
			afterUnsentLen: 1,
			afterChanLen:   1,
		},
		{
			desc:           "unsent is non-empty, queue fills up",
			unsent:         []targets.Batch{&testBatch{1, 1}},
			toSend:         []targets.Batch{&testBatch{2, 1}, &testBatch{3, 1}},
			queueSize:      2,
			count:          1,
			afterCount:     3,
			afterUnsentLen: 3,
			afterChanLen:   0,
		},
	}
	for _, c := range cases {
		ch := newDuplexChannel(c.queueSize)
		for _, b := range c.toSend {
			c.unsent = sendOrQueueBatch(ch, &c.count, b, c.unsent)
		}
		if c.afterCount != c.count {
			t.Errorf("%s: count incorrect: want %d got %d", c.desc, c.afterCount, c.count)
		}
		if c.afterUnsentLen != len(c.unsent) {
			t.Errorf("%s: unsent len incorrect: want %d got %d", c.desc, c.afterUnsentLen, len(c.unsent))
		}
		if c.afterChanLen != len(ch.toWorker) {
			t.Errorf("%s: unsent chan incorrect: want %d got %d", c.desc, c.afterChanLen, len(ch.toWorker))
		}
	}
}

func TestNewPoint(t *testing.T) {
	// simple equality types
	temp := []interface{}{64, 5.5, true, uint(5), "test string"}
	for _, x := range temp {
		p := data.NewLoadedPoint(x)
		if p.Data != x {
			t.Errorf("NewPoint did not have right data: got %v want %d", p.Data, x)
		}
	}

	// with a byte arr
	byteArr := []byte("test")
	p := data.NewLoadedPoint(byteArr)
	if !bytes.Equal(p.Data.([]byte), byteArr) {
		t.Errorf("NewPoint did not have right byte arr: got %v want %v", p.Data, byteArr)
	}

	// with a struct
	batch := &testBatch{id: 101, len: 500}
	p = data.NewLoadedPoint(batch)
	if got := p.Data.(*testBatch); got.id != 101 || got.len != 500 {
		t.Errorf("NewPoint did not have right struct: got %v want %v", got, batch)
	}
}

type testDataSource struct {
	br     *bufio.Reader
	called uint64
}

func (d *testDataSource) NextItem() data.LoadedPoint {
	ret := data.LoadedPoint{}
	b, err := d.br.ReadByte()
	if err != nil {
		if err == io.EOF {
			return data.LoadedPoint{}
		}
		panic(err)
	}
	ret.Data = b
	d.called++

	return ret
}

func (d *testDataSource) Headers() *common.GeneratedDataHeaders {
	panic("implement me")
}

type testFactory struct{}

func (f *testFactory) New() targets.Batch {
	return &testBatch{}
}

func _checkScan(t *testing.T, desc string, called, read, want uint64) {
	if called != want {
		t.Errorf("%s: data source not called enough: got %d want %d", desc, called, want)
	}
	if read != want {
		t.Errorf("%s: read incorrect: got %d want %d", desc, read, want)
	}
}

func _boringWorker(c *duplexChannel) {
	for range c.toWorker {
		c.sendToScanner()
	}
}

func TestScanWithIndexer(t *testing.T) {
	testData := []byte{0x00, 0x01, 0x02}

	cases := []struct {
		desc        string
		batchSize   uint
		limit       uint64
		wantCalls   uint64
		shouldPanic bool
	}{
		{
			desc:      "scan w/ zero limit",
			batchSize: 1,
			limit:     0,
			wantCalls: uint64(len(testData)),
		},
		{
			desc:      "scan w/ one limit",
			batchSize: 1,
			limit:     1,
			wantCalls: 1,
		},
		{
			desc:      "scan w/ over limit",
			batchSize: 1,
			limit:     4,
			wantCalls: uint64(len(testData)),
		},

		{
			desc:      "scan w/ leftover batches",
			batchSize: 2,
			limit:     4,
			wantCalls: uint64(len(testData)),
		},
		{
			desc:        "batchSize = 0 is panic",
			batchSize:   0,
			limit:       0,
			shouldPanic: true,
		},
	}
	for _, c := range cases {
		br := bufio.NewReader(bytes.NewReader(testData))
		channels := []*duplexChannel{newDuplexChannel(1)}
		testDataSource := &testDataSource{called: 0, br: br}
		indexer := &targets.ConstantIndexer{}
		if c.shouldPanic {
			func() {
				defer func() {
					if re := recover(); re == nil {
						t.Errorf("%s: did not panic when should", c.desc)
					}
				}()
				scanWithFlowControl(channels, c.batchSize, c.limit, testDataSource, &testFactory{}, indexer)
			}()
			continue
		} else {
			go _boringWorker(channels[0])
			read := scanWithFlowControl(channels, c.batchSize, c.limit, testDataSource, &testFactory{}, indexer)
			_checkScan(t, c.desc, testDataSource.called, read, c.wantCalls)
		}
	}
}
