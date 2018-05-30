package load

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type testProcessor struct {
	worker int
	closed bool
}

func (p *testProcessor) Init(workerNum int, _ bool) {
	p.worker = workerNum
}

func (p *testProcessor) ProcessBatch(b Batch, doLoad bool) (metricCount, rowCount uint64) {
	return 1, 0
}

func (p *testProcessor) Close(_ bool) {
	p.closed = true
}

type testBenchmark struct {
	processors []*testProcessor
	offset     int
}

func (b *testBenchmark) GetPointDecoder(_ *bufio.Reader) PointDecoder { return nil }

func (b *testBenchmark) GetBatchFactory() BatchFactory { return nil }

func (b *testBenchmark) GetPointIndexer(maxPartitions uint) PointIndexer { return &ConstantIndexer{} }

func (b *testBenchmark) GetProcessor() Processor {
	idx := b.offset
	b.offset++
	return b.processors[idx]
}

func TestCreateChannelsAndPartitions(t *testing.T) {
	cases := []struct {
		desc           string
		queues         uint
		workers        uint
		wantPartitions uint
		wantChanLen    int
		shouldPanic    bool
	}{
		{
			desc:           "single queue",
			queues:         SingleQueue,
			workers:        2,
			wantPartitions: 1,
			wantChanLen:    2,
		},
		{
			desc:           "worker per queue",
			queues:         WorkerPerQueue,
			workers:        2,
			wantPartitions: 2,
			wantChanLen:    1,
		},
		{
			desc:           "workers divide evenly into queues",
			queues:         3,
			workers:        6,
			wantPartitions: 3,
			wantChanLen:    2,
		},
		{
			desc:           "workers do not divide evenly into queues",
			queues:         3,
			workers:        7,
			wantPartitions: 3,
			wantChanLen:    3,
		},
		{
			desc:           "too many queues for workers, panic",
			queues:         3,
			workers:        2,
			wantPartitions: 0,
			wantChanLen:    0,
			shouldPanic:    true,
		},
	}
	testPanic := func(br *BenchmarkRunner, queues uint, desc string) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("%s: did not panic when should", desc)
			}
		}()
		_ = br.createChannels(queues)
	}
	for _, c := range cases {
		br := &BenchmarkRunner{}
		br.workers = c.workers
		if c.shouldPanic {
			testPanic(br, c.queues, c.desc)
		} else {
			channels := br.createChannels(c.queues)
			if got := uint(len(channels)); got != c.wantPartitions {
				t.Errorf("%s: incorrect number of partitions: got %d want %d", c.desc, got, c.wantPartitions)
			}
			if got := cap(channels[0].toWorker); got != c.wantChanLen {
				t.Errorf("%s: incorrect channel length: got %d want %d", c.desc, got, c.wantChanLen)
			}
		}
	}
}

func TestWork(t *testing.T) {
	br := loader
	b := &testBenchmark{}
	for i := 0; i < 2; i++ {
		b.processors = append(b.processors, &testProcessor{})
	}
	var wg sync.WaitGroup
	wg.Add(2)
	c := newDuplexChannel(2)
	c.sendToWorker(&testBatch{})
	c.sendToWorker(&testBatch{})
	go br.work(b, &wg, c, 0)
	time.Sleep(100 * time.Millisecond)
	go br.work(b, &wg, c, 1)
	<-c.toScanner
	<-c.toScanner
	c.close()
	wg.Wait()

	if got := b.processors[0].worker; got != 0 {
		t.Errorf("TestWork: processor 0 has wrong worker id: got %d want %d", got, 0)
	}

	if got := b.processors[1].worker; got != 1 {
		t.Errorf("TestWork: processor 0 has wrong worker id: got %d want %d", got, 1)
	}

	if got := br.metricCnt; got != 2 {
		t.Errorf("TestWork: invalid metric count: got %d want %d", got, 2)
	}

	if !b.processors[0].closed {
		t.Errorf("TestWork: processor 0 not closed")
	}

	if !b.processors[1].closed {
		t.Errorf("TestWork: processor 1 not closed")
	}
}

func TestSummary(t *testing.T) {
	cases := []struct {
		desc    string
		metrics uint64
		rows    uint64
		took    time.Duration
		want    string
	}{
		{
			desc:    "10 metrics, 0 rows, 1 second",
			metrics: 10,
			rows:    0,
			took:    time.Second,
			want:    "\nSummary:\nloaded 10 metrics in 1.000sec with 0 workers (mean rate 10.00 metrics/sec)\n",
		},
		{
			desc:    "fractional rate: 10 metrics, 0 rows, 1 second",
			metrics: 15,
			rows:    0,
			took:    10 * time.Second,
			want:    "\nSummary:\nloaded 15 metrics in 10.000sec with 0 workers (mean rate 1.50 metrics/sec)\n",
		},
		{
			desc:    "fraction time: 10 metrics, 0 rows, .5 second",
			metrics: 10,
			rows:    0,
			took:    500 * time.Millisecond,
			want:    "\nSummary:\nloaded 10 metrics in 0.500sec with 0 workers (mean rate 20.00 metrics/sec)\n",
		},
		{
			desc:    "include rows: 10 metrics, 1 rows, 1 second",
			metrics: 10,
			rows:    1,
			took:    time.Second,
			want:    "\nSummary:\nloaded 10 metrics in 1.000sec with 0 workers (mean rate 10.00 metrics/sec)\nloaded 1 rows in 1.000sec with 0 workers (mean rate 1.00 rows/sec)\n",
		},
	}

	for _, c := range cases {
		br := &BenchmarkRunner{}
		br.metricCnt = c.metrics
		br.rowCnt = c.rows
		var b bytes.Buffer
		printFn = func(s string, args ...interface{}) (n int, err error) {
			return fmt.Fprintf(&b, s, args...)
		}
		br.summary(c.took)
		if got := string(b.Bytes()); got != c.want {
			t.Errorf("%s: incorrect summary\ngot %s\nwant %s", c.desc, got, c.want)
		}
	}
}

func TestReport(t *testing.T) {
	var b bytes.Buffer
	counter := 0
	printFn = func(s string, args ...interface{}) (n int, err error) {
		counter++
		return fmt.Fprintf(&b, s, args...)
	}
	br := &BenchmarkRunner{}
	duration := 200 * time.Millisecond
	go br.report(duration)

	time.Sleep(25 * time.Millisecond)
	if got := counter; counter != 1 {
		t.Errorf("TestReport: header count check incorrect: got %d want %d", got, 1)
	}

	time.Sleep(duration)
	if got := counter; counter != 2 {
		t.Errorf("TestReport: counter check incorrect (1): got %d want %d", got, 2)
	}

	time.Sleep(duration)
	if got := counter; counter != 3 {
		t.Errorf("TestReport: counter check incorrect (2): got %d want %d", got, 3)
	}
	end := strings.TrimSpace(string(b.Bytes()))
	if end[len(end)-1:len(end)] != "-" {
		t.Errorf("TestReport: non-row report does not end in -")
	}

	// update row count so line is different
	br.rowCnt = 1
	time.Sleep(duration)
	if got := counter; counter != 4 {
		t.Errorf("TestReport: counter check incorrect (1): got %d want %d", got, 4)
	}
	end = strings.TrimSpace(string(b.Bytes()))
	if end[len(end)-1:len(end)] == "-" {
		t.Errorf("TestReport: row report ends in -")
	}
}
