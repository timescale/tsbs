package load

import (
	"bytes"
	"fmt"
	"github.com/timescale/tsbs/pkg/targets"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testProcessor struct {
	worker int
	closed bool
}

func (p *testProcessor) Init(workerNum int, _, _ bool) {
	p.worker = workerNum
}

func (p *testProcessor) ProcessBatch(targets.Batch, bool) (metricCount, rowCount uint64) {
	return 1, 0
}

func (p *testProcessor) Close(_ bool) {
	p.closed = true
}

type testCreator struct {
	exists    bool
	errRemove bool
	errCreate bool

	initCalled   bool
	createCalled bool
	removeCalled bool
	postCalled   bool
	closedCalled bool
}

func (c *testCreator) Init() {
	c.initCalled = true
}
func (c *testCreator) DBExists(string) bool {
	return c.exists
}
func (c *testCreator) CreateDB(string) error {
	c.createCalled = true
	if c.errCreate {
		return fmt.Errorf("create error")
	}
	return nil
}
func (c *testCreator) RemoveOldDB(string) error {
	c.removeCalled = true
	if c.errRemove {
		return fmt.Errorf("remove error")
	}
	return nil
}

type testCreatorPost struct {
	testCreator
}

func (c *testCreatorPost) PostCreateDB(string) error {
	c.postCalled = true
	return nil
}

type testCreatorClose struct {
	testCreator
}

func (c *testCreatorClose) Close() {
	c.closedCalled = true
}

type testBenchmark struct {
	processors []*testProcessor
	offset     int64
}

func (b *testBenchmark) GetDataSource() targets.DataSource {
	return nil
}

func (b *testBenchmark) GetBatchFactory() targets.BatchFactory { return nil }
func (b *testBenchmark) GetPointIndexer(uint) targets.PointIndexer {
	return &targets.ConstantIndexer{}
}
func (b *testBenchmark) GetProcessor() targets.Processor {
	idx := atomic.AddInt64(&b.offset, 1)
	idx--
	return b.processors[idx]
}
func (b *testBenchmark) GetDBCreator() targets.DBCreator {
	return nil
}

type testSleepRegulator struct {
	calledTimes int
	lock        sync.Mutex
}

func (sr *testSleepRegulator) Sleep(int, time.Time) {
	sr.lock.Lock()
	sr.calledTimes++
	sr.lock.Unlock()
}

func TestUseDBCreator(t *testing.T) {
	cases := []struct {
		desc         string
		doLoad       bool
		exists       bool
		abortOnExist bool
		doCreate     bool
		doPost       bool
		doClose      bool

		shouldPanic bool
		errRemove   bool
		errCreate   bool
	}{
		{
			desc:   "doLoad is false",
			doLoad: false,
		},
		{
			desc:         "doLoad is true, nothing else",
			doLoad:       true,
			exists:       false,
			abortOnExist: false,
			doCreate:     false,
		},
		{
			desc:     "doLoad, doCreate = true",
			doLoad:   true,
			doCreate: true,
		},
		{
			desc:     "doLoad, doCreate, exists = true",
			doLoad:   true,
			doCreate: true,
			exists:   true,
		},
		{
			desc:     "post create = true",
			doLoad:   true,
			exists:   false,
			doCreate: true,
			doPost:   true,
		},
		{
			desc:    "close = true",
			doLoad:  true,
			exists:  false,
			doClose: true,
		},
		{
			desc:         "exists, doAbortOnExist = true, should panic",
			doLoad:       true,
			exists:       true,
			abortOnExist: true,
			shouldPanic:  true,
		},
		{
			desc:        "removeDB errs, should panic",
			doLoad:      true,
			doCreate:    true,
			exists:      true,
			errRemove:   true,
			shouldPanic: true,
		},

		{
			desc:        "createDB errs, should panic",
			doLoad:      true,
			doCreate:    true,
			exists:      true,
			errCreate:   true,
			shouldPanic: true,
		},
	}
	testPanic := func(r *CommonBenchmarkRunner, dbc targets.DBCreator, desc string) {
		defer func() {
			if re := recover(); re == nil {
				t.Errorf("%s: did not panic when should", desc)
			}
		}()
		_ = r.useDBCreator(dbc)
	}
	for _, c := range cases {
		r := &CommonBenchmarkRunner{
			BenchmarkRunnerConfig: BenchmarkRunnerConfig{
				DoLoad:         c.doLoad,
				DoCreateDB:     c.doCreate,
				DoAbortOnExist: c.abortOnExist,
			},
		}
		core := testCreator{
			exists:    c.exists,
			errCreate: c.errCreate,
			errRemove: c.errRemove,
		}

		// Decide whether to decorate the core DBCreator
		var dbc targets.DBCreator
		if c.doPost {
			dbc = &testCreatorPost{core}
		} else if c.doClose {
			dbc = &testCreatorClose{core}
		} else {
			dbc = &core
		}

		if c.shouldPanic {
			testPanic(r, dbc, c.desc)
			continue
		}

		deferFn := r.useDBCreator(dbc)
		deferFn()

		// Recover the core if decorated
		if c.doPost {
			core = dbc.(*testCreatorPost).testCreator
		} else if c.doClose {
			core = dbc.(*testCreatorClose).testCreator
		}
		if c.doLoad {
			if !core.initCalled {
				t.Errorf("%s: doLoad is true but Init not called", c.desc)
			}
			if c.doCreate {
				if !core.createCalled {
					t.Errorf("%s: doCreate is true but CreateDB not called", c.desc)
				}
				if c.exists {
					if !core.removeCalled {
						t.Errorf("%s: exists is true but RemoveDB not called", c.desc)
					}
				} else if core.removeCalled {
					t.Errorf("%s: exists is false but RemoveDB was called", c.desc)
				}
			} else if core.createCalled {
				t.Errorf("%s: doCreate is false but CreateDB was called", c.desc)
			}
			if c.doPost && !core.postCalled {
				t.Errorf("%s: doPost is true but PostCreateDB not called", c.desc)
			} else if !c.doPost && core.postCalled {
				t.Errorf("%s: doPost is false but PostCreateDB was called", c.desc)
			}
		} else if !core.initCalled {
			t.Errorf("%s: doLoad is false but Init not called", c.desc)
		}

		// Test closing function is set or not set
		if c.doClose != core.closedCalled {
			t.Errorf("%s: close condition not equal: got %v want %v", c.desc, core.closedCalled, c.doClose)
		}
	}
}

func TestCreateChannelsAndPartitions(t *testing.T) {
	cases := []struct {
		desc        string
		wantChanLen int
		wantChanCap int
	}{
		{
			desc:        "single queue",
			wantChanLen: 1,
			wantChanCap: 1,
		},
		{
			desc:        "worker per queue",
			wantChanLen: 2,
			wantChanCap: 1,
		}, {
			desc:        "worker per queue, larger cap",
			wantChanLen: 2,
			wantChanCap: 5,
		},
	}
	for _, c := range cases {
		br := &CommonBenchmarkRunner{}
		t.Run(c.desc, func(t *testing.T) {
			channels := br.createChannels(uint(c.wantChanLen), uint(c.wantChanCap))
			if got := len(channels); got != c.wantChanLen {
				t.Errorf("incorrect number of channels: got %d want %d", got, c.wantChanLen)
			}
			for _, ch := range channels {
				if got := cap(ch.toWorker); got != c.wantChanCap {
					t.Errorf("incorrect toWorker channel cap: got %d want %d", got, c.wantChanCap)
				}
				if got := cap(ch.toScanner); got != c.wantChanCap {
					t.Errorf("incorrect toScanner channel cap: got %d want %d", got, c.wantChanCap)
				}
			}
		})
	}
}

func TestWork(t *testing.T) {
	br := &CommonBenchmarkRunner{}
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

func TestWorkWithSleep(t *testing.T) {
	br := &CommonBenchmarkRunner{
		sleepRegulator: &testSleepRegulator{lock: sync.Mutex{}},
	}
	b := &testBenchmark{}
	b.processors = append(b.processors, &testProcessor{})
	var wg sync.WaitGroup
	wg.Add(1)
	c := newDuplexChannel(1)
	c.sendToWorker(&testBatch{})
	go br.work(b, &wg, c, 0)
	<-c.toScanner
	c.close()
	wg.Wait()

	if got := b.processors[0].worker; got != 0 {
		t.Errorf("processor 0 has wrong worker id: got %d want %d", got, 0)
	}

	if got := br.metricCnt; got != 1 {
		t.Errorf("invalid metric count: got %d want %d", got, 1)
	}

	if !b.processors[0].closed {
		t.Errorf("processor 0 not closed")
	}

	numTimesSleepRegulatorCalled := br.sleepRegulator.(*testSleepRegulator).calledTimes
	if numTimesSleepRegulatorCalled != 1 {
		t.Errorf("sleep regulator called %d times instead of 1", numTimesSleepRegulatorCalled)
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
		br := &CommonBenchmarkRunner{}
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
	counter := int64(0)
	var m sync.Mutex
	printFn = func(s string, args ...interface{}) (n int, err error) {
		atomic.AddInt64(&counter, 1)
		m.Lock()
		defer m.Unlock()
		return fmt.Fprintf(&b, s, args...)
	}
	br := &CommonBenchmarkRunner{}
	duration := 200 * time.Millisecond
	go br.report(duration)

	time.Sleep(25 * time.Millisecond)
	if got := atomic.LoadInt64(&counter); got != 1 {
		t.Errorf("TestReport: header count check incorrect: got %d want %d", got, 1)
	}

	time.Sleep(duration)
	if got := atomic.LoadInt64(&counter); got != 2 {
		t.Errorf("TestReport: counter check incorrect (1): got %d want %d", got, 2)
	}

	time.Sleep(duration)
	if got := atomic.LoadInt64(&counter); got != 3 {
		t.Errorf("TestReport: counter check incorrect (2): got %d want %d", got, 3)
	}
	m.Lock()
	end := strings.TrimSpace(string(b.Bytes()))
	m.Unlock()
	if end[len(end)-1:len(end)] != "-" {
		t.Errorf("TestReport: non-row report does not end in -")
	}

	// update row count so line is different
	atomic.StoreUint64(&br.rowCnt, 1)
	time.Sleep(duration)
	if got := atomic.LoadInt64(&counter); got != 4 {
		t.Errorf("TestReport: counter check incorrect (1): got %d want %d", got, 4)
	}
	m.Lock()
	end = strings.TrimSpace(string(b.Bytes()))
	m.Unlock()
	if end[len(end)-1:len(end)] == "-" {
		t.Errorf("TestReport: row report ends in -")
	}
}
