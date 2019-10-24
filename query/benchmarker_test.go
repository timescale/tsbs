package query

import (
	"golang.org/x/time/rate"
	"io/ioutil"
	"math"
	"os"
	"strings"
	"sync"
	"testing"
)

type testProcessor struct {
	count int
	wNum  int
}

func (p *testProcessor) Init(workerNum int) {
	p.wNum = workerNum
	p.count = 0
}

func (p *testProcessor) ProcessQuery(_ Query, _ bool) ([]*Stat, error) {
	p.count++
	return nil, nil
}

func TestProcessorHandler(t *testing.T) {
	qLimit := 17
	p1Num := 0
	p2Num := 5

	p1 := &testProcessor{}
	p2 := &testProcessor{}
	b := NewBenchmarkRunner(BenchmarkRunnerConfig{})
	b.ch = make(chan Query, 2)

	var wg sync.WaitGroup
	qPool := &testQueryPool
	wg.Add(2)
	var requestRate = rate.Limit(math.MaxFloat64)
	var requestBurst = 0
	var rateLimiter *rate.Limiter = rate.NewLimiter(requestRate, requestBurst)

	go b.processorHandler(&wg, rateLimiter, qPool, p1, 0)
	go b.processorHandler(&wg, rateLimiter, qPool, p2, 5)
	for i := 0; i < qLimit; i++ {
		q := qPool.Get().(*testQuery)
		b.ch <- q
	}
	close(b.ch)
	wg.Wait()

	if p1.wNum != p1Num {
		t.Errorf("p1 Init() not called: want %d got %d", p1Num, p1.wNum)
	}
	if p2.wNum != p2Num {
		t.Errorf("p2 Init() not called: want %d got %d", p2Num, p2.wNum)
	}
	if p1.count+p2.count != qLimit {
		t.Errorf("total queries wrong: want %d got %d", qLimit, p1.count+p2.count)
	}
}

func TestProcessorHandlerPreWarm(t *testing.T) {
	qLimit := 17
	p1Num := 0
	p2Num := 5

	p1 := &testProcessor{}
	p2 := &testProcessor{}
	b := &BenchmarkRunner{}
	b.scanner = newScanner(&b.Limit)
	spArgs := &statProcessorArgs{
		limit:          &b.Limit,
		prewarmQueries: true,
	}
	var requestRate = rate.Limit(math.MaxFloat64)
	var requestBurst = 0
	var rateLimiter *rate.Limiter = rate.NewLimiter(requestRate, requestBurst)

	b.sp = newStatProcessor(spArgs, "")
	b.ch = make(chan Query, 2)
	var wg sync.WaitGroup
	qPool := &testQueryPool
	wg.Add(2)
	go b.processorHandler(&wg, rateLimiter, qPool, p1, 0)
	go b.processorHandler(&wg, rateLimiter, qPool, p2, 5)
	for i := 0; i < qLimit; i++ {
		q := qPool.Get().(*testQuery)
		b.ch <- q
	}
	close(b.ch)
	wg.Wait()

	if p1.wNum != p1Num {
		t.Errorf("p1 Init() not called: want %d got %d", p1Num, p1.wNum)
	}
	if p2.wNum != p2Num {
		t.Errorf("p2 Init() not called: want %d got %d", p2Num, p2.wNum)
	}
	if p1.count+p2.count != 2*qLimit {
		t.Errorf("total queries wrong: want %d got %d", 2*qLimit, p1.count+p2.count)
	}
}
func TestBenchmarkRunnerGetBufferedReaderPanicOnMissingFile(t *testing.T) {
	dumbFileName := "some-random-file-that-should-not-exist"
	_, err := os.Stat(dumbFileName)
	if os.IsExist(err) {
		t.Fatalf("file '%s' should not exist", dumbFileName)
	}
	b := &BenchmarkRunner{
		BenchmarkRunnerConfig: BenchmarkRunnerConfig{
			FileName: dumbFileName,
		},
	}
	defer func() {
		if r := recover(); !strings.HasPrefix(r.(string), "cannot open file for read") {
			t.Error("wrong panic")
		}
	}()
	b.GetBufferedReader()
	t.Error("the code did not panic")
}

func TestBenchmarkRunnerGetBufferedReaderCached(t *testing.T) {
	// SETUP
	// create temporary empty file to open
	randomFile, err := ioutil.TempFile("", "temp_file_*")
	if err != nil {
		t.Fatalf("Could not create temp file: %v", err)
	}

	// and a file name for a non-existing file
	dumbFileName := "some-random-file-that-should-not-exist"
	_, err = os.Stat(dumbFileName)
	if os.IsExist(err) {
		t.Fatalf("File '%s' should not exist", dumbFileName)
	}

	b := &BenchmarkRunner{
		BenchmarkRunnerConfig: BenchmarkRunnerConfig{
			FileName: randomFile.Name(),
		},
	}

	// RUN
	// first call to existing file
	// creates a new buffered reader and caches it
	b.GetBufferedReader()

	//change file name
	b.FileName = dumbFileName

	// second call should use cached, not open another BuffReader
	b.GetBufferedReader()
}

func TestBenchmarkRunnerRunPanicOnNoWorkers(t *testing.T) {
	runner := &BenchmarkRunner{}
	defer func() {
		if r := recover(); r != "must have at least one worker" {
			t.Error("wrong panic")
		}
	}()
	runner.Run(nil, nil)
	t.Errorf("the code did not panic")
}

func TestBenchmarkRunnerGettersAndSetters(t *testing.T) {
	b := &BenchmarkRunner{}
	b.SetLimit(1)
	if b.Limit != 1 {
		t.Errorf("Expected %d, got %d", 1, b.Limit)
	}

	b.PrintResponses = true
	if !b.DoPrintResponses() {
		t.Error("Expected true, got false")
	}
	b.Debug = 12
	if b.DebugLevel() != 12 {
		t.Errorf("Expected 12, got %d", b.DebugLevel())
	}

	b.DBName = "Some name"
	if b.DatabaseName() != "Some name" {
		t.Errorf("Expected 'Some name', got '%s'", b.DatabaseName())
	}
}
func TestBenchmarkRunnerRunPanicOnBurnInBiggerThanLimit(t *testing.T) {
	limit := uint64(1)
	runner := &BenchmarkRunner{
		BenchmarkRunnerConfig: BenchmarkRunnerConfig{
			Workers: 1,
		},
		sp: &defaultStatProcessor{
			args: &statProcessorArgs{burnIn: limit + 1},
		},
	}
	defer func() {
		if r := recover(); r != "burn-in is larger than limit" {
			t.Error("wrong panic")
		}
	}()
	runner.Run(nil, nil)
	t.Errorf("the code did not panic")
}

func TestBenchmarkRunnerRunNoQueries(t *testing.T) {
	// SETUP
	// ..empty query file
	fakeQueriesFile, err := ioutil.TempFile("", "fake_queries*")
	if err != nil {
		t.Fatal(err)
	}

	profFile, err := ioutil.TempFile("", "temp_file_*")
	if err != nil {
		t.Fatalf("Could not create temp file: %v", err)
	}

	spStarted := false
	sendStatsCalled := false
	// lock controlls access to spStarted and sendStatsCalled
	// wg gets Done when sp is closed
	wg := &sync.WaitGroup{}
	lock := &sync.Mutex{}
	sp := mockStatProcessor{
		args: &statProcessorArgs{},
		onProcess: func(_ uint) {
			lock.Lock()
			spStarted = true
			lock.Unlock()
		},
		onSend: func(_ []*Stat) {
			lock.Lock()
			sendStatsCalled = true
			lock.Unlock()
		},
		wg: wg,
	}

	/* Query execution workers wait to receive a decoded query
	on a input channel. The scanner reads an input file, decodes
	the queries	and submits them to the input channel. When EOF
	reached is reached, the input channel is closed. Benchmarker
	waits for workers to finish and reports stats. */
	limit := uint64(1)
	b := &BenchmarkRunner{
		BenchmarkRunnerConfig: BenchmarkRunnerConfig{
			Workers:    1,
			Limit:      limit,
			FileName:   fakeQueriesFile.Name(),
			MemProfile: profFile.Name(),
		},
		sp:      &sp,
		scanner: newScanner(&limit),
	}

	processor := &mockProcessor{}
	processorsCreated := uint(0)
	createProcessorFn := func() Processor {
		lock.Lock()
		processorsCreated++
		lock.Unlock()
		return processor
	}
	// no queries => no worker should execute/process a query
	// no errors expected

	// RUN
	wg.Add(1)
	b.Run(&TimescaleDBPool, createProcessorFn)
	wg.Wait()
	lock.Lock()
	// ASSERT
	if !spStarted {
		t.Error("stat processor wasn't started")
	}
	if processorsCreated != b.Workers {
		t.Errorf("expected %d processors to be created, but %d were", b.Workers, processorsCreated)
	}
	if !sp.closed {
		t.Error("stat processor wasn't closed")
	}
	if !processor.initCalled {
		t.Errorf("init of processor wasn't called")
	}
	if sendStatsCalled {
		t.Errorf("send Stats should not have been called. No queries")
	}
}

type mockStatProcessor struct {
	args      *statProcessorArgs
	onSend    func([]*Stat)
	onProcess func(uint)
	closed    bool
	wg        *sync.WaitGroup
}

func (m *mockStatProcessor) getArgs() *statProcessorArgs {
	return m.args
}
func (m *mockStatProcessor) send(stats []*Stat) {
	if m.onSend != nil {
		m.onSend(stats)
	}
}
func (m *mockStatProcessor) sendWarm(stats []*Stat) {
	if m.onSend != nil {
		m.onSend(stats)
	}
}
func (m *mockStatProcessor) process(workers uint, latencyFile string) {
	if m.onProcess != nil {
		m.onProcess(workers)
	}
}
func (m *mockStatProcessor) CloseAndWait() {
	m.closed = true
	m.wg.Done()
}

type mockProcessor struct {
	processRes []*Stat
	processErr error
	initCalled bool
}

func (mp *mockProcessor) Init(workerNum int) { mp.initCalled = true }
func (mp *mockProcessor) ProcessQuery(q Query, isWarm bool) ([]*Stat, error) {
	return mp.processRes, mp.processErr
}
