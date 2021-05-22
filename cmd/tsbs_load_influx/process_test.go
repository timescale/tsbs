package main

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/timescale/tsbs/pkg/data"
)

func emptyLog(_ string, _ ...interface{}) (int, error) {
	return 0, nil
}

func TestProcessorInit(t *testing.T) {
	daemonURLs = []string{"url1", "url2"}
	printFn = emptyLog
	p := &processor{}
	p.Init(0, false, false)
	p.Close(true)
	if got := p.httpWriter.c.Host; got != daemonURLs[0] {
		t.Errorf("incorrect host: got %s want %s", got, daemonURLs[0])
	}
	if got := p.httpWriter.c.Database; got != loader.DatabaseName() {
		t.Errorf("incorrect database: got %s want %s", got, loader.DatabaseName())
	}

	p = &processor{}
	p.Init(1, false, false)
	p.Close(true)
	if got := p.httpWriter.c.Host; got != daemonURLs[1] {
		t.Errorf("incorrect host: got %s want %s", got, daemonURLs[1])
	}

	p = &processor{}
	p.Init(len(daemonURLs), false, false)
	p.Close(true)
	if got := p.httpWriter.c.Host; got != daemonURLs[0] {
		t.Errorf("incorrect host: got %s want %s", got, daemonURLs[0])
	}

}

func TestProcessorInitWithHTTPWriterConfig(t *testing.T) {
	var b bytes.Buffer
	counter := int64(0)
	var m sync.Mutex
	printFn = func(s string, args ...interface{}) (n int, err error) {
		atomic.AddInt64(&counter, 1)
		m.Lock()
		defer m.Unlock()
		return fmt.Fprintf(&b, s, args...)
	}
	workerNum := 4
	p := &processor{}
	w := NewHTTPWriter(testConf, testConsistency)
	p.initWithHTTPWriter(workerNum, w)
	p.Close(true)

	// Check p was initialized correctly with channels
	if got := cap(p.backingOffChan); got != backingOffChanCap {
		t.Errorf("backing off chan cap incorrect: got %d want %d", got, backingOffChanCap)
	}
	if got := cap(p.backingOffDone); got != 0 {
		t.Errorf("backing off done chan cap not 0: got %d", got)
	}

	// Check p was initialized with correct writer given conf
	err := testWriterMatchesConfig(p.httpWriter, testConf, testConsistency)
	if err != nil {
		t.Error(err)
	}

	// Check that backoff successfully shut down
	if got := atomic.LoadInt64(&counter); got != 1 {
		t.Errorf("printFn called incorrect # of times: got %d want %d", got, 1)
	}
	got := string(b.Bytes())
	if !strings.Contains(got, fmt.Sprintf("worker %d", workerNum)) {
		t.Errorf("printFn did not contain correct worker number: %s", got)
	}
}

func TestProcessorProcessBatch(t *testing.T) {
	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}
	f := &factory{}
	b := f.New().(*batch)
	pt := data.LoadedPoint{
		Data: []byte("tag1=tag1val,tag2=tag2val col1=0.0,col2=0.0 140"),
	}
	b.Append(pt)

	cases := []struct {
		doLoad        bool
		useGzip       bool
		shouldBackoff bool
		shouldFatal   bool
	}{
		{
			doLoad:  false,
			useGzip: false,
		},
		{
			doLoad:  true,
			useGzip: false,
		},
		{
			doLoad:  true,
			useGzip: true,
		},
		{
			doLoad:        true,
			useGzip:       false,
			shouldBackoff: true,
		},
		{
			doLoad:      true,
			shouldFatal: true,
		},
	}

	for _, c := range cases {
		var ch chan struct{}
		fatalCalled := false
		if c.shouldFatal {
			fatal = func(format string, args ...interface{}) {
				fatalCalled = true
			}
		} else {
			fatal = func(format string, args ...interface{}) {
				t.Errorf("fatal called for case %v unexpectedly\n", c)
				fmt.Printf(format, args...)
			}
			ch = launchHTTPServer()
		}

		p := &processor{}
		w := NewHTTPWriter(testConf, testConsistency)

		// If the case should backoff, we tell our dummy server to do so by
		// modifying the URL params. This should keep ProcessBatch in a loop
		// until it gets a response that is not a backoff (every other response from the server).
		if c.shouldBackoff {
			normalURL := string(w.url)
			w.url = []byte(fmt.Sprintf("%s&%s=true", normalURL, shouldBackoffParam))
		}

		p.initWithHTTPWriter(0, w)
		useGzip = c.useGzip
		mCnt, rCnt := p.ProcessBatch(b, c.doLoad)
		if c.shouldFatal {
			if !fatalCalled {
				t.Errorf("fatal was not called when it should have been")
			}
			continue
		} else {
			if mCnt != b.metrics {
				t.Errorf("process batch returned less metrics than batch: got %d want %d", mCnt, b.metrics)
			}
			if rCnt != uint64(b.rows) {
				t.Errorf("process batch returned less rows than batch: got %d want %d", rCnt, b.rows)
			}
			p.Close(true)

			shutdownHTTPServer(ch)
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func TestProcessorProcessBackoffMessages(t *testing.T) {
	var b bytes.Buffer
	counter := int64(0)
	var m sync.Mutex
	printFn = func(s string, args ...interface{}) (n int, err error) {
		atomic.AddInt64(&counter, 1)
		m.Lock()
		defer m.Unlock()
		return fmt.Fprintf(&b, s, args...)
	}
	workerNum := 4
	p := &processor{}
	w := NewHTTPWriter(testConf, testConsistency)
	p.initWithHTTPWriter(workerNum, w)

	// Sending false at the beginning should do nothing
	p.backingOffChan <- false
	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt64(&counter); got != 0 {
		m.Lock()
		defer m.Unlock()
		t.Errorf("printFn called when not expected after just false: msg %s", string(b.Bytes()))
	}
	// Same if another false:
	p.backingOffChan <- false
	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt64(&counter); got != 0 {
		m.Lock()
		defer m.Unlock()
		t.Errorf("printFn called when not expected after second false: msg %s", string(b.Bytes()))
	}

	// Send true, should be no print
	p.backingOffChan <- true
	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt64(&counter); got != 0 {
		m.Lock()
		defer m.Unlock()
		t.Errorf("printFn called when not expected after first true: msg %s", string(b.Bytes()))
	}
	// Another true, should be no print still
	p.backingOffChan <- true
	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt64(&counter); got != 0 {
		m.Lock()
		defer m.Unlock()
		t.Errorf("printFn called when not expected after second true: msg %s", string(b.Bytes()))
	}
	// Now false, should be a print with non-0 time
	p.backingOffChan <- false
	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt64(&counter); got != 1 {
		m.Lock()
		defer m.Unlock()
		t.Errorf("printFn not called right number of times after false: %d with %s", got, string(b.Bytes()))
	} else {
		m.Lock()
		msg := string(b.Bytes())
		m.Unlock()
		if !strings.Contains(msg, "took 0.1") {
			t.Errorf("backoff might have taken an incorrect amt of time: %s", msg)
		}
	}
}
