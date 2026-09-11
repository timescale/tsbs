package zigtsdb

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/timescale/tsbs/pkg/data"
)

// fakeServer mimics the zigtsdb ingest endpoint. Each call pops the next
// scripted response; when the script is exhausted it answers 200 ok.
type fakeServer struct {
	mu        sync.Mutex
	server    *httptest.Server
	bodies    [][]byte
	paths     []string
	responses []scripted
}

type scripted struct {
	code int
	body string
}

func (f *fakeServer) handler(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	body, _ := ioutil.ReadAll(r.Body)
	f.bodies = append(f.bodies, body)
	f.paths = append(f.paths, r.URL.RequestURI())
	if len(f.responses) > 0 {
		next := f.responses[0]
		f.responses = f.responses[1:]
		w.WriteHeader(next.code)
		_, _ = w.Write([]byte(next.body))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(fmt.Sprintf(`{"status":"ok","points":%d}`, bytes.Count(body, newLine))))
}

func (f *fakeServer) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.bodies)
}

func startFakeServer(t *testing.T, script ...scripted) *fakeServer {
	f := &fakeServer{responses: script}
	f.server = httptest.NewServer(http.HandlerFunc(f.handler))
	t.Cleanup(f.server.Close)
	return f
}

func newProcessor(url string, maxBody int) *processor {
	return &processor{
		urls:       []string{url},
		ingestPath: defaultIngestPath,
		maxBody:    maxBody,
		maxRetries: 3,
	}
}

func fillBatch(t *testing.T, lines ...string) *batch {
	b := newTestFactory().New().(*batch)
	for _, l := range lines {
		b.Append(data.NewLoadedPoint([]byte(l)))
	}
	return b
}

func TestProcessBatchPostsExpandedLines(t *testing.T) {
	srv := startFakeServer(t)
	p := newProcessor(srv.server.URL, 1<<20)
	p.Init(0, true, false)
	b := fillBatch(t, "cpu,hostname=host_0 a=1,b=2 100")
	metrics, rows := p.ProcessBatch(b, true)
	if metrics != 2 || rows != 1 {
		t.Fatalf("expected 2 metrics / 1 row, got %d / %d", metrics, rows)
	}
	if srv.calls() != 1 {
		t.Fatalf("expected exactly one POST, got %d", srv.calls())
	}
	if got := string(srv.bodies[0]); got != "cpu_a,hostname=host_0 value=1 100\ncpu_b,hostname=host_0 value=2 100\n" {
		t.Fatalf("unexpected body %q", got)
	}
	if !strings.HasSuffix(srv.paths[0], defaultIngestPath) {
		t.Fatalf("expected default ingest path, got %s", srv.paths[0])
	}
}

func TestProcessBatchDoLoadFalseSkipsNetwork(t *testing.T) {
	srv := startFakeServer(t)
	p := newProcessor(srv.server.URL, 1<<20)
	p.Init(0, false, false)
	b := fillBatch(t, "cpu,hostname=host_0 a=1 100")
	metrics, rows := p.ProcessBatch(b, false)
	if metrics != 1 || rows != 1 || srv.calls() != 0 {
		t.Fatalf("expected counts without network calls, got %d/%d calls=%d", metrics, rows, srv.calls())
	}
}

func TestProcessBatchChunksAtLineBoundaries(t *testing.T) {
	srv := startFakeServer(t)
	line := "cpu,hostname=host_0 a=1,b=2,c=3,d=4 100" // expands to 4 lines of 36 bytes
	b := fillBatch(t, line, line, line)               // 12 wire lines
	total := b.buf.Len()
	maxBody := total/3 + 5 // forces several chunks, never on a line boundary
	p := newProcessor(srv.server.URL, maxBody)
	p.Init(0, true, false)
	p.ProcessBatch(b, true)
	if srv.calls() < 3 {
		t.Fatalf("expected at least 3 chunks, got %d", srv.calls())
	}
	var joined []byte
	for _, body := range srv.bodies {
		if len(body) > maxBody {
			t.Fatalf("chunk of %d bytes exceeds max body %d", len(body), maxBody)
		}
		if body[len(body)-1] != '\n' {
			t.Fatalf("chunk does not end on a line boundary: %q", body)
		}
		joined = append(joined, body...)
	}
	if bytes.Count(joined, newLine) != 12 {
		t.Fatalf("expected all 12 lines to be delivered, got %d", bytes.Count(joined, newLine))
	}
}

func TestProcessBatchHonorsRetryAfterOn429(t *testing.T) {
	srv := startFakeServer(t,
		scripted{429, `{"error":"ingest_backlog","retry_after_seconds":2}`},
		scripted{429, `{"error":"Quota exceeded. Upgrade tier for more capacity.","retry_after_seconds":3}`},
	)
	var slept []time.Duration
	restore := stubSleep(func(d time.Duration) { slept = append(slept, d) })
	defer restore()
	p := newProcessor(srv.server.URL, 1<<20)
	p.Init(0, true, false)
	p.ProcessBatch(fillBatch(t, "cpu,hostname=host_0 a=1 100"), true)
	if srv.calls() != 3 {
		t.Fatalf("expected 2 retries + 1 success = 3 calls, got %d", srv.calls())
	}
	if len(slept) != 2 || slept[0] != 2*time.Second || slept[1] != 3*time.Second {
		t.Fatalf("expected sleeps of 2s and 3s, got %v", slept)
	}
}

func TestProcessBatchRetriesOn503(t *testing.T) {
	srv := startFakeServer(t, scripted{503, `{"error":"not_leader"}`})
	restore := stubSleep(func(time.Duration) {})
	defer restore()
	p := newProcessor(srv.server.URL, 1<<20)
	p.Init(0, true, false)
	p.ProcessBatch(fillBatch(t, "cpu,hostname=host_0 a=1 100"), true)
	if srv.calls() != 2 {
		t.Fatalf("expected one retry after 503, got %d calls", srv.calls())
	}
}

func TestProcessBatchExhaustedRetriesIsFatal(t *testing.T) {
	srv := startFakeServer(t,
		scripted{429, `{"error":"ingest_backlog","retry_after_seconds":1}`},
		scripted{429, `{"error":"ingest_backlog","retry_after_seconds":1}`},
		scripted{429, `{"error":"ingest_backlog","retry_after_seconds":1}`},
		scripted{429, `{"error":"ingest_backlog","retry_after_seconds":1}`},
	)
	restoreSleep := stubSleep(func(time.Duration) {})
	defer restoreSleep()
	restoreFatal := captureFatal(t)
	defer restoreFatal()
	p := newProcessor(srv.server.URL, 1<<20) // maxRetries = 3
	p.Init(0, true, false)
	p.ProcessBatch(fillBatch(t, "cpu,hostname=host_0 a=1 100"), true)
	if fatalCalls() != 1 {
		t.Fatalf("expected fatal after retries exhausted")
	}
}

func TestProcessBatch413IsFatalWithHint(t *testing.T) {
	srv := startFakeServer(t, scripted{413, `{"error":"payload too large"}`})
	restoreFatal := captureFatal(t)
	defer restoreFatal()
	p := newProcessor(srv.server.URL, 1<<20)
	p.Init(0, true, false)
	p.ProcessBatch(fillBatch(t, "cpu,hostname=host_0 a=1 100"), true)
	if fatalCalls() != 1 || !strings.Contains(lastFatal(), "max-body-bytes") {
		t.Fatalf("expected fatal mentioning max-body-bytes, got %q", lastFatal())
	}
}

func TestProcessBatchNonOkStatusBodyIsFatal(t *testing.T) {
	srv := startFakeServer(t, scripted{200, `{"error":"unexpected"}`})
	restoreFatal := captureFatal(t)
	defer restoreFatal()
	p := newProcessor(srv.server.URL, 1<<20)
	p.Init(0, true, false)
	p.ProcessBatch(fillBatch(t, "cpu,hostname=host_0 a=1 100"), true)
	if fatalCalls() != 1 {
		t.Fatalf("expected fatal on 200 without status ok")
	}
}

func TestSplitChunksSingleOversizedLineIsFatal(t *testing.T) {
	restoreFatal := captureFatal(t)
	defer restoreFatal()
	_ = splitChunks([]byte("this line is longer than the limit\n"), 10)
	if fatalCalls() != 1 {
		t.Fatalf("expected fatal for a line larger than max body")
	}
}

func TestInitRoundRobinsURLs(t *testing.T) {
	p := &processor{urls: []string{"http://a:8080/", "http://b:8080"}, ingestPath: defaultIngestPath}
	p.Init(0, true, false)
	if p.url != "http://a:8080"+defaultIngestPath {
		t.Fatalf("worker 0 got %s", p.url)
	}
	p.Init(1, true, false)
	if p.url != "http://b:8080"+defaultIngestPath {
		t.Fatalf("worker 1 got %s", p.url)
	}
	p.Init(2, true, false)
	if p.url != "http://a:8080"+defaultIngestPath {
		t.Fatalf("worker 2 got %s", p.url)
	}
}
