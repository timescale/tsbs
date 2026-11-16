package zigtsdb

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

type fakeQueryServer struct {
	mu      sync.Mutex
	server  *httptest.Server
	metrics []string
	deleted []string
	healthy bool
}

func (f *fakeQueryServer) handler(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case r.URL.Path == "/health":
		if !f.healthy {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"status":"healthy"}`))
	case r.URL.Path == "/query" && r.Method == http.MethodGet:
		if r.URL.Query().Get("zql") != "SHOW METRICS" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		out := `{"status":"ok","metrics":[`
		for i, m := range f.metrics {
			if i > 0 {
				out += ","
			}
			out += `"` + m + `"`
		}
		_, _ = w.Write([]byte(out + "]}"))
	case r.URL.Path == "/query" && r.Method == http.MethodDelete:
		m := r.URL.Query().Get("metric")
		f.deleted = append(f.deleted, m)
		kept := f.metrics[:0]
		for _, x := range f.metrics {
			if x != m {
				kept = append(kept, x)
			}
		}
		f.metrics = kept
		_, _ = w.Write([]byte(`{"status":"ok","metric":"` + m + `","deleted_series":1}`))
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func startFakeQueryServer(t *testing.T, healthy bool, metrics ...string) *fakeQueryServer {
	f := &fakeQueryServer{healthy: healthy, metrics: metrics}
	f.server = httptest.NewServer(http.HandlerFunc(f.handler))
	t.Cleanup(f.server.Close)
	return f
}

func TestDBCreatorInitRequiresHealth(t *testing.T) {
	srv := startFakeQueryServer(t, false)
	restore := captureFatal(t)
	defer restore()
	c := &dbCreator{url: srv.server.URL}
	c.Init()
	if fatalCalls() != 1 {
		t.Fatalf("expected fatal when /health is not 200")
	}
}

func TestDBCreatorExistsAndRemoveOnlyTSBSMetrics(t *testing.T) {
	srv := startFakeQueryServer(t, true, "cpu_usage_user", "mem_used", "unrelated_metric", "cpu_usage_system")
	c := &dbCreator{url: srv.server.URL}
	c.Init()
	if !c.DBExists("benchmark") {
		t.Fatalf("expected DBExists true when TSBS metrics are present")
	}
	if err := c.RemoveOldDB("benchmark"); err != nil {
		t.Fatalf("RemoveOldDB: %v", err)
	}
	if len(srv.deleted) != 3 {
		t.Fatalf("expected 3 deletions, got %v", srv.deleted)
	}
	for _, d := range srv.deleted {
		if d == "unrelated_metric" {
			t.Fatalf("must never delete non-TSBS metrics")
		}
	}
	if c.DBExists("benchmark") {
		t.Fatalf("expected DBExists false after removal")
	}
	if err := c.CreateDB("benchmark"); err != nil {
		t.Fatalf("CreateDB must be a no-op, got %v", err)
	}
}

func TestDBCreatorExistsFalseOnEmptyServer(t *testing.T) {
	srv := startFakeQueryServer(t, true, "unrelated_metric")
	c := &dbCreator{url: srv.server.URL}
	c.Init()
	if c.DBExists("benchmark") {
		t.Fatalf("expected DBExists false without TSBS metrics")
	}
}
