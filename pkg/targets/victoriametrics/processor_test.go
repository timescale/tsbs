package victoriametrics

import (
	"bytes"
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
)

func TestProcessorProcessBatch(t *testing.T) {

	testCases := []struct {
		doLoad        bool
		points        []string
		metrics, rows uint64
	}{
		{
			doLoad: true,
			points: []string{
				"tag1=tag1val,tag2=tag2val col1=0.0,col2=0.0 140",
			},
			metrics: 2,
			rows:    1,
		},
		{
			doLoad: false,
			points: []string{
				"tag1=tag1val,tag2=tag2val col1=0.0,col2=0.0 140",
			},
			metrics: 2,
			rows:    1,
		},
		{
			doLoad: true,
			points: []string{
				"tag1=tag1val,tag2=tag2val col1=0.0,col2=0.0 140",
				"tag1=tag1val,tag2=tag2val col3=1.0,col4=1.0 190",
				"tag1=tag1val,tag2=tag2val col5=1.0,col6=1.0 190",
				"tag1=tag1val,tag2=tag2val col7=1.0,col8=1.0 190",
			},
			metrics: 8,
			rows:    4,
		},
	}

	f := &factory{bufPool: &sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 16*1024*1024))
		},
	}}
	vm := startFakeVMServer(t)
	vmURLs := []string{vm.server.URL}
	for _, tc := range testCases {
		name := fmt.Sprintf("%dmetrics %drows %dpoints load %v",
			tc.metrics, tc.rows, len(tc.points), tc.doLoad)
		t.Run(name, func(t *testing.T) {
			b := f.New().(*batch)
			for _, point := range tc.points {
				b.Append(data.LoadedPoint{
					Data: []byte(point),
				})
			}

			p := &processor{vmURLs: vmURLs}
			const ignored = false
			p.Init(1, ignored, ignored)
			callsBefore := vm.getCalls()
			metrics, rows := p.ProcessBatch(b, tc.doLoad)
			if metrics != tc.metrics {
				t.Fatalf("expected %d metrics; got %d", tc.metrics, metrics)
			}
			if rows != tc.rows {
				t.Fatalf("expected %d rows; got %d", tc.rows, rows)
			}
			calls := vm.getCalls() - callsBefore
			if tc.doLoad && calls != 1 {
				t.Fatalf("expected batch to be processed")
			}
			if !tc.doLoad && calls != 0 {
				t.Fatalf("expected batch to be not processed")
			}
		})
	}
}

type fakeVMServer struct {
	t      *testing.T
	calls  uint64
	server *httptest.Server
}

func (vm *fakeVMServer) incCalls()        { atomic.AddUint64(&vm.calls, 1) }
func (vm *fakeVMServer) getCalls() uint64 { return atomic.LoadUint64(&vm.calls) }

func (vm *fakeVMServer) handler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		vm.t.Fatalf("unexpected HTTP method %q", r.Method)
	}
	vm.incCalls()
	w.WriteHeader(http.StatusNoContent)
}

func startFakeVMServer(t *testing.T) *fakeVMServer {
	vm := &fakeVMServer{t: t}
	s := httptest.NewServer(http.HandlerFunc(vm.handler))
	vm.server = s
	return vm
}
