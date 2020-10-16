package noop

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/log"
	"github.com/timescale/promscale/pkg/prompb"
)

type Adapter struct {
	port          int
	ReqCounter    uint64
	SampleCounter uint64
}

func NewAdapter(port int) *Adapter {
	return &Adapter{port: port}
}

// Start starts no-op Prometheus adapter. This call will block go-routine
func (adapter *Adapter) Start() error {
	http.HandleFunc("/", adapter.Handler)
	log.Info("msg", fmt.Sprintf("Starting noop adapter listening on: %d\n", adapter.port))
	return http.ListenAndServe(fmt.Sprintf(":%d", adapter.port), nil)
}

// Handler counts number of requests and samples
func (adapter *Adapter) Handler(rw http.ResponseWriter, req *http.Request) {
	compressed, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Error("msg", "error while reading request", "error", err)
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	decompressed, err := snappy.Decode(nil, compressed)
	if err != nil {
		log.Error("msg", "error while decompressing request", "error", err)
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	var protoReq prompb.WriteRequest
	if err := proto.Unmarshal(decompressed, &protoReq); err != nil {
		log.Error("msg", "error while unmarshalling protobuf request", "error", err)
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}
	adapter.ReqCounter++
	adapter.SampleCounter += uint64(len(protoReq.Timeseries))
}
