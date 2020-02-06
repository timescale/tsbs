package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/prometheus/prometheus/prompb"
	"github.com/timescale/tsbs/cmd/tsbs_load_prometheus/adapter/noop"
)

func TestPrometheusLoader(t *testing.T) {
	adapter := noop.Adapter{}
	server := httptest.NewServer(http.HandlerFunc(adapter.Handler))
	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	adapterWriteUrl = serverURL.String()
	pb := PrometheusBenchmark{}
	pp := pb.GetProcessor().(*PrometheusProcessor)
	batch := &PrometheusBatch{series: []prompb.TimeSeries{prompb.TimeSeries{}}}
	samples, _ := pp.ProcessBatch(batch, true)
	if samples != 1 {
		t.Error("wrong number of samples")
	}
	if adapter.SampleCounter != samples {
		t.Error("wrong number of samples processed")
	}
}
