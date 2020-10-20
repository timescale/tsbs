package prometheus

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/timescale/promscale/pkg/prompb"
)

// Client is a wrapper around http.Client
// Client sends data to Prometheus adapter
type Client struct {
	url        *url.URL
	httpClient *http.Client
}

// NewClient ..
func NewClient(urlStr string, timeout time.Duration) (*Client, error) {
	url, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	//options copied from Prometheus
	var rt http.RoundTripper = &http.Transport{
		MaxIdleConns:        20000,
		MaxIdleConnsPerHost: 1000, // see https://github.com/golang/go/issues/13801
		DisableKeepAlives:   false,
		DisableCompression:  true,
		// 5 minutes is typically above the maximum sane scrape interval. So we can
		// use keepalive for all configurations.
		IdleConnTimeout:       5 * time.Minute,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	httpClient := &http.Client{Transport: rt, Timeout: timeout}
	return &Client{url: url, httpClient: httpClient}, nil
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(proto.Buffer)
	},
}

var noBytes = []byte{}

var snappyPool = sync.Pool{
	New: func() interface{} {
		return noBytes
	},
}

// Post sends POST request to Prometheus adapter
func (c *Client) Post(series []prompb.TimeSeries) error {
	wr := &prompb.WriteRequest{
		Timeseries: series,
	}

	buffer := bufferPool.Get().(*proto.Buffer)
	buffer.Reset()
	err := buffer.Marshal(wr)
	if err != nil {
		return err
	}
	compressed := snappyPool.Get().([]byte)
	compressed = compressed[:cap(compressed)]
	compressed = snappy.Encode(compressed, buffer.Bytes())
	bufferPool.Put(buffer)
	httpReq, err := http.NewRequest("POST", c.url.String(), bytes.NewReader(compressed))
	if err != nil {
		return err
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	httpResp, err := c.httpClient.Do(httpReq)
	snappyPool.Put(compressed)
	if err != nil {
		return err
	}
	defer func() {
		io.Copy(ioutil.Discard, httpResp.Body)
		httpResp.Body.Close()
	}()

	if httpResp.StatusCode/100 != 2 {
		return fmt.Errorf("Prometheus adapter returned status: %s", httpResp.Status)
	}
	return nil
}
