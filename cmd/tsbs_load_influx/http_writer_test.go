package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valyala/fasthttp"
)

const (
	shouldBackoffParam = "shouldErr"
	shouldInvalidParam = "shouldInvalid"
	httpServerPort     = ":8080"
	httpDelay          = 50 * time.Millisecond
)

var (
	testConf = HTTPWriterConfig{
		Host:     "http://localhost" + httpServerPort + "/",
		Database: "test",
	}
	testConsistency = "one"
)

func runHTTPServer(c chan struct{}) {
	m := http.NewServeMux()
	s := http.Server{Addr: httpServerPort, Handler: m}
	i := int64(0)
	m.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.RawQuery, shouldBackoffParam) {
			coinflip := atomic.AddInt64(&i, 1)
			if coinflip%2 == 1 {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, string(backoffMagicWords1))
			} else {
				w.WriteHeader(http.StatusNoContent)
				fmt.Fprintf(w, "")
			}
		} else if strings.Contains(r.URL.RawQuery, shouldInvalidParam) {
			fmt.Fprintf(w, "success should be an empty msg")
		} else {
			w.WriteHeader(http.StatusNoContent)
			fmt.Fprintf(w, "")
		}
	})

	go s.ListenAndServe()
	time.Sleep(httpDelay) // give time for server to start
	c <- struct{}{}
	time.Sleep(httpDelay) // sleep to not grab my own msg
	<-c
	s.Shutdown(context.Background())
	c <- struct{}{}
}

func launchHTTPServer() chan struct{} {
	c := make(chan struct{})
	go runHTTPServer(c)
	<-c // wait for server to be ready
	return c
}

func shutdownHTTPServer(c chan struct{}) {
	c <- struct{}{}       // tell server to shutdown
	time.Sleep(httpDelay) // sleep to not grab my own msg
	<-c                   // wait for clean shutdown
}

func testWriterMatchesConfig(w *HTTPWriter, conf HTTPWriterConfig, consistency string) error {
	// Check HTTP Config is the same
	if got := w.c.Host; got != conf.Host {
		return fmt.Errorf("incorrect config host: got %s want %s", got, conf.Host)
	}
	if got := w.c.Database; got != conf.Database {
		return fmt.Errorf("incorrect config host: got %s want %s", got, conf.Database)
	}

	// Check URL is accurate
	got := string(w.url)
	if !strings.Contains(got, conf.Host) {
		return fmt.Errorf("url does not contain correct host: looking for %s in %s", conf.Host, got)
	}
	if !strings.Contains(got, consistency) {
		return fmt.Errorf("url does not contain correct consistency: looking for %s in %s", consistency, got)
	}
	if want := url.QueryEscape(conf.Database); !strings.Contains(got, want) {
		return fmt.Errorf("url does not contain correct database name: looking for %s in %s", want, got)
	}

	return nil
}

func TestNewHTTPWriter(t *testing.T) {
	w := NewHTTPWriter(testConf, testConsistency)
	// Check client name
	if got := w.client.Name; got != httpClientName {
		t.Errorf("name of http client is incorrect: got %s want %s", got, httpClientName)
	}

	err := testWriterMatchesConfig(w, testConf, testConsistency)
	if err != nil {
		t.Error(err)
	}
}

func TestHTTPWriterInitializeReq(t *testing.T) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	w := NewHTTPWriter(testConf, testConsistency)
	body := "this is a test body"
	w.initializeReq(req, []byte(body), false, "")

	if got := string(req.Body()); got != body {
		t.Errorf("non-gzip: body not correct: got '%s' want '%s'", got, body)
	}
	if got := string(req.Header.Method()); got != string(methodPost) {
		t.Errorf("non-gzip: method not correct: got %s want %s", got, string(methodPost))
	}
	if got := string(req.Header.RequestURI()); got != string(w.url) {
		t.Errorf("non-gzip: URI is not correct: got %s want %s", got, string(w.url))
	}
	if got := string(req.Header.Peek(headerContentEncoding)); got != "" {
		t.Errorf("non-gzip: Content-Encoding is not empty: got %s", got)
	}

	w.initializeReq(req, []byte(body), true, "")
	if got := string(req.Header.Peek(headerContentEncoding)); got != headerGzip {
		t.Errorf("gzip: Content-Encoding is not correct: got %s want %s", got, headerGzip)
	}
}

func TestHTTPWriterExecuteReq(t *testing.T) {
	c := launchHTTPServer()

	// Success case test, make sure no error and positive latency
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	w := NewHTTPWriter(testConf, testConsistency)
	body := "this is a test body"
	normalURL := w.url // save for later modification
	w.initializeReq(req, []byte(body), false, "")
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	lat, err := w.executeReq(req, resp)
	if err != nil {
		t.Errorf("unexpected error received: %v", err)
	}
	if lat <= 0 {
		t.Errorf("latency is unrealistic (<= 0): %d", lat)
	}

	// Backoff case test, make sure its a backoff error and positive latency
	resp = fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	w.url = []byte(fmt.Sprintf("%s&%s=true", string(normalURL), shouldBackoffParam))
	req = fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	w.initializeReq(req, []byte(body), false, "")
	lat, err = w.executeReq(req, resp)
	if err != errBackoff {
		t.Errorf("unexpected error response received (not backoff error): %v", err)
	}
	if lat <= 0 {
		t.Errorf("latency is unrealistic (<= 0): %d", lat)
	}

	// Unexpected response case test, make sure its an error and positive latency
	resp = fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	w.url = []byte(fmt.Sprintf("%s&%s=true", string(normalURL), shouldInvalidParam))
	req = fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	w.initializeReq(req, []byte(body), false, "")
	lat, err = w.executeReq(req, resp)
	if err == nil {
		t.Errorf("unexpected non-error response received")
	}
	if lat <= 0 {
		t.Errorf("latency is unrealistic (<= 0): %d", lat)
	}

	shutdownHTTPServer(c)
}

func TestBackpressurePred(t *testing.T) {
	cases := []struct {
		body string
		want bool
	}{
		{
			body: "yadda yadda" + string(backoffMagicWords0),
			want: true,
		},
		{
			body: "yadda" + string(backoffMagicWords1),
			want: true,
		},
		{
			body: string(backoffMagicWords2a),
			want: false, // need both magic strings or it fails
		},
		{
			body: string(backoffMagicWords2a) + " AND " + string(backoffMagicWords2b),
			want: true,
		},
		{
			body: string(backoffMagicWords3) + " yadda",
			want: true,
		},
		{
			body: "yadda " + string(backoffMagicWords4) + " yadda",
			want: true,
		},
		{
			body: "foo " + string(backoffMagicWords5) + " yadda",
			want: true,
		},
		{
			body: string(backoffMagicWords0[2:]),
			want: false,
		},
	}

	for _, c := range cases {
		if got := backpressurePred([]byte(c.body)); got != c.want {
			t.Errorf("'%s' did not give correct backpressure result: got %v want %v", c.body, got, c.want)
		}
	}
}
