package telemetry

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"sync"

	"github.com/valyala/fasthttp"
)

type valueKind byte

const (
	invalidValueKind valueKind = iota
	float64ValueKind
	int64ValueKind
	boolValueKind
)

// Tag represents an InfluxDB tag.
// Users should prefer to keep the strings as long-lived items.
type Tag struct {
	Key, Value string
}

func (t *Tag) Serialize(w io.Writer) {
	fmt.Fprintf(w, "%s=%s", t.Key, t.Value)
}

// Field represents an InfluxDB field.
// Users should prefer to keep the strings as long-lived items.
//
// Implementor's note: this type uses more memory than is ideal, but it avoids
// unsafe and it avoids reflection.
type Field struct {
	key          string
	int64Value   int64
	float64Value float64
	boolValue    bool
	mode         valueKind
}

func (f *Field) SetFloat64(key string, x float64) {
	if f.mode != invalidValueKind {
		panic("logic error: Field already has a value")
	}
	f.key = key
	f.float64Value = x
	f.mode = float64ValueKind
}
func (f *Field) SetInt64(key string, x int64) {
	if f.mode != invalidValueKind {
		panic("logic error: Field already has a value")
	}
	f.key = key
	f.int64Value = x
	f.mode = int64ValueKind
}
func (f *Field) SetBool(key string, x bool) {
	if f.mode != invalidValueKind {
		panic("logic error: Field already has a value")
	}
	f.key = key
	f.boolValue = x
	f.mode = boolValueKind
}

func (f *Field) Serialize(w io.Writer) {
	if f.mode == int64ValueKind {
		fmt.Fprintf(w, "%s=%di", f.key, f.int64Value)
	} else if f.mode == float64ValueKind {
		fmt.Fprintf(w, "%s=%f", f.key, f.float64Value)
	} else {
		fmt.Fprintf(w, "%s=%v", f.key, f.boolValue)
	}
}

// Point wraps an InfluxDB point data.
// Its primary purpose is to be serialized out to a []byte.
// Users should prefer to keep the strings as long-lived items.
type Point struct {
	Measurement   string
	Tags          []Tag
	Fields        []Field
	TimestampNano int64
}

func (p *Point) Init(m string, ts int64) {
	p.Measurement = m
	p.TimestampNano = ts
}

func (p *Point) AddTag(k, v string) {
	p.Tags = append(p.Tags, Tag{Key: k, Value: v})
}

func (p *Point) AddInt64Field(k string, x int64) {
	f := Field{}
	f.SetInt64(k, x)
	p.Fields = append(p.Fields, f)
}

func (p *Point) AddBoolField(k string, x bool) {
	f := Field{}
	f.SetBool(k, x)
	p.Fields = append(p.Fields, f)
}

func (p *Point) AddFloat64Field(k string, x float64) {
	f := Field{}
	f.SetFloat64(k, x)
	p.Fields = append(p.Fields, f)
}

func (p *Point) Serialize(w io.Writer) {
	fmt.Fprintf(w, "%s", p.Measurement)
	for i, tag := range p.Tags {
		if i == 0 {
			fmt.Fprint(w, ",")
		}

		tag.Serialize(w)
		if i < len(p.Tags)-1 {
			fmt.Fprint(w, ",")
		}
	}
	for i, field := range p.Fields {
		if i == 0 {
			fmt.Fprint(w, " ")
		}

		field.Serialize(w)
		if i < len(p.Fields)-1 {
			fmt.Fprint(w, ",")
		}
	}
	if p.TimestampNano > 0 {
		fmt.Fprintf(w, " %d", p.TimestampNano)
	}
}

func (p *Point) Reset() {
	p.Measurement = ""
	p.Tags = p.Tags[:0]
	p.Fields = p.Fields[:0]
	p.TimestampNano = 0
}

var GlobalPointPool *sync.Pool = &sync.Pool{New: func() interface{} { return &Point{} }}

func GetPointFromGlobalPool() *Point {
	return GlobalPointPool.Get().(*Point)
}
func PutPointIntoGlobalPool(p *Point) {
	p.Reset()
	GlobalPointPool.Put(p)
}

type Collector struct {
	Points []*Point

	client           *fasthttp.Client
	uri              string
	encodedBasicAuth string

	buf *bytes.Buffer
}

func NewCollector(influxhost, dbname, basicAuth string) *Collector {
	encodedBasicAuth := ""
	if basicAuth != "" {
		encodedBasicAuth = "Basic " + base64.StdEncoding.EncodeToString([]byte(basicAuth))
	}
	return &Collector{
		buf:    new(bytes.Buffer),
		Points: make([]*Point, 0, 0),
		client: &fasthttp.Client{
			Name: "collector",
		},
		uri:              influxhost + "/write?db=" + url.QueryEscape(dbname),
		encodedBasicAuth: encodedBasicAuth,
	}
}

func (c *Collector) Put(p *Point) {
	c.Points = append(c.Points, p)
}

func (c *Collector) Reset() {
	c.Points = c.Points[:0]
	c.buf.Reset()
}

func (c *Collector) PrepBatch() {
	for _, p := range c.Points {
		p.Serialize(c.buf)
		fmt.Fprint(c.buf, "\n")
	}
}

func (c *Collector) SendBatch() error {
	req := fasthttp.AcquireRequest()
	req.Header.SetMethod("POST")
	req.Header.SetRequestURI(c.uri)
	if c.encodedBasicAuth != "" {
		req.Header.Set("Authorization", c.encodedBasicAuth)
	}
	req.SetBody(c.buf.Bytes())

	// Perform the request while tracking latency:
	resp := fasthttp.AcquireResponse()
	err := c.client.Do(req, resp)

	if resp.StatusCode() != fasthttp.StatusNoContent {
		return fmt.Errorf("collector error: unexpected status code %d", resp.StatusCode())
	}

	fasthttp.ReleaseResponse(resp)
	fasthttp.ReleaseRequest(req)

	return err
}

// EZRunAsync runs a collection loop with many defaults already set. It will
// abort the program if an error occurs. Assumes points are owned by the
// GlobalPointPool.
func EZRunAsync(c *Collector, batchSize uint64, writeToStderr bool, skipN uint64) (src chan *Point, done chan struct{}) {
	src = make(chan *Point, 100)
	done = make(chan struct{})

	send := func() {
		c.PrepBatch()
		if writeToStderr {
			_, err := os.Stderr.Write(c.buf.Bytes())
			if err != nil {
				log.Fatalf("collector error (stderr): %v", err.Error())
			}
		}

		err := c.SendBatch()
		if err != nil {
			log.Fatalf("collector error (http): %v", err.Error())
		}

		for _, p := range c.Points {
			PutPointIntoGlobalPool(p)
		}
	}

	go func() {
		var i uint64
		for p := range src {
			i++

			if i <= skipN {
				continue
			}

			c.Put(p)

			if i%batchSize == 0 {
				send()
				c.Reset()
			}
		}
		if len(c.Points) > 0 {
			send()
			c.Reset()
		}
		done <- struct{}{}
	}()

	return
}
