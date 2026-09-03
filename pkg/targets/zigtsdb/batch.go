package zigtsdb

import (
	"bytes"
	"fmt"
	"log"
	"sync"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/targets"
)

// fatal is overridable so tests can assert on fatal conditions.
var fatal = log.Fatalf

var (
	spaceSep = []byte(" ")
	commaSep = []byte(",")
	newLine  = []byte("\n")
	valueKey = []byte(" value=")
)

// batch accumulates zigtsdb wire lines. Each influx-format point appended is
// expanded into one zigtsdb line per field, because zigtsdb stores exactly
// one value per series (metric name + labels).
type batch struct {
	buf     *bytes.Buffer
	rows    uint64
	metrics uint64
	pool    *sync.Pool
}

func (b *batch) Len() uint {
	return uint(b.rows)
}

func (b *batch) Append(item data.LoadedPoint) {
	line := item.Data.([]byte)
	n, err := expandLine(line, b.buf)
	if err != nil {
		fatal("parse error: %s: %q", err, line)
		return
	}
	b.rows++
	b.metrics += n
}

// release returns the buffer to the pool once the batch has been sent.
func (b *batch) release() {
	b.buf.Reset()
	if b.pool != nil {
		b.pool.Put(b.buf)
	}
}

// expandLine rewrites one InfluxDB line-protocol point
//
//	<measurement>[,<tags>] <k1>=<v1>[,<k2>=<v2>...] <timestamp>
//
// into one zigtsdb line per field
//
//	<measurement>_<k>[,<tags>] value=<v> <timestamp>\n
//
// It returns the number of fields written. Integer fields lose the influx
// `i` suffix. String and boolean fields are rejected: zigtsdb values are
// float64, so only the devops/cpu-only use cases can be loaded.
func expandLine(line []byte, w *bytes.Buffer) (uint64, error) {
	if bytes.Count(line, spaceSep) != 2 {
		return 0, fmt.Errorf("line does not have 3 tuples, has %d", bytes.Count(line, spaceSep)+1)
	}
	firstSpace := bytes.IndexByte(line, ' ')
	lastSpace := bytes.LastIndexByte(line, ' ')
	head := line[:firstSpace]
	fields := line[firstSpace+1 : lastSpace]
	timestamp := line[lastSpace+1:]
	if len(fields) == 0 {
		return 0, fmt.Errorf("no fields")
	}
	if len(timestamp) == 0 {
		return 0, fmt.Errorf("empty timestamp")
	}

	measurement := head
	var tags []byte
	if comma := bytes.IndexByte(head, ','); comma >= 0 {
		measurement = head[:comma]
		tags = head[comma:] // keeps the leading comma, appended verbatim (order preserved)
	}

	var n uint64
	for len(fields) > 0 {
		var field []byte
		if comma := bytes.IndexByte(fields, ','); comma >= 0 {
			field, fields = fields[:comma], fields[comma+1:]
		} else {
			field, fields = fields, nil
		}
		eq := bytes.IndexByte(field, '=')
		if eq <= 0 || eq == len(field)-1 {
			return 0, fmt.Errorf("malformed field %q", field)
		}
		key, value := field[:eq], field[eq+1:]
		if value[0] == '"' {
			return 0, fmt.Errorf("string field %q is not supported by zigtsdb (iot use case)", key)
		}
		if isBool(value) {
			return 0, fmt.Errorf("boolean field %q is not supported by zigtsdb", key)
		}
		if last := value[len(value)-1]; last == 'i' || last == 'u' {
			value = value[:len(value)-1]
		}
		w.Write(measurement)
		w.WriteByte('_')
		w.Write(key)
		w.Write(tags)
		w.Write(valueKey)
		w.Write(value)
		w.WriteByte(' ')
		w.Write(timestamp)
		w.Write(newLine)
		n++
	}
	return n, nil
}

func isBool(v []byte) bool {
	switch string(v) {
	case "t", "T", "true", "True", "TRUE", "f", "F", "false", "False", "FALSE":
		return true
	}
	return false
}

type factory struct {
	bufPool *sync.Pool
}

func newFactory(bufSize int) *factory {
	return &factory{bufPool: &sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, bufSize))
		},
	}}
}

func (f *factory) New() targets.Batch {
	return &batch{buf: f.bufPool.Get().(*bytes.Buffer), pool: f.bufPool}
}
