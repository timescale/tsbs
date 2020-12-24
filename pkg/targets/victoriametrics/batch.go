package victoriametrics

import (
	"bytes"
	"github.com/timescale/tsbs/pkg/data"
	"log"
)

const errNotThreeTuplesFmt = "parse error: line does not have 3 tuples, has %d"

var (
	spaceSep = []byte(" ")
	commaSep = []byte(",")
	newLine  = []byte("\n")
)

type batch struct {
	buf     *bytes.Buffer
	rows    uint64
	metrics uint64
}

func (b *batch) Len() uint {
	return uint(b.rows)
}

func (b *batch) Append(item data.LoadedPoint) {
	that := item.Data.([]byte)
	b.rows++

	// Each influx line is format "csv-tags csv-fields timestamp"
	if args := bytes.Count(that, spaceSep); args != 2 {
		log.Fatalf(errNotThreeTuplesFmt, args+1)
		return
	}

	// seek for fields position in slice
	fieldsPos := bytes.Index(that, spaceSep)
	// seek for timestamps position in slice
	timestampPos := bytes.Index(that[fieldsPos+1:], spaceSep) + fieldsPos
	fields := that[fieldsPos+1 : timestampPos]
	b.metrics += uint64(bytes.Count(fields, commaSep) + 1)

	b.buf.Write(that)
	b.buf.Write(newLine)
}
