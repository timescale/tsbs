package main

import (
	"bufio"
	"strings"

	"bitbucket.org/440-labs/tsbs/load"
)

// point is a single row of data keyed by which hypertable it belongs
type point struct {
	hypertable string
	row        *insertData
}

type hypertableArr struct {
	m   map[string][]*insertData
	cnt int
}

func (ha *hypertableArr) Len() int {
	return ha.cnt
}

func (ha *hypertableArr) Append(item *load.Point) {
	that := item.Data.(*point)
	k := that.hypertable
	ha.m[k] = append(ha.m[k], that.row)
	ha.cnt++
}

type factory struct{}

func (f *factory) New() load.Batch {
	return &hypertableArr{
		m:   map[string][]*insertData{},
		cnt: 0,
	}
}

type decoder struct {
	scanner *bufio.Scanner
}

const tagsPrefix = "tags"

func (d *decoder) Decode(_ *bufio.Reader) *load.Point {
	data := &insertData{}
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return nil
	} else if !ok {
		fatal("scan error: %v", d.scanner.Err())
	}

	// The first line is a CSV line of tags with the first element being "tags"
	parts := strings.SplitN(d.scanner.Text(), ",", 2) // prefix & then rest of line
	prefix := parts[0]
	if prefix != tagsPrefix {
		fatal("data file in invalid format; got %s expected %s", prefix, tagsPrefix)
		return nil
	}
	data.tags = parts[1]

	// Scan again to get the data line
	ok = d.scanner.Scan()
	if !ok {
		fatal("scan error: %v", d.scanner.Err())
		return nil
	}
	parts = strings.SplitN(d.scanner.Text(), ",", 2) // prefix & then rest of line
	prefix = parts[0]
	data.fields = parts[1]

	return load.NewPoint(&point{
		hypertable: prefix,
		row:        data,
	})
}
