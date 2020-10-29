package main

import (
	"bufio"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/timescale/tsbs/load"
)

const tagsPrefix = "tags"

type decoder struct {
	scanner      *bufio.Scanner
	parsedHeader bool
}

func (d *decoder) Decode(_ *bufio.Reader) *load.Point {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return nil
	} else if !ok {
		log.Fatalf("scan error: %v", d.scanner.Err())
		return nil
	}

	// The first line is a CSV line of tags with the first element being "tags"
	parts := strings.SplitN(d.scanner.Text(), ",", 2) // prefix & then rest of line
	prefix := parts[0]
	if prefix != tagsPrefix {
		log.Fatalf("data file in invalid format; got %s expected %s", prefix, tagsPrefix)
		return nil
	}

	p := point{
		tags: decodeTags(parts[1]),
		vals: []string{},
	}

	// Scan again to get the data line
	ok = d.scanner.Scan()
	if !ok {
		log.Fatalf("scan error: %v", d.scanner.Err())
		return nil
	}

	// tableName, timestamp, cols...
	parts = strings.Split(d.scanner.Text(), ",")
	p.table = parts[0]

	// First field is time
	timeInt, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		panic(err)
	}
	p.ts = time.Unix(0, timeInt)

	// Next fields are column values
	for _, v := range parts[1:] {
		if v == "" {
			p.vals = append(p.vals, "NULL")
		} else {
			p.vals = append(p.vals, v)
		}
	}

	return load.NewPoint(&p)
}

func decodeTags(s string) []tag {
	tags := []tag{}

	parts := strings.Split(s, ",")
	for _, p := range parts {
		kv := strings.Split(p, "=")

		value := "'" + kv[1] + "'"
		_, err := strconv.ParseFloat(kv[1], 64)
		if err == nil {
			// No quotes around numbers
			value = kv[1]
		}

		tags = append(tags, tag{
			key:   kv[0],
			value: value,
		})
	}

	return tags
}

type tag struct {
	key   string
	value string
}

func (t tag) ToString() string {
	return fmt.Sprintf("%s=%s", t.key, t.value)
}

type point struct {
	table string
	ts    time.Time
	tags  []tag
	vals  []string
}

func (p point) TagsToString() string {
	var s strings.Builder
	s.WriteString("[")
	for i, t := range p.tags {
		if i > 0 {
			s.WriteString(",")
		}
		s.WriteString(t.ToString())
	}
	s.WriteString("]")
	return s.String()
}
