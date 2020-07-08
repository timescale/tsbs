package main

import (
	"bufio"
	"crypto/md5"
	"github.com/google/uuid"
	"github.com/iznauy/tsbs/load"
	"github.com/pkg/errors"
	"strconv"
	"strings"
	"time"
)

type point struct {
	id        uuid.UUID
	timestamp int64
	value     float64
}

type insertion struct {
	Uuid   string          `json:"uuid"`
	Points [][]interface{} `json:"readings"`
}

type insertionBatch struct {
	insertions map[[16]byte]*insertion
	rows       uint64
	metrics    uint64
}

func (b *insertionBatch) Len() int {
	return int(b.rows)
}

func (b *insertionBatch) Append(item *load.Point) {
	p := item.Data.(*point)
	insert, ok := b.insertions[[16]byte(p.id)]
	if !ok {
		insert = &insertion{
			Uuid:   p.id.String(),
			Points: make([][]interface{}, 0, 16),
		}
		b.metrics += 1
	}
	insert.Points = append(insert.Points, []interface{}{p.timestamp, p.value})
	b.insertions[[16]byte(p.id)] = insert
	b.rows += 1
}

type factory struct{}

func (f *factory) New() load.Batch {
	return &insertionBatch{
		insertions: make(map[[16]byte]*insertion, 128),
		rows:       0,
		metrics:    0,
	}
}

type decoder struct {
	scanner *bufio.Scanner
}

func (d *decoder) Decode(_ *bufio.Reader) *load.Point {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil {
		return nil
	} else if !ok {
		fatal("scan error: %v", d.scanner.Err())
		return nil
	}

	parts := strings.Split(d.scanner.Text(), "\t")
	if len(parts) != 3 {
		fatal("incorrect point format, points must has three parts")
		return nil
	}
	prefix := parts[0]
	ts, err := parseTime(parts[1])
	if err != nil {
		fatal("cannot parse timestamp: %v", err)
		return nil
	}
	subKey, value, err := parseSubKeyAndValue(parts[2])
	if err != nil {
		fatal("cannot parse subkey and value: %v", err)
		return nil
	}
	key := md5.Sum([]byte(prefix + subKey))
	id, err := uuid.FromBytes(key[:])
	if err != nil {
		fatal("cannot generate uuid: %v", err)
	}
	return load.NewPoint(&point{
		id:        id,
		timestamp: ts.UnixNano(),
		value:     value,
	})
}

func parseTime(v string) (time.Time, error) {
	ts, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, ts), nil
}

func parseSubKeyAndValue(s string) (string, float64, error) {
	entry := strings.Split(s, "=")
	if len(entry) != 2 {
		fatal("incorrect point format, points field must has two parts")
		return "", 0.0, errors.New("incorrect point format, points field must has two parts")
	}
	value, err := strconv.ParseFloat(entry[1], 64)
	if err != nil {
		return "", 0.0, err
	}
	return entry[0], value, nil
}
