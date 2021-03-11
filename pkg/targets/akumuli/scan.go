package akumuli

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"sync"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
)

type fileDataSource struct {
	reader *bufio.Reader
}

func (d *fileDataSource) NextItem() data.LoadedPoint {
	hdr, err := d.reader.Peek(6)
	if err == io.EOF {
		return data.LoadedPoint{}
	}
	nbytes := binary.LittleEndian.Uint16(hdr[4:6])
	body := make([]byte, nbytes)
	_, err = io.ReadFull(d.reader, body)
	if err == io.EOF {
		return data.LoadedPoint{}
	}
	return data.NewLoadedPoint(body)
}

// Cassandra doesn't serialize headers, no need to read them
func (d *fileDataSource) Headers() *common.GeneratedDataHeaders { return nil }

type pointIndexer struct {
	nchan uint
}

func (i *pointIndexer) GetIndex(p data.LoadedPoint) uint {
	hdr := p.Data.([]byte)
	id := binary.LittleEndian.Uint32(hdr[0:4])
	return uint(id) % i.nchan
}

type batch struct {
	buf  *bytes.Buffer
	rows uint
}

func (b *batch) Len() uint {
	return b.rows
}

func (b *batch) Append(item data.LoadedPoint) {
	payload := item.Data.([]byte)
	b.buf.Write(payload)
	b.rows++
}

type factory struct {
	bufPool *sync.Pool
}

func (f *factory) New() targets.Batch {
	return &batch{buf: f.bufPool.Get().(*bytes.Buffer)}
}
