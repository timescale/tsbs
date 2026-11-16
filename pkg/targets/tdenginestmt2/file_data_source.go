package tdenginestmt2

import (
	"bufio"
	"encoding/binary"
	"io"
	"log"
	"os"
	"reflect"
	"unsafe"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
)

var fatal = log.Fatalf

func newFileDataSource(fileName string) targets.DataSource {
	br := GetBufferedReader(fileName)
	return &fileDataSource{br: br, exchange: make(chan struct{}, 1), exchangeStatus: make(chan int, 1)}
}

const (
	defaultReadSize = 4 << 20 // 4 MB
)

func GetBufferedReader(fileName string) *bufio.Reader {
	if len(fileName) == 0 {
		// Read from STDIN
		return bufio.NewReaderSize(os.Stdin, defaultReadSize)
	}
	// Read from specified file
	file, err := os.Open(fileName)
	if err != nil {
		fatal("cannot open file for read %s: %v", fileName, err)
		return nil
	}
	return bufio.NewReaderSize(file, defaultReadSize)
}

type fileDataSource struct {
	br             *bufio.Reader
	cache          []*[]byte
	cacheIndex     int
	cacheSize      int
	scale          int
	exchange       chan struct{}
	exchangeStatus chan int
	readDirect     bool
}

/*
 fixed header

| version(1 byte) | case (1 byte) | scale (4 bytes)

*/

func (d *fileDataSource) readHeader() (byte, uint32) {
	buf := make([]byte, 6)
	_, err := io.ReadFull(d.br, buf)
	if err != nil {
		fatal("cannot read header: %v", err)
	}
	if buf[0] != 1 {
		fatal("invalid version: %d", buf[0])
	}
	scale := binary.LittleEndian.Uint32(buf[2:])
	d.scale = int(scale)
	return buf[1], scale
}

func (d *fileDataSource) SetConfig(worker int, batchSize int, scale int) {
	globalSlicePool = NewSlicePool(worker, batchSize, scale)
}

func (d *fileDataSource) Headers() *common.GeneratedDataHeaders {
	return nil
}

var globalLoadedPoint = data.NewLoadedPoint(nil)

type SlicePool struct {
	smallPool chan []*[]byte
}

var globalSlicePool *SlicePool

func NewSlicePool(worker, batchSize, scale int) *SlicePool {
	totalItem := (worker*batchSize)*500 + scale*2 + 2
	arrayLen := worker * 2
	pool := make(chan []*[]byte, arrayLen*50)
	totalBytes := make([]*[]byte, totalItem)
	for i := 0; i < totalItem; i++ {
		bs := make([]byte, 0, 256)
		totalBytes[i] = &bs
	}
	bss := SplitBytes(totalBytes, arrayLen)
	for i := 0; i < arrayLen; i++ {
		pool <- bss[i]
	}
	return &SlicePool{smallPool: pool}
}

func (p *SlicePool) Get() []*[]byte {
	//return <-p.smallPool
	select {
	case b := <-p.smallPool:
		return b
	default:
		panic("no more slice")
	}
}

func (p *SlicePool) Put(b []*[]byte) {
	select {
	case p.smallPool <- b:
	default:
		return
	}
}

func (d *fileDataSource) NextItem() data.LoadedPoint {
	if d.cacheIndex == d.cacheSize {
		d.cache = globalSlicePool.Get()
		d.cacheSize = len(d.cache)
		d.cacheIndex = 0
	}
	u8length, err := d.br.ReadByte()
	if err != nil {
		if err == io.EOF {
			globalLoadedPoint.Data = nil
			return globalLoadedPoint
		}
		panic(err)
	}
	if u8length < 128 {
		length := int(u8length)
		ptr := d.cache[d.cacheIndex]
		(*reflect.SliceHeader)(unsafe.Pointer(ptr)).Len = length
		s := *ptr
		nn, err := d.br.Read(s)
		if err != nil {
			panic(err)
		}
		if nn != length {
			n := nn
			for n < length && err == nil {
				nn, err = d.br.Read(s[n:])
				n += nn
			}
		}
		d.cacheIndex++
		globalLoadedPoint.Data = ptr
		return globalLoadedPoint
	} else {
		tmp, err := d.br.ReadByte()
		if err != nil {
			panic(err)
		}
		u16Length := int(u8length&0x7f) + int(tmp<<7)
		ptr := d.cache[d.cacheIndex]
		(*reflect.SliceHeader)(unsafe.Pointer(ptr)).Len = u16Length
		s := *ptr
		nn, err := d.br.Read(s)
		if err != nil {
			panic(err)
		}
		if nn != u16Length {
			n := nn
			for n < u16Length && err == nil {
				nn, err = d.br.Read(s[n:])
				n += nn
			}
		}
		d.cacheIndex++
		globalLoadedPoint.Data = ptr
		return globalLoadedPoint
	}
}

func SplitBytes(arr []*[]byte, n int) [][]*[]byte {
	if n <= 0 {
		return nil
	}
	subArraySize := (len(arr) + n - 1) / n
	result := make([][]*[]byte, 0, n)
	for i := 0; i < len(arr); i += subArraySize {
		end := i + subArraySize
		if end > len(arr) {
			end = len(arr)
		}
		result = append(result, arr[i:end])
	}

	return result
}
