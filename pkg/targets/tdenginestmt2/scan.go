package tdenginestmt2

import (
	"bytes"
	"fmt"
	"unsafe"

	"github.com/spaolacci/murmur3"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/targets"
)

// Indexer is used to consistently send the same hostnames to the same worker
type Indexer struct {
	cache [3][]uint
}

func NewIndexer(prefix []byte, partitions int, hashEndGroups []uint32, useCase byte, scale uint32) (_ *Indexer, _ [3][]uint32, hostTableIndex [][]uint32, readingsTableIndex [][]uint32, diagnosticsTableIndex [][]uint32) {
	cache := [3][]uint{}
	buf := &bytes.Buffer{}
	var idx uint32
	switch useCase {
	case CpuCase:
		cache[SuperTableHost] = make([]uint, scale+1)
		hostTableIndex = make([][]uint32, partitions)
		//partitionIndex := make([]uint32, partitions)
		tableOffset := make([]uint32, scale+1)
		buf.Write(prefix)
		buf.WriteString("host_null")
		hash := murmur3.Sum32WithSeed(buf.Bytes(), 0x12345678)
		buf.Reset()
		for j := 0; j < partitions; j++ {
			if hash <= hashEndGroups[j] {
				cache[SuperTableHost][0] = uint(j)
				tableOffset[0] = uint32(len(hostTableIndex[j]))
				hostTableIndex[j] = append(hostTableIndex[j], 0)
				break
			}
		}
		tbPrefix := append(prefix, []byte("host_")...)
		for i := uint32(0); i < scale; i++ {
			buf.Write(tbPrefix)
			_, err := fmt.Fprintf(buf, "%d", i)
			if err != nil {
				panic(err)
			}
			hash = murmur3.Sum32WithSeed(buf.Bytes(), 0x12345678)
			buf.Reset()
			for j := 0; j < partitions; j++ {
				if hash <= hashEndGroups[j] {
					idx = i + 1
					cache[SuperTableHost][idx] = uint(j)
					tableOffset[idx] = uint32(len(hostTableIndex[j]))
					hostTableIndex[j] = append(hostTableIndex[j], idx)
					break
				}
			}
		}
		return &Indexer{
				cache: cache,
			},
			[3][]uint32{
				tableOffset,
			},
			hostTableIndex,
			nil,
			nil
	case IoTCase:
		cache[SuperTableReadings] = make([]uint, scale+1)
		readingsTableIndex = make([][]uint32, partitions)
		rTableOffset := make([]uint32, scale+1)
		buf.Write(prefix)
		buf.WriteString("r_truck_null")
		hash := murmur3.Sum32WithSeed(buf.Bytes(), 0x12345678)
		buf.Reset()
		for j := 0; j < partitions; j++ {
			if hash <= hashEndGroups[j] {
				cache[SuperTableReadings][0] = uint(j)
				rTableOffset[0] = uint32(len(readingsTableIndex[j]))
				readingsTableIndex[j] = append(readingsTableIndex[j], 0)
				break
			}
		}
		tbPrefix := append(prefix, []byte("r_truck_")...)
		for i := uint32(0); i < scale; i++ {
			buf.Write(tbPrefix)
			_, err := fmt.Fprintf(buf, "%d", i)
			if err != nil {
				panic(err)
			}
			hash := murmur3.Sum32WithSeed(buf.Bytes(), 0x12345678)
			buf.Reset()
			for j := 0; j < partitions; j++ {
				if hash <= hashEndGroups[j] {
					idx = i + 1
					cache[SuperTableReadings][idx] = uint(j)
					rTableOffset[idx] = uint32(len(readingsTableIndex[j]))
					readingsTableIndex[j] = append(readingsTableIndex[j], idx)
					break
				}
			}
		}

		cache[SuperTableDiagnostics] = make([]uint, scale+1)
		diagnosticsTableIndex = make([][]uint32, partitions)
		dTableOffset := make([]uint32, scale+1)
		buf.Write(prefix)
		buf.WriteString("d_truck_null")
		hash = murmur3.Sum32WithSeed(buf.Bytes(), 0x12345678)
		buf.Reset()
		for j := 0; j < partitions; j++ {
			if hash <= hashEndGroups[j] {
				cache[SuperTableDiagnostics][0] = uint(j)
				dTableOffset[0] = uint32(len(diagnosticsTableIndex[j]))
				diagnosticsTableIndex[j] = append(diagnosticsTableIndex[j], 0)
				break
			}
		}
		tbPrefix = append(prefix, []byte("d_truck_")...)
		for i := uint32(0); i < scale; i++ {
			buf.Write(tbPrefix)
			_, err := fmt.Fprintf(buf, "%d", i)
			if err != nil {
				panic(err)
			}
			hash := murmur3.Sum32WithSeed(buf.Bytes(), 0x12345678)
			buf.Reset()
			for j := 0; j < partitions; j++ {
				if hash <= hashEndGroups[j] {
					idx = i + 1
					cache[SuperTableDiagnostics][idx] = uint(j)
					dTableOffset[idx] = uint32(len(diagnosticsTableIndex[j]))
					diagnosticsTableIndex[j] = append(diagnosticsTableIndex[j], idx)
					break
				}
			}
		}

		return &Indexer{
				cache: cache,
			},
			[3][]uint32{
				nil,
				rTableOffset,
				dTableOffset,
			},
			nil,
			readingsTableIndex,
			diagnosticsTableIndex
	default:
		panic(fmt.Sprintf("invalid use case: %d", useCase))
	}
}

func (i *Indexer) GetIndex(item data.LoadedPoint) uint {
	p := *item.Data.(*[]byte)
	return i.cache[p[1]][*(*uint32)(unsafe.Pointer(&p[2]))]
}

type hypertableArr struct {
	data        []*[]byte
	createSql   []*[]byte
	totalMetric uint64
	cnt         uint
}

func (ha *hypertableArr) Len() uint {
	return ha.cnt
}

func (ha *hypertableArr) Append(item data.LoadedPoint) {
	p := item.Data.(*[]byte)
	s := *p
	_ = s[7]
	if s[0] == InsertData {
		if s[6] != 1 {
			ha.data = append(ha.data, p)
		}
		switch s[1] {
		case SuperTableHost:
			ha.totalMetric += 10
		case SuperTableReadings:
			ha.totalMetric += 7
		case SuperTableDiagnostics:
			ha.totalMetric += 3
		default:
			fatal("invalid table type:%d", s[1])
		}
		ha.cnt++
	} else {
		ha.createSql = append(ha.createSql, p)
	}
}

type BatchFactory struct {
	batchSize uint
}

func (b *BatchFactory) New() targets.Batch {
	return &hypertableArr{
		data: make([]*[]byte, 0, b.batchSize),
	}
}

func NewBatchFactory() targets.BatchFactory {
	return &BatchFactory{}
}
