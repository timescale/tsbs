package tdenginestmt2

/*
#cgo CFLAGS: -I/usr/include
#cgo linux LDFLAGS: -L/usr/lib -ltaos
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
extern void QueryCallback2(void *param,TAOS_RES *,int code);
void taos_query_a_wrapper2(TAOS *taos,const char *sql, void *param){
	return taos_query_a(taos,sql,QueryCallback2,param);
};
*/
import "C"
import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"

	taosCommon "github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/driver-go/v3/wrapper/cgo"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/tdengine"
	"github.com/timescale/tsbs/pkg/targets/tdengine/async"
	"github.com/timescale/tsbs/pkg/targets/tdengine/commonpool"
)

type Ctx struct {
	c      context.Context
	cancel context.CancelFunc
}

type processor struct {
	tableSlot                   [3][]uint32
	cBuffers                    [3]*bufferPointer
	tableNamePointerCache       [3][]unsafe.Pointer
	stmt2CHandle                [3]unsafe.Pointer
	stmt2CBHandle               [3]*async.Handler
	hostTableNameCBuffer        unsafe.Pointer
	readingsTableNameCBuffer    unsafe.Pointer
	diagnosticsTableNameCBuffer unsafe.Pointer

	hostSlot                [][][]byte
	readingsSlot            [][][]byte
	diagnosticsSlot         [][][]byte
	hostBatchIndexer        []uint32
	readingsBatchIndexer    []uint32
	diagnosticsBatchIndexer []uint32
	wg                      *sync.WaitGroup
	partitionTable          [3][][]uint32

	_db          *commonpool.Conn
	bufIndex     int
	cBufPosition unsafe.Pointer
	cBuf         unsafe.Pointer
	caller       *async.Caller
	hp           unsafe.Pointer
	handle       cgo.Handle

	hostExecCount        int32
	readingsExecCount    int32
	diagnosticsExecCount int32
	createWg             *sync.WaitGroup
	opts                 *tdengine.LoadingOptions
	dbName               string
	useCase              int
	batchSize            uint
	scale                uint32

	exitSign chan struct{}
	finishWg *sync.WaitGroup
}

type bufferPointer struct {
	bindVP     unsafe.Pointer
	tableNameP []unsafe.Pointer
	bindsP     [][]unsafe.Pointer
	colP       []unsafe.Pointer
	isNullP    []unsafe.Pointer
}

const Size1M = 1 << 20

func newProcessor(opts *tdengine.LoadingOptions, dbName string, batchSize uint, useCase byte, scale uint32, partitionTable [3][][]uint32, tableOffset [3][]uint32) *processor {
	p := &processor{
		tableSlot:      tableOffset,
		partitionTable: partitionTable,
		opts:           opts,
		dbName:         dbName,
		batchSize:      batchSize,
		wg:             &sync.WaitGroup{},
		createWg:       &sync.WaitGroup{},
		useCase:        int(useCase),
		scale:          scale,
		exitSign:       make(chan struct{}, 1),
		finishWg:       &sync.WaitGroup{},
	}
	return p
}

const (
	CpuHandleIndex         = 0
	ReadingsHandleIndex    = 1
	DiagnosticsHandleIndex = 2
)

func (p *processor) Init(id int, doLoad, _ bool) {
	if !doLoad {
		return
	}
	var err error
	p._db, err = commonpool.GetConnection(p.opts.User, p.opts.Pass, p.opts.Host, p.opts.Port)
	if err != nil {
		panic(err)
	}
	err = async.GlobalAsync.TaosExecWithoutResult(p._db.TaosConnection, "use "+p.dbName)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := recover(); err != nil {
			p.Close(true)
			panic(err)
		}
	}()
	p.caller = async.NewCaller()
	p.handle = cgo.NewHandle(p.caller)
	p.hp = p.handle.Pointer()
	p.cBuf = C.malloc(Size1M)
	createTable := []byte("create table")
	createTableP := unsafe.Pointer(&createTable[0])
	C.memcpy(p.cBuf, createTableP, C.size_t(12))
	p.cBufPosition = p.cBuf
	p.bufIndex = 0

	// max table name 23
	switch p.useCase {
	case CpuCase:
		// init stmt2
		handler := async.GlobalAsync.HandlerPool.Get()
		prepareSql := "insert into ? values(?,?,?,?,?,?,?,?,?,?,?)"
		stmt2Pointer := wrapper.TaosStmt2Init(p._db.TaosConnection, int64(1<<56|id), true, true, handler.Handler)
		if stmt2Pointer == nil {
			panic(fmt.Errorf("failed to create stmt2"))
		}
		code := wrapper.TaosStmt2Prepare(stmt2Pointer, prepareSql)
		if code != 0 {
			errMsg := wrapper.TaosStmt2Error(stmt2Pointer)
			panic(fmt.Errorf("failed to prepare stmt2: %s", errMsg))
		}

		// init table name c buffer
		hostTableIndex := p.partitionTable[SuperTableHost][id]
		p.tableNamePointerCache[SuperTableHost] = make([]unsafe.Pointer, len(hostTableIndex))
		p.tableNamePointerCache[SuperTableHost] = make([]unsafe.Pointer, len(hostTableIndex))
		p.hostTableNameCBuffer = C.calloc(C.size_t(24*(len(hostTableIndex))), 1)
		cursor := p.hostTableNameCBuffer
		prefix := []byte("host_")
		var tableNameBytes []byte
		for i := uint64(0); i < uint64(len(hostTableIndex)); i++ {
			if hostTableIndex[i] == 0 {
				tableNameNull := []byte("host_null")
				C.memcpy(cursor, unsafe.Pointer(&tableNameNull[0]), C.size_t(len(tableNameNull)))
			} else {
				tableNameBytes = strconv.AppendUint(prefix, uint64(hostTableIndex[i]-1), 10)
				C.memcpy(cursor, unsafe.Pointer(&tableNameBytes[0]), C.size_t(len(tableNameBytes)))
			}
			p.tableNamePointerCache[SuperTableHost][i] = cursor
			cursor = unsafe.Pointer(uintptr(cursor) + 24)
		}

		// crate stmt2 bind buffer
		cBuffer := allocBuffer(len(hostTableIndex), int(p.batchSize), []int{
			8,
			8,
			8,
			8,
			8,
			8,
			8,
			8,
			8,
			8,
			8,
		}, []int{
			taosCommon.TSDB_DATA_TYPE_TIMESTAMP,
			taosCommon.TSDB_DATA_TYPE_BIGINT,
			taosCommon.TSDB_DATA_TYPE_BIGINT,
			taosCommon.TSDB_DATA_TYPE_BIGINT,
			taosCommon.TSDB_DATA_TYPE_BIGINT,
			taosCommon.TSDB_DATA_TYPE_BIGINT,
			taosCommon.TSDB_DATA_TYPE_BIGINT,
			taosCommon.TSDB_DATA_TYPE_BIGINT,
			taosCommon.TSDB_DATA_TYPE_BIGINT,
			taosCommon.TSDB_DATA_TYPE_BIGINT,
			taosCommon.TSDB_DATA_TYPE_BIGINT,
		})
		p.cBuffers[CpuHandleIndex] = cBuffer
		p.stmt2CHandle[CpuHandleIndex] = stmt2Pointer
		p.stmt2CBHandle[CpuHandleIndex] = handler

		// init slot
		p.hostSlot = make([][][]byte, len(hostTableIndex))
		for i := 0; i < len(hostTableIndex); i++ {
			p.hostSlot[i] = make([][]byte, 0, 10)
		}
		p.hostBatchIndexer = make([]uint32, 0, p.batchSize)
		p.receiveHostExecResult()

	case IoTCase:
		// init stmt2
		handlerReading := async.GlobalAsync.HandlerPool.Get()
		prepareReadingSql := "insert into ? values(?,?,?,?,?,?,?,?)"
		stmt2ReadingPointer := wrapper.TaosStmt2Init(p._db.TaosConnection, int64(2<<56|id), true, true, handlerReading.Handler)
		if stmt2ReadingPointer == nil {
			panic(fmt.Errorf("failed to create stmt2"))
		}
		code := wrapper.TaosStmt2Prepare(stmt2ReadingPointer, prepareReadingSql)
		if code != 0 {
			errMsg := wrapper.TaosStmt2Error(stmt2ReadingPointer)
			panic(fmt.Errorf("failed to prepare stmt2: %s", errMsg))
		}

		handlerDiagnostics := async.GlobalAsync.HandlerPool.Get()
		prepareDiagnosticsSql := "insert into ? values(?,?,?,?)"
		stmt2DiagnosticsPointer := wrapper.TaosStmt2Init(p._db.TaosConnection, int64(3<<56|id), true, true, handlerDiagnostics.Handler)
		if stmt2DiagnosticsPointer == nil {
			panic(fmt.Errorf("failed to create stmt2"))
		}
		code = wrapper.TaosStmt2Prepare(stmt2DiagnosticsPointer, prepareDiagnosticsSql)
		if code != 0 {
			errMsg := wrapper.TaosStmt2Error(stmt2DiagnosticsPointer)
			panic(fmt.Errorf("failed to prepare stmt2: %s", errMsg))
		}

		// init table name c buffer
		readingsTableIndex := p.partitionTable[SuperTableReadings][id]
		p.tableNamePointerCache[SuperTableReadings] = make([]unsafe.Pointer, len(readingsTableIndex))
		p.readingsTableNameCBuffer = C.calloc(C.size_t(24*len(readingsTableIndex)), 1)
		cursor := p.readingsTableNameCBuffer
		prefix := []byte("r_truck_")
		var tableNameBytes []byte
		for i := uint64(0); i < uint64(len(readingsTableIndex)); i++ {
			if readingsTableIndex[i] == 0 {
				tableNameNull := []byte("r_truck_null")
				C.memcpy(cursor, unsafe.Pointer(&tableNameNull[0]), C.size_t(len(tableNameNull)))
			} else {
				tableNameBytes = strconv.AppendUint(prefix, uint64(readingsTableIndex[i]-1), 10)
				C.memcpy(cursor, unsafe.Pointer(&tableNameBytes[0]), C.size_t(len(tableNameBytes)))
			}
			p.tableNamePointerCache[SuperTableReadings][i] = cursor
			cursor = unsafe.Pointer(uintptr(cursor) + 24)
		}

		diagnosticsTableIndex := p.partitionTable[SuperTableDiagnostics][id]
		p.tableNamePointerCache[SuperTableDiagnostics] = make([]unsafe.Pointer, len(diagnosticsTableIndex))
		p.diagnosticsTableNameCBuffer = C.calloc(C.size_t(24*len(diagnosticsTableIndex)), 1)
		cursor = p.diagnosticsTableNameCBuffer
		prefix = []byte("d_truck_")
		for i := uint64(0); i < uint64(len(diagnosticsTableIndex)); i++ {
			if diagnosticsTableIndex[i] == 0 {
				tableNameNull := []byte("d_truck_null")
				C.memcpy(cursor, unsafe.Pointer(&tableNameNull[0]), C.size_t(len(tableNameNull)))
			} else {
				tableNameBytes = strconv.AppendUint(prefix, uint64(diagnosticsTableIndex[i]-1), 10)
				C.memcpy(cursor, unsafe.Pointer(&tableNameBytes[0]), C.size_t(len(tableNameBytes)))
			}
			p.tableNamePointerCache[SuperTableDiagnostics][i] = cursor
			cursor = unsafe.Pointer(uintptr(cursor) + 24)
		}

		// crate stmt2 bind buffer
		cBuffer := allocBuffer(len(readingsTableIndex), int(p.batchSize), []int{
			8,
			8,
			8,
			8,
			8,
			8,
			8,
			8,
		}, []int{
			taosCommon.TSDB_DATA_TYPE_TIMESTAMP,
			taosCommon.TSDB_DATA_TYPE_DOUBLE,
			taosCommon.TSDB_DATA_TYPE_DOUBLE,
			taosCommon.TSDB_DATA_TYPE_DOUBLE,
			taosCommon.TSDB_DATA_TYPE_DOUBLE,
			taosCommon.TSDB_DATA_TYPE_DOUBLE,
			taosCommon.TSDB_DATA_TYPE_DOUBLE,
			taosCommon.TSDB_DATA_TYPE_DOUBLE,
		})
		p.cBuffers[ReadingsHandleIndex] = cBuffer
		p.stmt2CHandle[ReadingsHandleIndex] = stmt2ReadingPointer
		p.stmt2CBHandle[ReadingsHandleIndex] = handlerReading

		cBuffer = allocBuffer(len(diagnosticsTableIndex), int(p.batchSize), []int{
			8,
			8,
			8,
			8,
		}, []int{
			taosCommon.TSDB_DATA_TYPE_TIMESTAMP,
			taosCommon.TSDB_DATA_TYPE_DOUBLE,
			taosCommon.TSDB_DATA_TYPE_DOUBLE,
			taosCommon.TSDB_DATA_TYPE_BIGINT,
		})
		p.cBuffers[DiagnosticsHandleIndex] = cBuffer
		p.stmt2CHandle[DiagnosticsHandleIndex] = stmt2DiagnosticsPointer
		p.stmt2CBHandle[DiagnosticsHandleIndex] = handlerDiagnostics

		// init slot
		p.readingsSlot = make([][][]byte, len(readingsTableIndex))
		for i := 0; i < len(readingsTableIndex); i++ {
			p.readingsSlot[i] = make([][]byte, 0, 10)
		}
		p.readingsBatchIndexer = make([]uint32, 0, p.batchSize)

		p.diagnosticsSlot = make([][][]byte, len(diagnosticsTableIndex))
		for i := 0; i < len(diagnosticsTableIndex); i++ {
			p.diagnosticsSlot[i] = make([][]byte, 0, 10)
		}
		p.diagnosticsBatchIndexer = make([]uint32, 0, p.batchSize)

		p.receiveReadingsExecResult()
		p.receiveDiagnosticsExecResult()
	}
}

func (p *processor) receiveHostExecResult() {
	p.finishWg.Add(1)
	go func() {
		defer p.finishWg.Done()
		stmtHandler := p.stmt2CHandle[CpuHandleIndex]
		handle := p.stmt2CBHandle[CpuHandleIndex]
		exitSign := p.exitSign
		for {
			select {
			case result := <-handle.Caller.ExecResult:
				if result.Code != 0 {
					errStr := wrapper.TaosStmt2Error(stmtHandler)
					panic(fmt.Errorf("failed to exec stmt2: %d:%s", result.Code, errStr))
				}
				atomic.AddInt32(&p.hostExecCount, -1)
			case <-exitSign:
				c := atomic.LoadInt32(&p.hostExecCount)
				if c == 0 {
					return
				}
				for i := int32(0); i < c; i++ {
					result := <-handle.Caller.ExecResult
					if result.Code != 0 {
						errStr := wrapper.TaosStmt2Error(stmtHandler)
						panic(fmt.Errorf("failed to exec stmt2: %d:%s", result.Code, errStr))
					}
				}
				return
			}
		}
	}()
}

func (p *processor) receiveReadingsExecResult() {
	p.finishWg.Add(1)
	go func() {
		defer p.finishWg.Done()
		stmtHandler := p.stmt2CHandle[ReadingsHandleIndex]
		handle := p.stmt2CBHandle[ReadingsHandleIndex]
		exitSign := p.exitSign
		for {
			select {
			case result := <-handle.Caller.ExecResult:
				if result.Code != 0 {
					errStr := wrapper.TaosStmt2Error(stmtHandler)
					panic(fmt.Errorf("failed to exec stmt2: %d:%s", result.Code, errStr))
				}
				atomic.AddInt32(&p.readingsExecCount, -1)
			case <-exitSign:
				c := atomic.LoadInt32(&p.readingsExecCount)
				if c == 0 {
					return
				}
				for i := int32(0); i < c; i++ {
					result := <-handle.Caller.ExecResult
					if result.Code != 0 {
						errStr := wrapper.TaosStmt2Error(stmtHandler)
						panic(fmt.Errorf("failed to exec stmt2: %d:%s", result.Code, errStr))
					}
				}
				return
			}
		}
	}()
}

func (p *processor) receiveDiagnosticsExecResult() {
	p.finishWg.Add(1)
	go func() {
		defer p.finishWg.Done()
		stmtHandler := p.stmt2CHandle[DiagnosticsHandleIndex]
		handle := p.stmt2CBHandle[DiagnosticsHandleIndex]
		exitSign := p.exitSign
		for {
			select {
			case result := <-handle.Caller.ExecResult:
				if result.Code != 0 {
					errStr := wrapper.TaosStmt2Error(stmtHandler)
					panic(fmt.Errorf("failed to exec stmt2: %d:%s", result.Code, errStr))
				}
				atomic.AddInt32(&p.diagnosticsExecCount, -1)
			case <-exitSign:
				c := atomic.LoadInt32(&p.diagnosticsExecCount)
				if c == 0 {
					return
				}
				for i := int32(0); i < c; i++ {
					result := <-handle.Caller.ExecResult
					if result.Code != 0 {
						errStr := wrapper.TaosStmt2Error(stmtHandler)
						panic(fmt.Errorf("failed to exec stmt2: %d:%s", result.Code, errStr))
					}
				}
				return
			}
		}
	}()
}

//typedef struct TAOS_STMT2_BINDV {
//  int               count;
//  char            **tbnames;
//  TAOS_STMT2_BIND **tags;
//  TAOS_STMT2_BIND **bind_cols;
//} TAOS_STMT2_BINDV;

// typedef struct TAOS_STMT2_BIND {
//  int      buffer_type;
//  void    *buffer;
//  int32_t *length;
//  char    *is_null;
//  int      num;
//} TAOS_STMT2_BIND;

const BindVSize = uintptr(C.sizeof_struct_TAOS_STMT2_BINDV)
const BindSize = uintptr(C.sizeof_struct_TAOS_STMT2_BIND)

func allocBuffer(partitionTables int, batchSize int, colSize []int, colTypes []int) *bufferPointer {
	colCount := len(colSize)
	bufferSize := 0
	for i := 0; i < colCount; i++ {
		bufferSize += colSize[i]
	}
	bindTables := partitionTables
	if bindTables > batchSize {
		bindTables = batchSize
	}
	maxRow := batchSize
	/*

		| TAOS_STMT2_BINDV      |
		| tbnames pointer 1     | tbnames pointer 2     | tbnames pointer 3     | tbnames pointer ...   | maxTable
		| bind_cols pointer 1   | bind_cols pointer 2   | bind_cols pointer 3   | bind_cols pointer ... | maxTable * colCount
		| TAOS_STMT2_BIND 1     | TAOS_STMT2_BIND 2     | TAOS_STMT2_BIND 3     | TAOS_STMT2_BIND ...   | maxTable * colCount
	*/

	// bindv + tbname pointer + bindcols pointer + bind struct pointer + bind structs
	fixedSize :=
		// TAOS_STMT2_BINDV
		BindVSize +
			// table name pointer
			wrapper.PointerSize*uintptr(bindTables) +
			// bind_cols pointer
			wrapper.PointerSize*uintptr(bindTables*colCount) +
			// bind_cols TAOS_STMT2_BIND
			BindSize*uintptr(bindTables*colCount)

	dataSize := uintptr(bufferSize*maxRow) + 1
	isNullSize := uintptr(maxRow*colCount) + 1
	totalSize := fixedSize + dataSize + isNullSize
	_ = totalSize
	bindVPointer := unsafe.Pointer(C.calloc(C.size_t(fixedSize), 1))
	dataPointer := unsafe.Pointer(C.calloc(C.size_t(dataSize), 1))
	isNullPointer := unsafe.Pointer(C.calloc(C.size_t(isNullSize), 1))

	// bindV
	bindV := (*C.TAOS_STMT2_BINDV)(bindVPointer)
	// tableName
	tableNamesPointer := unsafe.Pointer(uintptr(bindVPointer) + BindVSize)
	bindV.tbnames = (**C.char)(tableNamesPointer)

	// bind_cols pointer
	bindStructsPointerPointer := unsafe.Pointer(uintptr(tableNamesPointer) + uintptr(bindTables)*wrapper.PointerSize)
	bindV.bind_cols = (**C.TAOS_STMT2_BIND)(bindStructsPointerPointer)

	// TAOS_STMT2_BIND
	bindStructsPointer := unsafe.Pointer(uintptr(bindStructsPointerPointer) + uintptr(bindTables*colCount)*wrapper.PointerSize)

	tableNamesPointers := make([]unsafe.Pointer, bindTables)
	bindPointers := make([][]unsafe.Pointer, bindTables)
	var bind *C.TAOS_STMT2_BIND
	for tableIndex := 0; tableIndex < bindTables; tableIndex++ {
		// table name pointer
		tableNamesPointers[tableIndex] = tableNamesPointer
		tableNamesPointer = unsafe.Pointer(uintptr(tableNamesPointer) + wrapper.PointerSize)
		bindPointers[tableIndex] = make([]unsafe.Pointer, colCount)
		// set bind struct pointer
		*(**C.TAOS_STMT2_BIND)(unsafe.Pointer(uintptr(bindStructsPointerPointer) + uintptr(tableIndex)*wrapper.PointerSize)) = (*C.TAOS_STMT2_BIND)(bindStructsPointer)

		for colIndex := 0; colIndex < colCount; colIndex++ {
			// bind struct pointer
			bindPointers[tableIndex][colIndex] = bindStructsPointer
			// set col type
			bind = (*C.TAOS_STMT2_BIND)(bindStructsPointer)
			bind.buffer_type = (C.int)(colTypes[colIndex])
			bindStructsPointer = unsafe.Pointer(uintptr(bindStructsPointer) + BindSize)
		}
	}

	tableColDataPointers := make([]unsafe.Pointer, colCount)
	isNullPointers := make([]unsafe.Pointer, colCount)
	for colIndex := 0; colIndex < colCount; colIndex++ {
		tableColDataPointers[colIndex] = dataPointer
		dataPointer = unsafe.Pointer(uintptr(dataPointer) + uintptr(colSize[colIndex]*maxRow))
		isNullPointers[colIndex] = isNullPointer
		isNullPointer = unsafe.Pointer(uintptr(isNullPointer) + uintptr(maxRow))
	}

	buffer := &bufferPointer{
		bindVP:     bindVPointer,
		tableNameP: tableNamesPointers,
		bindsP:     bindPointers,
		colP:       tableColDataPointers,
		isNullP:    isNullPointers,
	}
	return buffer
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
	batches := b.(*hypertableArr)
	metricCnt := batches.totalMetric
	if !doLoad {
		return metricCnt, uint64(batches.cnt)
	}
	rowCount = uint64(batches.cnt)
	if len(batches.createSql) > 0 {
		p.createWg.Add(1)
		go func() {
			sqls := batches.createSql
			p.cBufPosition = unsafe.Pointer(uintptr(p.cBuf) + 12)
			p.bufIndex = 12
			for i := range sqls {
				sh := (*reflect.SliceHeader)(unsafe.Pointer(sqls[i]))
				createSize := sh.Len - 6
				if p.bufIndex+createSize >= Size1M {
					*(*C.char)(p.cBufPosition) = C.char(0)
					C.taos_query_a_wrapper2(p._db.TaosConnection, (*C.char)(p.cBuf), p.hp)
					res := <-p.caller.QueryResult
					if res.N != 0 {
						panic(wrapper.TaosErrorStr(res.Res))
					}
					go func(result unsafe.Pointer) {
						C.taos_free_result(result)
					}(res.Res)
					p.cBufPosition = unsafe.Pointer(uintptr(p.cBuf) + 12)
					p.bufIndex = 12
				}
				C.memcpy(p.cBufPosition, unsafe.Pointer(sh.Data+6), C.size_t(createSize))
				p.cBufPosition = unsafe.Pointer(uintptr(p.cBufPosition) + uintptr(createSize))
				p.bufIndex += createSize
			}
			if p.bufIndex > 12 {
				*(*C.char)(p.cBufPosition) = C.char(0)
				C.taos_query_a_wrapper2(p._db.TaosConnection, (*C.char)(p.cBuf), p.hp)
				res := <-p.caller.QueryResult
				if res.N != 0 {
					panic(wrapper.TaosErrorStr(res.Res))
				}
				go func(result unsafe.Pointer) {
					C.taos_free_result(result)
				}(res.Res)
			}
			p.createWg.Done()
		}()
	}

	switch p.useCase {
	case CpuCase:
		if len(batches.data) > 0 {
			var bind *C.TAOS_STMT2_BIND
			hostTableIndex := 0
			hostCBuffers := p.cBuffers[CpuHandleIndex]
			var nullByte byte
			var currentRowData []byte
			var dataPointer unsafe.Pointer
			currentIsNullPointer := [11]unsafe.Pointer{
				hostCBuffers.isNullP[0],
				hostCBuffers.isNullP[1],
				hostCBuffers.isNullP[2],
				hostCBuffers.isNullP[3],
				hostCBuffers.isNullP[4],
				hostCBuffers.isNullP[5],
				hostCBuffers.isNullP[6],
				hostCBuffers.isNullP[7],
				hostCBuffers.isNullP[8],
				hostCBuffers.isNullP[9],
				hostCBuffers.isNullP[10],
			}
			currentDataPointer := [11]unsafe.Pointer{
				hostCBuffers.colP[0],
				hostCBuffers.colP[1],
				hostCBuffers.colP[2],
				hostCBuffers.colP[3],
				hostCBuffers.colP[4],
				hostCBuffers.colP[5],
				hostCBuffers.colP[6],
				hostCBuffers.colP[7],
				hostCBuffers.colP[8],
				hostCBuffers.colP[9],
				hostCBuffers.colP[10],
			}
			for i := 0; i < len(batches.data); i++ {
				row := *(batches.data[i])
				slotID := p.tableSlot[SuperTableHost][*(*uint32)(unsafe.Pointer(&row[2]))]
				p.hostBatchIndexer = append(p.hostBatchIndexer, slotID)
				p.hostSlot[slotID] = append(p.hostSlot[slotID], row)
			}
			for _, slotID := range p.hostBatchIndexer {
				rowData := p.hostSlot[slotID]
				if len(rowData) == 0 {
					continue
				}
				p.hostSlot[slotID] = p.hostSlot[slotID][:0]
				rowLen := len(rowData)
				for i := 0; i < len(rowData); i++ {
					tmp := rowData[i]
					currentRowData = tmp[7:len(tmp):len(tmp)]
					nullByte = currentRowData[0]
					dataPointer = unsafe.Pointer(&currentRowData[2])

					*(*C.int64_t)(currentDataPointer[0]) = *(*C.int64_t)(dataPointer)

					currentDataPointer[0] = unsafe.Pointer(uintptr(currentDataPointer[0]) + 8)
					dataPointer = unsafe.Pointer(uintptr(dataPointer) + 8)

					// set col data and is_null
					for colIndex := 1; colIndex < 11; colIndex++ {
						if colIndex == 8 {
							nullByte = currentRowData[1]
						}
						isNullP := currentIsNullPointer[colIndex]
						dataP := currentDataPointer[colIndex]
						if nullByte&(1<<(7-(colIndex&7))) != 0 {
							*(*C.char)(currentIsNullPointer[colIndex]) = C.char(0)
						} else {
							*(*C.char)(isNullP) = C.char(0)
							*(*C.int64_t)(dataP) = *(*C.int64_t)(dataPointer)
						}

						currentIsNullPointer[colIndex] = unsafe.Pointer(uintptr(isNullP) + 1)
						currentDataPointer[colIndex] = unsafe.Pointer(uintptr(dataP) + 8)
						dataPointer = unsafe.Pointer(uintptr(dataPointer) + 8)
					}

					// first row, set buffer and is_null pointer
					if i == 0 {
						for colIndex := 0; colIndex < 11; colIndex++ {
							bind = (*C.TAOS_STMT2_BIND)(hostCBuffers.bindsP[hostTableIndex][colIndex])
							bind.buffer = unsafe.Pointer(uintptr(currentDataPointer[colIndex]) - 8)
							bind.is_null = (*C.char)(unsafe.Pointer(uintptr(currentIsNullPointer[colIndex]) - 1))
							bind.num = (C.int)(rowLen)
						}
					}
				}
				*(**C.char)(hostCBuffers.tableNameP[hostTableIndex]) = (*C.char)(p.tableNamePointerCache[SuperTableHost][slotID])
				hostTableIndex += 1
			}
			p.hostBatchIndexer = p.hostBatchIndexer[:0]

			// stmt2 bind
			bindv := (*C.TAOS_STMT2_BINDV)(hostCBuffers.bindVP)
			bindv.count = C.int(hostTableIndex)
			handler := p.stmt2CHandle[CpuHandleIndex]
			if len(batches.createSql) > 0 {
				p.createWg.Wait()
			}
			code := int(C.taos_stmt2_bind_param(handler, bindv, C.int32_t(-1)))
			if code != 0 {
				errStr := wrapper.TaosStmt2Error(handler)
				panic(fmt.Errorf("failed to bind param stmt2: %d:%s", code, errStr))
			}
			code = wrapper.TaosStmt2Exec(handler)
			if code != 0 {
				errStr := wrapper.TaosStmt2Error(handler)
				panic(fmt.Errorf("failed to exec stmt2: %d:%s", code, errStr))
			}
			atomic.AddInt32(&p.hostExecCount, 1)
		}

	case IoTCase:
		var row []byte
		for i := 0; i < len(batches.data); i++ {
			row = *batches.data[i]
			if row[1] == SuperTableReadings {
				slotID := p.tableSlot[SuperTableReadings][*(*uint32)(unsafe.Pointer(&row[2]))]
				p.readingsBatchIndexer = append(p.readingsBatchIndexer, slotID)
				p.readingsSlot[slotID] = append(p.readingsSlot[slotID], row)
			} else {
				slotID := p.tableSlot[SuperTableDiagnostics][*(*uint32)(unsafe.Pointer(&row[2]))]
				p.diagnosticsBatchIndexer = append(p.diagnosticsBatchIndexer, slotID)
				p.diagnosticsSlot[slotID] = append(p.diagnosticsSlot[slotID], row)
			}
		}
		p.wg.Add(2)
		go func() {
			if len(p.readingsBatchIndexer) > 0 {
				var bind *C.TAOS_STMT2_BIND
				readingTableIndex := 0
				readingCBuffers := p.cBuffers[ReadingsHandleIndex]
				var nullByte byte
				var currentRowData []byte
				var dataPointer unsafe.Pointer
				currentIsNullPointer := [8]unsafe.Pointer{
					readingCBuffers.isNullP[0],
					readingCBuffers.isNullP[1],
					readingCBuffers.isNullP[2],
					readingCBuffers.isNullP[3],
					readingCBuffers.isNullP[4],
					readingCBuffers.isNullP[5],
					readingCBuffers.isNullP[6],
					readingCBuffers.isNullP[7],
				}
				currentDataPointer := [8]unsafe.Pointer{
					readingCBuffers.colP[0],
					readingCBuffers.colP[1],
					readingCBuffers.colP[2],
					readingCBuffers.colP[3],
					readingCBuffers.colP[4],
					readingCBuffers.colP[5],
					readingCBuffers.colP[6],
					readingCBuffers.colP[7],
				}

				for _, slotID := range p.readingsBatchIndexer {
					rowData := p.readingsSlot[slotID]
					if len(rowData) == 0 {
						continue
					}
					p.readingsSlot[slotID] = p.readingsSlot[slotID][:0]
					rowLen := len(rowData)
					for i := 0; i < len(rowData); i++ {
						tmp := rowData[i]
						currentRowData = tmp[7:len(tmp):len(tmp)]
						nullByte = currentRowData[0]
						dataPointer = unsafe.Pointer(&currentRowData[1])

						*(*C.int64_t)(currentDataPointer[0]) = *(*C.int64_t)(dataPointer)

						currentDataPointer[0] = unsafe.Pointer(uintptr(currentDataPointer[0]) + 8)
						dataPointer = unsafe.Pointer(uintptr(dataPointer) + 8)

						for colIndex := 1; colIndex < 8; colIndex++ {
							if nullByte&(1<<(7-colIndex)) != 0 {
								*(*C.char)(currentIsNullPointer[colIndex]) = C.char(1)
							} else {
								*(*C.char)(currentIsNullPointer[colIndex]) = C.char(0)
								*(*C.int64_t)(currentDataPointer[colIndex]) = *(*C.int64_t)(dataPointer)
							}

							currentIsNullPointer[colIndex] = unsafe.Pointer(uintptr(currentIsNullPointer[colIndex]) + 1)
							currentDataPointer[colIndex] = unsafe.Pointer(uintptr(currentDataPointer[colIndex]) + 8)
							dataPointer = unsafe.Pointer(uintptr(dataPointer) + 8)
						}

						if i == 0 {
							// first row set buffer and is_null pointer
							for colIndex := 0; colIndex < 8; colIndex++ {
								bind = (*C.TAOS_STMT2_BIND)(readingCBuffers.bindsP[readingTableIndex][colIndex])
								bind.buffer = unsafe.Pointer(uintptr(currentDataPointer[colIndex]) - 8)
								bind.is_null = (*C.char)(unsafe.Pointer(uintptr(currentIsNullPointer[colIndex]) - 1))
								bind.num = (C.int)(rowLen)
							}
						}
					}
					*(**C.char)(readingCBuffers.tableNameP[readingTableIndex]) = (*C.char)(p.tableNamePointerCache[SuperTableReadings][slotID])
					readingTableIndex += 1
				}
				p.readingsBatchIndexer = p.readingsBatchIndexer[:0]
				bindv := (*C.TAOS_STMT2_BINDV)(readingCBuffers.bindVP)
				bindv.count = C.int(readingTableIndex)
				handler := p.stmt2CHandle[ReadingsHandleIndex]

				if len(batches.createSql) > 0 {
					p.createWg.Wait()
				}
				code := int(C.taos_stmt2_bind_param(handler, bindv, C.int32_t(-1)))
				if code != 0 {
					errStr := wrapper.TaosStmt2Error(handler)
					panic(fmt.Errorf("failed to bind param stmt2: %d:%s", code, errStr))
				}
				code = wrapper.TaosStmt2Exec(handler)
				if code != 0 {
					errStr := wrapper.TaosStmt2Error(handler)
					panic(fmt.Errorf("failed to exec stmt2: %d:%s", code, errStr))
				}
				atomic.AddInt32(&p.readingsExecCount, 1)
			}
			p.wg.Done()
		}()
		go func() {
			if len(p.diagnosticsBatchIndexer) > 0 {
				var bind *C.TAOS_STMT2_BIND
				diagnosticsTableIndex := 0
				diagnosticsCBuffers := p.cBuffers[DiagnosticsHandleIndex]
				var nullByte byte
				var currentRowData []byte
				var dataPointer unsafe.Pointer
				currentIsNullPointer := [4]unsafe.Pointer{
					diagnosticsCBuffers.isNullP[0],
					diagnosticsCBuffers.isNullP[1],
					diagnosticsCBuffers.isNullP[2],
					diagnosticsCBuffers.isNullP[3],
				}
				currentDataPointer := [4]unsafe.Pointer{
					diagnosticsCBuffers.colP[0],
					diagnosticsCBuffers.colP[1],
					diagnosticsCBuffers.colP[2],
					diagnosticsCBuffers.colP[3],
				}

				for _, slotID := range p.diagnosticsBatchIndexer {
					rowData := p.diagnosticsSlot[slotID]
					if len(rowData) == 0 {
						continue
					}
					p.diagnosticsSlot[slotID] = p.diagnosticsSlot[slotID][:0]
					rowLen := len(rowData)
					for i := 0; i < len(rowData); i++ {
						tmp := rowData[i]
						currentRowData = tmp[7:len(tmp):len(tmp)]
						nullByte = currentRowData[0]
						dataPointer = unsafe.Pointer(&currentRowData[1])

						*(*C.int64_t)(currentDataPointer[0]) = *(*C.int64_t)(dataPointer)

						currentDataPointer[0] = unsafe.Pointer(uintptr(currentDataPointer[0]) + 8)
						dataPointer = unsafe.Pointer(uintptr(dataPointer) + 8)

						// col1
						if nullByte&(1<<6) != 0 {
							*(*C.char)(currentIsNullPointer[1]) = C.char(1)
						} else {
							*(*C.char)(currentIsNullPointer[1]) = C.char(0)
							*(*C.double)(currentDataPointer[1]) = *(*C.double)(dataPointer)
						}
						currentIsNullPointer[1] = unsafe.Pointer(uintptr(currentIsNullPointer[1]) + 1)
						currentDataPointer[1] = unsafe.Pointer(uintptr(currentDataPointer[1]) + 8)
						dataPointer = unsafe.Pointer(uintptr(dataPointer) + 8)

						// col2
						if nullByte&(1<<5) != 0 {
							*(*C.char)(currentIsNullPointer[2]) = C.char(1)
						} else {
							*(*C.char)(currentIsNullPointer[2]) = C.char(0)
							*(*C.double)(currentDataPointer[2]) = *(*C.double)(dataPointer)
						}
						currentIsNullPointer[2] = unsafe.Pointer(uintptr(currentIsNullPointer[2]) + 1)
						currentDataPointer[2] = unsafe.Pointer(uintptr(currentDataPointer[2]) + 8)
						dataPointer = unsafe.Pointer(uintptr(dataPointer) + 8)

						// col3
						if nullByte&(1<<4) != 0 {
							*(*C.char)(currentIsNullPointer[3]) = C.char(1)
						} else {
							*(*C.char)(currentIsNullPointer[3]) = C.char(0)
							*(*C.int64_t)(currentDataPointer[3]) = *(*C.int64_t)(dataPointer)
						}
						currentIsNullPointer[3] = unsafe.Pointer(uintptr(currentIsNullPointer[3]) + 1)
						currentDataPointer[3] = unsafe.Pointer(uintptr(currentDataPointer[3]) + 8)
						if i == 0 {
							// first row set buffer and is_null pointer
							for colIndex := 0; colIndex < 4; colIndex++ {
								bind = (*C.TAOS_STMT2_BIND)(diagnosticsCBuffers.bindsP[diagnosticsTableIndex][colIndex])
								bind.buffer = unsafe.Pointer(uintptr(currentDataPointer[colIndex]) - 8)
								bind.is_null = (*C.char)(unsafe.Pointer(uintptr(currentIsNullPointer[colIndex]) - 1))
								bind.num = (C.int)(rowLen)
							}
						}
					}
					*(**C.char)(diagnosticsCBuffers.tableNameP[diagnosticsTableIndex]) = (*C.char)(p.tableNamePointerCache[SuperTableDiagnostics][slotID])
					diagnosticsTableIndex += 1
				}
				p.diagnosticsBatchIndexer = p.diagnosticsBatchIndexer[:0]
				bindv := (*C.TAOS_STMT2_BINDV)(diagnosticsCBuffers.bindVP)
				bindv.count = C.int(diagnosticsTableIndex)
				handler := p.stmt2CHandle[DiagnosticsHandleIndex]
				if len(batches.createSql) > 0 {
					p.createWg.Wait()
				}
				code := int(C.taos_stmt2_bind_param(handler, bindv, C.int32_t(-1)))
				if code != 0 {
					errStr := wrapper.TaosStmt2Error(handler)
					panic(fmt.Errorf("failed to bind param stmt2: %d:%s", code, errStr))
				}
				code = wrapper.TaosStmt2Exec(handler)
				if code != 0 {
					errStr := wrapper.TaosStmt2Error(handler)
					panic(fmt.Errorf("failed to exec stmt2: %d:%s", code, errStr))
				}
				atomic.AddInt32(&p.diagnosticsExecCount, 1)
			}

			p.wg.Done()
		}()
		p.wg.Wait()
	}
	go func() {
		globalSlicePool.Put(batches.data)
	}()
	return metricCnt, rowCount
}

func (p *processor) Close(doLoad bool) {
	if doLoad {
		close(p.exitSign)
		p.finishWg.Wait()
	}
}
