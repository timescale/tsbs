package async

import (
	"database/sql/driver"
	"errors"
	"sync"
	"unsafe"

	"github.com/taosdata/driver-go/v3/common/parser"
	tErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/timescale/tsbs/pkg/targets/tdengine/thread"
)

var FetchRowError = errors.New("fetch row error")
var GlobalAsync *Async

type Async struct {
	HandlerPool *HandlerPool
}

func NewAsync(handlerPool *HandlerPool) *Async {
	return &Async{HandlerPool: handlerPool}
}

func (a *Async) TaosExec(taosConnect unsafe.Pointer, sql string, timeFormat parser.FormatTimeFunc) (*ExecResult, error) {
	handler := a.HandlerPool.Get()
	defer a.HandlerPool.Put(handler)
	result, err := a.TaosQuery(taosConnect, sql, handler)
	defer func() {
		if result != nil && result.Res != nil {
			thread.Lock()
			wrapper.TaosFreeResult(result.Res)
			thread.Unlock()
		}
	}()
	if err != nil {
		return nil, err
	}
	res := result.Res
	code := wrapper.TaosError(res)
	if code != int(tErrors.SUCCESS) {
		errStr := wrapper.TaosErrorStr(res)
		return nil, tErrors.NewError(code, errStr)
	}
	isUpdate := wrapper.TaosIsUpdateQuery(res)
	execResult := &ExecResult{}
	if isUpdate {
		affectRows := wrapper.TaosAffectedRows(res)
		execResult.AffectedRows = affectRows
		return execResult, nil
	}
	fieldsCount := wrapper.TaosNumFields(res)
	execResult.FieldCount = fieldsCount
	var rowsHeader *wrapper.RowsHeader
	rowsHeader, err = wrapper.ReadColumn(res, fieldsCount)
	if err != nil {
		return nil, err
	}
	execResult.Header = rowsHeader
	precision := wrapper.TaosResultPrecision(res)
	for {
		result, err = a.TaosFetchRawBlockA(res, handler)
		if err != nil {
			return nil, err
		}
		if result.N == 0 {
			return execResult, nil
		} else if result.N < 0 {
			errStr := wrapper.TaosErrorStr(result.Res)
			return nil, tErrors.NewError(result.N, errStr)
		} else {
			res = result.Res
			block := wrapper.TaosGetRawBlock(res)
			values := parser.ReadBlockWithTimeFormat(block, result.N, rowsHeader.ColTypes, precision, timeFormat)
			execResult.Data = append(execResult.Data, values...)
		}
	}
}

func (a *Async) TaosQuery(taosConnect unsafe.Pointer, sql string, handler *Handler) (*Result, error) {
	thread.Lock()
	wrapper.TaosQueryA(taosConnect, sql, handler.Handler)
	thread.Unlock()
	r := <-handler.Caller.QueryResult
	return r, nil
}

func (a *Async) TaosFetchRawBlockA(res unsafe.Pointer, handler *Handler) (*Result, error) {
	thread.Lock()
	wrapper.TaosFetchRawBlockA(res, handler.Handler)
	thread.Unlock()
	r := <-handler.Caller.FetchResult
	return r, nil
}

type ExecResult struct {
	AffectedRows int
	FieldCount   int
	Header       *wrapper.RowsHeader
	Data         [][]driver.Value
}

func (a *Async) TaosExecWithoutResult(taosConnect unsafe.Pointer, sql string) error {
	handler := a.HandlerPool.Get()
	defer a.HandlerPool.Put(handler)
	result, err := a.TaosQuery(taosConnect, sql, handler)
	defer func() {
		if result != nil && result.Res != nil {
			thread.Lock()
			wrapper.TaosFreeResult(result.Res)
			thread.Unlock()
		}
	}()
	if err != nil {
		return err
	}
	res := result.Res
	code := wrapper.TaosError(res)
	if code != int(tErrors.SUCCESS) {
		errStr := wrapper.TaosErrorStr(res)
		return tErrors.NewError(code, errStr)
	}
	return nil
}

var once sync.Once

func Init() {
	once.Do(func() {
		GlobalAsync = NewAsync(NewHandlerPool(10000))
	})
}
