package tdengine

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"sync"

	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/tdengine/async"
	"github.com/timescale/tsbs/pkg/targets/tdengine/commonpool"
)

type syncCSI struct {
	m sync.Map //table:ctx
}

const Size1M = 1 * 1024 * 1024

type Ctx struct {
	c      context.Context
	cancel context.CancelFunc
}

var globalSCI = &syncCSI{}

type processor struct {
	opts   *LoadingOptions
	dbName string
	sci    *syncCSI
	_db    *commonpool.Conn
	wg     *sync.WaitGroup
	buf    *bytes.Buffer
}

func newProcessor(opts *LoadingOptions, dbName string) *processor {
	return &processor{opts: opts, dbName: dbName, sci: globalSCI, wg: &sync.WaitGroup{}, buf: &bytes.Buffer{}}
}

func (p *processor) Init(_ int, doLoad, _ bool) {
	if !doLoad {
		return
	}
	p.buf.Grow(Size1M)
	var err error
	p._db, err = commonpool.GetConnection(p.opts.User, p.opts.Pass, p.opts.Host, p.opts.Port)
	if err != nil {
		panic(err)
	}
	err = async.GlobalAsync.TaosExecWithoutResult(p._db.TaosConnection, "use "+p.dbName)
	if err != nil {
		panic(err)
	}
}

func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
	batches := b.(*hypertableArr)
	rowCnt := uint64(0)
	metricCnt := batches.totalMetric
	if !doLoad {
		for _, sqls := range batches.m {
			rowCnt += uint64(len(sqls))
		}
		return metricCnt, rowCnt
	}
	p.buf.Reset()
	p.buf.WriteString("create table")
	for _, row := range batches.createSql {
		switch row.sqlType {
		case CreateSTable:
			c, cancel := context.WithCancel(context.Background())
			ctx := &Ctx{
				c:      c,
				cancel: cancel,
			}
			actual, _ := p.sci.m.LoadOrStore(row.superTable, ctx)
			err := async.GlobalAsync.TaosExecWithoutResult(p._db.TaosConnection, row.sql)
			if err != nil {
				fmt.Println(row.sql)
				panic(err)
			}
			GlobalTable.Store(row.subTable, nothing)
			actual.(*Ctx).cancel()
		case CreateSubTable:
			c, cancel := context.WithCancel(context.Background())
			ctx := &Ctx{
				c:      c,
				cancel: cancel,
			}
			actual, _ := p.sci.m.LoadOrStore(row.subTable, ctx)

			//check super table created
			_, ok := GlobalTable.Load(row.superTable)
			if !ok {
				v, ok := p.sci.m.Load(row.superTable)
				if ok {
					<-v.(*Ctx).c.Done()
					if p.buf.Len()+len(row.sql) > Size1M {
						sql := p.buf.String()
						err := async.GlobalAsync.TaosExecWithoutResult(p._db.TaosConnection, sql)
						if err != nil {
							fmt.Println(sql)
							panic(err)
						}
						p.buf.Reset()
						p.buf.WriteString("create table")
					}
					p.buf.WriteString(row.sql)
					GlobalTable.Store(row.subTable, nothing)
					actual.(*Ctx).cancel()
					continue
				}
				// wait for super table created
				superTableC, superTableCancel := context.WithCancel(context.Background())
				superTableCtx := &Ctx{
					c:      superTableC,
					cancel: superTableCancel,
				}
				superTableActual, _ := p.sci.m.LoadOrStore(row.superTable, superTableCtx)
				<-superTableActual.(*Ctx).c.Done()
			}
			if p.buf.Len()+len(row.sql) > Size1M {
				sql := p.buf.String()
				err := async.GlobalAsync.TaosExecWithoutResult(p._db.TaosConnection, sql)
				if err != nil {
					fmt.Println(sql)
					panic(err)
				}
				p.buf.Reset()
				p.buf.WriteString("create table")
			}
			p.buf.WriteString(row.sql)
			GlobalTable.Store(row.subTable, nothing)
			actual.(*Ctx).cancel()
		default:
			panic("impossible")
		}
	}
	if p.buf.Len() > 12 {
		sql := p.buf.String()
		err := async.GlobalAsync.TaosExecWithoutResult(p._db.TaosConnection, sql)
		if err != nil {
			fmt.Println(sql)
			panic(err)
		}
	}
	p.buf.Reset()
	p.wg.Add(len(batches.m))
	for tableName := range batches.m {
		tableName := tableName
		go func() {
			defer p.wg.Done()
			_, ok := GlobalTable.Load(tableName)
			if ok {
				return
			}
			v, ok := p.sci.m.Load(tableName)
			if ok {
				<-v.(*Ctx).c.Done()
				return
			}
			c, cancel := context.WithCancel(context.Background())
			ctx := &Ctx{
				c:      c,
				cancel: cancel,
			}
			actual, _ := p.sci.m.LoadOrStore(tableName, ctx)
			<-actual.(*Ctx).c.Done()
			return
		}()
	}
	p.wg.Wait()
	p.buf.WriteString("insert into ")
	for tableName, sqls := range batches.m {
		rowCnt += uint64(len(sqls))
		if p.buf.Len()+len(sqls[0])+len(tableName)+7 > Size1M {
			sql := p.buf.String()
			err := async.GlobalAsync.TaosExecWithoutResult(p._db.TaosConnection, sql)
			if err != nil {
				ioutil.WriteFile("wrongsql.txt", []byte(sql), 0755)
				fmt.Println(sql)
				panic(err)
			}
			p.buf.Reset()
			p.buf.WriteString("insert into ")
		}
		p.buf.WriteString(tableName)
		p.buf.WriteString(" values")
		for i := 0; i < len(sqls); i++ {
			if p.buf.Len()+len(sqls[i]) > Size1M {
				sql := p.buf.String()
				err := async.GlobalAsync.TaosExecWithoutResult(p._db.TaosConnection, sql)
				if err != nil {
					ioutil.WriteFile("wrongsql.txt", []byte(sql), 0755)
					fmt.Println(sql)
					panic(err)
				}
				p.buf.Reset()
				p.buf.WriteString("insert into ")
				p.buf.WriteString(tableName)
				p.buf.WriteString(" values")
			}
			p.buf.WriteString(sqls[i])
		}
	}
	if p.buf.Len() > 0 {
		sql := p.buf.String()
		err := async.GlobalAsync.TaosExecWithoutResult(p._db.TaosConnection, sql)
		if err != nil {
			fmt.Println(sql)
			panic(err)
		}
		p.buf.Reset()
	}

	batches.Reset()
	return metricCnt, rowCnt
}

func (p *processor) Close(doLoad bool) {
	if doLoad {
		p._db.Put()
	}
}
