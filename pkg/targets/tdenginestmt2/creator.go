package tdenginestmt2

import (
	"github.com/timescale/tsbs/pkg/targets/tdengine"
	"github.com/timescale/tsbs/pkg/targets/tdengine/async"
)

type DbCreator struct {
	tdengine.DbCreator
	useCase byte
	ds      *fileDataSource
}

func (d *DbCreator) PostCreateDB(dbName string) error {
	err := async.GlobalAsync.TaosExecWithoutResult(d.Db.TaosConnection, "use "+dbName)
	if err != nil {
		return err
	}
	switch d.useCase {
	case CpuCase:
		err = async.GlobalAsync.TaosExecWithoutResult(d.Db.TaosConnection, CpuSql)
		if err != nil {
			return err
		}
	case IoTCase:
		err = async.GlobalAsync.TaosExecWithoutResult(d.Db.TaosConnection, ReadingsSql)
		if err != nil {
			return err
		}
		err = async.GlobalAsync.TaosExecWithoutResult(d.Db.TaosConnection, DiagnosticsSql)
		if err != nil {
			return err
		}
	}
	return nil
}
