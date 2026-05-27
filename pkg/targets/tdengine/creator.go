package tdengine

import (
	"fmt"
	"log"
	"strconv"

	"github.com/timescale/tsbs/pkg/targets/tdengine/async"
	"github.com/timescale/tsbs/pkg/targets/tdengine/commonpool"
)

var fatal = log.Fatalf

type DbCreator struct {
	Opts *LoadingOptions
	Db   *commonpool.Conn
}

func (d *DbCreator) Init() {
	async.Init()
	db, err := commonpool.GetConnection(d.Opts.User, d.Opts.Pass, d.Opts.Host, d.Opts.Port)
	if err != nil {
		panic("TDengine can not get connection")
	}
	d.Db = db
}

func (d *DbCreator) DBExists(dbName string) bool {
	err := async.GlobalAsync.TaosExecWithoutResult(d.Db.TaosConnection, "use "+dbName)
	return err == nil
}

func (d *DbCreator) CreateDB(dbName string) error {
	sql := fmt.Sprintf("create database %s precision 'ms'", dbName)
	if d.Opts.Buffer > 0 {
		sql += " buffer " + strconv.Itoa(d.Opts.Buffer)
	}
	if d.Opts.Pages > 0 {
		sql += " pages " + strconv.Itoa(d.Opts.Pages)
	}
	if d.Opts.VGroups > 0 {
		sql += " vgroups " + strconv.Itoa(d.Opts.VGroups)
	}
	if d.Opts.SttTrigger > 0 {
		sql += " stt_trigger " + strconv.Itoa(d.Opts.SttTrigger)
	}
	if d.Opts.WalFsyncPeriod != nil {
		sql += " wal_fsync_period " + strconv.Itoa(*d.Opts.WalFsyncPeriod)
	}
	if d.Opts.WalLevel != nil {
		sql += " wal_level " + strconv.Itoa(*d.Opts.WalLevel)
	}
	if d.Opts.DBParameters != "" {
		sql += " " + d.Opts.DBParameters
	}
	return async.GlobalAsync.TaosExecWithoutResult(d.Db.TaosConnection, sql)
}

func (d *DbCreator) RemoveOldDB(dbName string) error {
	sql := fmt.Sprintf("drop database %s", dbName)
	return async.GlobalAsync.TaosExecWithoutResult(d.Db.TaosConnection, sql)
}

func (d *DbCreator) Close() {
	if d.Db != nil {
		d.Db.Put()
	}
}
