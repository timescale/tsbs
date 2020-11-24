package main

import (
	"time"
)

type dbCreator struct {
	questdbRESTEndPoint string
}

func (d *dbCreator) Init() {
	d.questdbRESTEndPoint = questdbRESTEndPoint
}

func (d *dbCreator) DBExists(dbName string) bool {
	return false
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	time.Sleep(time.Second)
	return nil
}
