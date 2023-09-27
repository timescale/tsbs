package main

import (
	"time"
)

type dbCreator struct {
}

func (d *dbCreator) Init() {
	// no-op
}

func (d *dbCreator) DBExists(dbName string) bool {
	// We don't really care if the table already exists,
	// especially when dedup is configured.
	return false
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	time.Sleep(time.Second)
	return nil
}
